from __future__ import annotations
import csv, os, shutil, glob
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task, TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import Error as DatabaseError

INPUT_DIR = "/opt/airflow/datasets"
OUTPUT_DIR = "/opt/airflow/data"
REPORT_DIR = os.path.join(OUTPUT_DIR, "analysis")

PG_CONN_ID = "Postgres"
SCHEMA = "week8_demo"

T_AIRLINES = "airlines"
T_AIRPORTS = "airports"
T_PLANES = "planes"
T_COUNTRY_SUMMARY = "country_summary"

OPENFLIGHTS_HEADERS = {
    "airports.txt": [
        "airport_id","name","city","country","iata","icao",
        "latitude","longitude","altitude","timezone",
        "dst","tz_database_timezone","type","source"
    ],
    "airlines.txt": [
        "airline_id","name","alias","iata","icao","callsign","country","active"
    ],
    "planes.txt": ["name","iata","icao"]
}

def _detect_delimiter(sample_header: str) -> str:
    if "," in sample_header: return ","
    if "\t" in sample_header: return "\t"
    if "|" in sample_header: return "|"
    return ","

@task()
def clear_folder(folder_path: str = OUTPUT_DIR) -> None:
    if not os.path.exists(folder_path): return
    for name in os.listdir(folder_path):
        if name == "analysis": continue
        p = os.path.join(folder_path, name)
        try:
            if os.path.isfile(p) or os.path.islink(p): os.remove(p)
            elif os.path.isdir(p): shutil.rmtree(p)
        except Exception: pass

@task()
def ingest_and_clean_file(input_filename: str, output_basename: str) -> str:
    in_path = os.path.join(INPUT_DIR, input_filename)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"{output_basename}.csv")
    header_override = OPENFLIGHTS_HEADERS.get(input_filename)
    with open(in_path, "r", encoding="utf-8", newline="") as f:
        if header_override:
            reader = csv.reader(f)
            rows = [r for r in reader if any(x.strip() for x in r)]
            fieldnames = header_override
        else:
            header_line = f.readline().rstrip("\n")
            delim = _detect_delimiter(header_line)
            f.seek(0)
            reader = csv.DictReader(f, delimiter=delim)
            fieldnames = [c.strip() for c in (reader.fieldnames or [])]
            rows = [tuple((r.get(c, "") or "").strip() for c in fieldnames) for r in reader]
    with open(out_path, "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf)
        w.writerow(fieldnames)
        w.writerows(rows)
    return out_path

@task()
def country_summary_csv(airports_csv_path: str) -> str:
    out_path = os.path.join(OUTPUT_DIR, f"{T_COUNTRY_SUMMARY}.csv")
    counts: dict[str, int] = {}
    with open(airports_csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        cols = [c.strip() for c in (reader.fieldnames or [])]
        country_key = None
        for k in ["country","Country","COUNTRY","iso_country","ISO_COUNTRY"]:
            if k in cols: country_key = k; break
        if country_key is None and cols: country_key = cols[-1]
        for r in reader:
            c = (r.get(country_key, "") or "").strip()
            if c: counts[c] = counts.get(c, 0) + 1
    with open(out_path, "w", encoding="utf-8", newline="") as wf:
        w = csv.writer(wf)
        w.writerow(["country","airport_count"])
        for c, n in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
            w.writerow([c, n])
    return out_path

@task()
def load_csv_to_pg(postgres_conn_id: str, csv_path: str, schema: str, table: str, append: bool = True) -> int:
    if not os.path.exists(csv_path): raise FileNotFoundError(csv_path)
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        rows = [tuple((r.get(col, "") or None) for col in fieldnames) for r in reader]
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cols_def = ", ".join([f'"{c}" TEXT' for c in fieldnames])
    create_schema = f'CREATE SCHEMA IF NOT EXISTS "{schema}";'
    create_table = f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({cols_def});'
    truncate_tbl = f'TRUNCATE TABLE "{schema}"."{table}";'
    field_list = ", ".join([f'"{c}"' for c in fieldnames])
    placeholders = ", ".join(["%s"] * len(fieldnames))
    insert_sql = f'INSERT INTO "{schema}"."{table}" ({field_list}) VALUES ({placeholders});'
    try:
        with conn.cursor() as cur:
            cur.execute(create_schema)
            cur.execute(create_table)
            if not append: cur.execute(truncate_tbl)
            if rows: cur.executemany(insert_sql, rows)
        conn.commit()
        return len(rows)
    except DatabaseError as e:
        conn.rollback()
        print(f"Database error: {e}")
        return 0
    finally:
        conn.close()

@task()
def analyze_top_countries(postgres_conn_id: str = PG_CONN_ID, schema: str = SCHEMA, out_dir: str = REPORT_DIR) -> str:
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "top_countries_by_airports.csv")
    sql = f'''
        SELECT COALESCE(country, '') AS country, COUNT(*) AS airport_count
        FROM "{schema}"."{T_AIRPORTS}"
        GROUP BY country
        ORDER BY airport_count DESC, country ASC
        LIMIT 10;
    '''
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur: cur.execute(sql); rows = cur.fetchall()
        with open(out_path, "w", encoding="utf-8", newline="") as wf:
            w = csv.writer(wf); w.writerow(["country","airport_count"])
            for r in rows: w.writerow([r[0], r[1]])
        return out_path
    finally:
        conn.close()

@task()
def visualize_report() -> str:
    import pandas as pd
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    os.makedirs(REPORT_DIR, exist_ok=True)
    csv_path = os.path.join(REPORT_DIR, "top_countries_by_airports.csv")
    png_path = os.path.join(REPORT_DIR, "top_countries_by_airports.png")
    df = pd.read_csv(csv_path)
    ax = df.plot(kind="bar", x="country", y="airport_count", legend=False)
    ax.set_title("Top 10 Countries by Airport Count (SQL)")
    ax.set_xlabel("Country")
    ax.set_ylabel("Number of Airports")
    plt.tight_layout()
    plt.savefig(png_path)
    return png_path

@task()
def analyze_top_countries_spark(airports_csv_path: str) -> str:
    import os
    from pyspark.sql import SparkSession, functions as F
    os.makedirs(REPORT_DIR, exist_ok=True)
    out_path = os.path.join(REPORT_DIR, "top_countries_by_airports.csv")
    assert os.path.exists(airports_csv_path), f"not found: {airports_csv_path}"

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("top_countries_by_airports")
        .getOrCreate()
    )

    df = spark.read.option("header", True).csv(airports_csv_path)
    top10 = (
        df.groupBy("country")
          .agg(F.count(F.lit(1)).alias("airport_count"))
          .orderBy(F.desc("airport_count"), F.asc("country"))
          .limit(10)
    )

    top10.toPandas().to_csv(out_path, index=False)
    spark.stop()
    return out_path


@task()
def visualize_report_from_csv(csv_path: str) -> str:
    import os
    import pandas as pd
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    assert os.path.exists(csv_path), f"not found: {csv_path}"
    os.makedirs(REPORT_DIR, exist_ok=True)

    df = pd.read_csv(csv_path)
    png_path = os.path.join(
        REPORT_DIR,
        os.path.splitext(os.path.basename(csv_path))[0] + ".png"
    )
    ax = df.plot(kind="bar", x="country", y="airport_count", legend=False)
    ax.set_title("Top 10 Countries by Airport Count")
    ax.set_xlabel("Country")
    ax.set_ylabel("Number of Airports")
    plt.tight_layout()
    plt.savefig(png_path)
    return png_path


default_args = {"owner": "IDS706", "retries": 1, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="pipeline",
    start_date=datetime(2025, 10, 1),
    schedule="@once",
    catchup=False,
    default_args=default_args,
    tags=["ids706","week8","spark"],
) as dag:
    start_clean = clear_folder(OUTPUT_DIR)

    with TaskGroup("ingest_and_clean") as ingest_group:
        clean_airlines = ingest_and_clean_file("airlines.txt", T_AIRLINES)
        clean_airports = ingest_and_clean_file("airports.txt", T_AIRPORTS)
        clean_planes = ingest_and_clean_file("planes.txt", T_PLANES)

    summary = country_summary_csv(clean_airports)

    load_airlines = load_csv_to_pg(PG_CONN_ID, clean_airlines, SCHEMA, T_AIRLINES, True)
    load_airports = load_csv_to_pg(PG_CONN_ID, clean_airports, SCHEMA, T_AIRPORTS, True)
    load_planes = load_csv_to_pg(PG_CONN_ID, clean_planes, SCHEMA, T_PLANES, True)
    load_summary = load_csv_to_pg(PG_CONN_ID, summary, SCHEMA, T_COUNTRY_SUMMARY, True)

    analysis_sql = analyze_top_countries()
    visualize_sql = visualize_report()

    analysis_spark = analyze_top_countries_spark(clean_airports) 
    visualize_spark = visualize_report_from_csv(analysis_spark) 
  

    end_clean = clear_folder(OUTPUT_DIR)

    start_clean >> ingest_group
    ingest_group >> [load_airlines, load_airports, load_planes]
    clean_airports >> summary >> load_summary
    [load_airlines, load_airports, load_planes, load_summary] >> analysis_sql >> visualize_sql
    clean_airports >> analysis_spark >> visualize_spark
    [visualize_sql, visualize_spark] >> end_clean
