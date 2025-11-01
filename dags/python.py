from __future__ import annotations
import csv
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sdk import task
from psycopg2 import Error as DatabaseError
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import shutil
from faker import Faker


OUTPUT_DIR = "/opt/airflow/data"
TARGET_TABLE = 'employees'

default_args = {
    "owner": "IDS706",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}

with DAG(
    dag_id="pipeline",
    start_date=datetime(2025, 10, 1),
    schedule= "@once", #"00 22 * * *",
    catchup=False,
) as dag:

    @task()
    def fetch_persons(output_dir: str = OUTPUT_DIR, quantity = 100) -> None:
        fake = Faker()
        data = []
        for _ in range(quantity):
            data.append({
                "firstname": fake.first_name(),
                "lastname": fake.last_name(),
                "email": fake.free_email(),
                "phone": fake.phone_number(),
                "address": fake.street_address(),
                "city": fake.city(),
                "country": fake.country()
            })

        filepath = os.path.join(output_dir, "persons.csv")

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        print(f"Data saved to {filepath}")

        return filepath

    @task()
    def fetch_companies(output_dir: str = OUTPUT_DIR, quantity = 100) -> str:
        fake = Faker()
        data = []
        for _ in range(quantity):
            data.append({
                "name": fake.company(),
                "email": f"info@{fake.domain_name()}",
                "phone": fake.phone_number(),
                "country": fake.country(),
                "website": fake.url(),
                "industry": fake.bs().split()[0].capitalize(),
                "catch_phrase": fake.catch_phrase(),
                "employees_count": fake.random_int(min=10, max=5000),
                "founded_year": fake.year()
        })


        filepath = os.path.join(output_dir, "companies.csv")
        os.makedirs(output_dir, exist_ok=True)

        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

        print(f"Companies data saved to {filepath}")
        return filepath


    @task()
    def merge_csvs(persons_path: str, companies_path: str, output_dir: str = OUTPUT_DIR) -> str:
        merged_path = os.path.join(output_dir, "merged_data.csv")

        with open(persons_path, newline="", encoding="utf-8") as f1, \
            open(companies_path, newline="", encoding="utf-8") as f2:

            persons_reader = list(csv.DictReader(f1))
            companies_reader = list(csv.DictReader(f2))

        merged_data = []
        for i in range(min(len(persons_reader), len(companies_reader))):
            person = persons_reader[i]
            company = companies_reader[i]
            merged_data.append({
                "firstname": person["firstname"],
                "lastname": person["lastname"],
                "email": person["email"],
                "company_name": company["name"],
                "company_email": company["email"]
            })

        with open(merged_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=merged_data[0].keys())
            writer.writeheader()
            writer.writerows(merged_data)

        print(f"Merged CSV saved to {merged_path}")
        return merged_path

     
    @task()
    def load_csv_to_pg(conn_id: str, csv_path: str, table: str = "employees", append: bool = True) -> int:
        schema = "week8_demo"

        # Reading the csv file
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = [tuple((r.get(col, "") or None) for col in fieldnames) for r in reader]

        if not rows:
            print("No rows found in CSV; nothing to insert.")
            return 0

        # Sql query preparations
        create_schema = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        create_table = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {', '.join([f'{col} TEXT' for col in fieldnames])}
            );
        """

        delete_rows = f"DELETE FROM {schema}.{table};" if not append else None

        insert_sql = f"""
            INSERT INTO {schema}.{table} ({', '.join(fieldnames)})
            VALUES ({', '.join(['%s' for _ in fieldnames])});
        """

        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(create_schema)
                cur.execute(create_table)

                if delete_rows:
                    cur.execute(delete_rows)
                    print(f"Cleared existing rows in {schema}.{table}")

                cur.executemany(insert_sql, rows)
                conn.commit()

            inserted = len(rows)
            print(f"Inserted {inserted} rows into {schema}.{table}")
            return inserted

        except DatabaseError as e:
            print(f"Database error: {e}")
            conn.rollback()
            return 0
        finally:
            conn.close()

    @task()
    def clear_folder(folder_path: str = "/opt/airflow/data") -> None:
        """
        Delete all files and subdirectories inside a folder.
        Keeps the folder itself.
        """

        if not os.path.exists(folder_path):
            print(f"Folder {folder_path} does not exist.")
            return

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.remove(file_path)
                    print(f"Removed file: {file_path}")
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                    print(f"Removed directory: {file_path}")
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

        print('Clean process completed!')
            

    persons_file = fetch_persons()
    companies_file = fetch_companies()
    merged_path = merge_csvs(persons_file, companies_file)

    load_to_database = load_csv_to_pg(
        conn_id="Postgres",
        csv_path=merged_path,
        table=TARGET_TABLE
        )

    clean_folder = clear_folder(folder_path=OUTPUT_DIR)

    load_to_database >> clean_folder

