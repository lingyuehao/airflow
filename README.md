
# Airflow Data Pipeline

## Overview

This project builds an Apache Airflow pipeline that ingests, transforms, analyzes, and visualizes aviation data using three related datasets.
The workflow demonstrates how to orchestrate a complete ETL process with scheduling, task parallelism, and PySpark integration.

## Datasets

Three related datasets from OurAirports were used:

- airports.txt – detailed information about airports (location, country, type)

- airlines.txt – airline names and their operating countries

- planes.txt – aircraft types and identifiers

All files are stored under /opt/airflow/datasets/ and are cleaned into /opt/airflow/data/.


## Pipeline Summary

The Airflow pipeline begins by deploying a DAG named pipeline, manually triggered on demand and executed within a fully containerized environment that includes Airflow, PostgreSQL, and PySpark. The workflow starts by clearing the working directory to ensure a clean state for each run. During the ingestion and transformation stage, three related aviation datasets are extracted and standardized—removing missing values, harmonizing schemas, and converting text data into structured tabular form suitable for downstream processing. Parallel tasks then aggregate data by country to identify key patterns in airport distribution and prepare intermediate tables for analysis.

In the analytical phase, the transformed data is loaded into a PostgreSQL database, where SQL queries summarize relationships among airlines, aircraft, and airports. In parallel, a PySpark task computes and ranks countries based on the number of active airports, leveraging distributed processing for faster computation. The visualization stage then consolidates these analytical results into summary charts, enabling comparison across multiple dimensions. Finally, a cleanup step removes all temporary artifacts created during execution, ensuring reproducibility and efficient resource management.

All tasks executed successfully, as shown in the Airflow UI logs and DAG graph, confirming the correct orchestration and dependency management of this multi-stage ETL and analysis workflow.

