# Cyclicist-Bike-share-Data-Validation-Trimming---ETL
A production-ready ETL pipeline written in Python to ingest, clean, transform, and aggregate raw trip data for the Cyclistic Bike-share Case Study. It includes logging, error handling, defensive directory checks, and downstream output generation for visual analysis in Tableau or BigQuery.

## 🚲 Cyclistic Bike-Share ETL Pipeline

This repository contains a modular Python script (`etl_pipeline.py`) designed to automate the Extract, Transform, Load (ETL) processing for the Cyclistic Bike-share dataset.

### Pipeline Workflow
1. **Extract (`extract_zip_archives` & `locate_csv_files`):** Automatically scans target directories for raw `.zip` files, extracts them, and maps `.csv` absolute paths.
2. **Ingest (`ingest_and_merge_data`):** Merges dynamic multi-month raw trip datasets into a unified pandas Dataframe.
3. **Clean & Engineer (`transform_and_clean_data`):** Converts timestamps, calculates ride durations in minutes/seconds, derives temporal feature tags, and filters out non-operational/short ride anomalies (< 60s).
4. **Aggregate & Export (`generate_and_export_summaries`):** Constructs high-level summary matrices exported as lightweight CSVs for downstream BI dashboards (Tableau / PowerBI) or BigQuery analytical queries.

### Key Features 
Automated Extraction: Discovers local .zip archives or pre-extracted .csv datasets and handles uncompressing automatically.
​Streamlined Ingestion: Dynamically locates and merges multi-file monthly datasets into a unified pandas DataFrame using memory-efficient list comprehensions.
​Transformations & Data Hygiene:
​Strict schema enforcement for safety.
​Time-based metrics engineering (ride_length_sec, ride_length_min, day_of_the_week, month).
​Automated data scrubbing (drops trips < 60s to remove system test rides/accidental docks).
​Summary Exports: Generates granular KPI summaries (user, weekly, and monthly aggregation tables) ready for Tableau or SQL workflows.
​Production Logging: Employs structured Python logging to trace step-by-step progress and raise descriptive errors. 

### Usage
```bash
python etl_pipeline.py
