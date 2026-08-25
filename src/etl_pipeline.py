# ==========================================
# 1. CORE ENVIRONMENTS & PACKAGES LOAD
# ==========================================
import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import glob     # Scans the folder matching specific patterns (like finding all '.zip' or '.csv' files)
import zipfile  # Opens, reads, and extracts compressed monthly data archives in the cloud
import os       # Interacts with the operating system (like creating folders)

# Input data files are available in the read-only "../input/" directory
# For example, running this (by clicking run or pressing Shift+Enter) will list all files under the input directory


import glob     # Used to scan the folder and find files matching 
                #specific patterns (like finding all '.zip' or '.csv' files)
import zipfile  # Provides the tools needed to open, read, and extract 
                #the compressed monthly data archives in the cloud

import logging  # Provides tools to track code execution, record operational 
                #events, and capture system errors with timestamps
# Configure structured logging for production-ready tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def extract_zip_archives(input_pattern: str = '/kaggle/input/**/*.zip', extract_to: str = './unzipped_csvs') -> str:
    """
    Locates and extracts compressed ZIP archives into a target cloud workspace directory.
    If no ZIP files are found, it gracefully checks if unzipped CSVs already exist.

    Args:
        input_pattern (str): Glob pattern matching the target ZIP archives.
        extract_to (str): Target directory where files will be uncompressed.

    Returns:
        str: The path to the directory containing the uncompressed data.

    Raises:
        FileNotFoundError: If neither ZIPs nor pre-extracted CSVs are available.
    """
    logging.info("Scanning Kaggle input directories for compressed ZIP archives...")
    zip_files = glob.glob(input_pattern, recursive=True)
    
    if not zip_files:
        logging.warning("No ZIP files found. Checking if dataset is already unzipped by Kaggle...")
        # Direct check to see if CSVs already exist from a pre-unzipped dataset upload
        if glob.glob('/kaggle/input/**/*.csv', recursive=True):
            logging.info("Pre-extracted CSV files detected. Bypassing extraction phase.")
            return '/kaggle/input/'
        
        error_msg = "No operational data sources (.zip or .csv) found inside /kaggle/input/."
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)
        
    logging.info(f"Located {len(zip_files)} raw ZIP archive(s) to process. Initializing extraction...")
    os.makedirs(extract_to, exist_ok=True)
    
    for file in zip_files:
        logging.info(f"Unzipping archive: {os.path.basename(file)}...")
        try:
            with zipfile.ZipFile(file, 'r') as zip_ref:
                zip_ref.extractall(extract_to)
        except zipfile.BadZipFile:
            logging.error(f"Failed to extract {os.path.basename(file)}: File is corrupt or not a valid ZIP archive.")
            raise
        except Exception as e:
            logging.error(f"Unexpected operational failure during extraction of {os.path.basename(file)}: {str(e)}")
            raise
            
    logging.info(f"Extraction phase complete. Staging folder verified: {extract_to}")
    return extract_to


def locate_csv_files(search_directory: str) -> list:
    """
    Scans the designated extraction or input directory deeply to compile a list of CSV files.
    
    Args:
        search_directory (str): Root directory path to begin the deep CSV search.
        
    Returns:
        list: A list of absolute file paths to the discovered CSV files.
        
    Raises:
        FileNotFoundError: If no CSV files are discovered inside the target directory.
    """
    search_pattern = os.path.join(search_directory, '**/*.csv')
    logging.info(f"Compiling CSV file manifests from pattern: {search_pattern}")
    files = glob.glob(search_pattern, recursive=True)
    
    if not files:
        error_msg = f"Zero operational CSV datasets found within the path: {search_directory}"
        logging.error(error_msg)
        raise FileNotFoundError(error_msg)
        
    logging.info(f"Successfully cataloged {len(files)} target CSV files for ingestion.")
    return files


def ingest_and_merge_data(file_paths: list) -> pd.DataFrame:
    """
    Reads multiple CSV data files and merges them into a unified master DataFrame.
    
    Args:
        file_paths (list): Collection of absolute file paths to load.
        
    Returns:
        pd.DataFrame: The consolidated, master dataset structure.
        
    Raises:
        ValueError: If concatenation fails due to file system or structural anomalies.
    """
    logging.info("Consolidating monthly source files into a single master framework...")
    try:
        # High-performance list comprehension minimizes memory footprint during instantiation
        master_df = pd.concat([pd.read_csv(f) for f in file_paths], ignore_index=True)
        logging.info(f"Master framework compiled. Initial Row Count: {len(master_df):,}")
        return master_df
    except Exception as e:
        logging.error(f"Critical error encountered during file concatenation: {str(e)}")
        raise ValueError(f"Failed to merge CSV collection components: {str(e)}")


def transform_and_clean_data(df: pd.DataFrame, min_duration_sec: float = 60.0) -> pd.DataFrame:
    """
    Performs data cleaning, schema reinforcement, and calculated metric engineering.
    
    Args:
        df (pd.DataFrame): The uncleaned consolidated master DataFrame.
        min_duration_sec (float): Filtering cutoff in seconds for short trips.
        
    Returns:
        pd.DataFrame: An isolated, scrubbed DataFrame optimized for analytics.
    """
    logging.info("Executing analytical data scrubbing and feature transformation pipelines...")
    
    required_schema = {'started_at', 'ended_at', 'ride_id', 'member_casual'}
    if not required_schema.issubset(df.columns):
        raise KeyError(f"Data schema violation. Missing required columns: {required_schema - set(df.columns)}")
        
    # Guard against side-effects by creating a clean copy
    processed_df = df.copy()
    
    # A. Enforce Object-to-Datetime Parsing
    processed_df['started_at'] = pd.to_datetime(processed_df['started_at'])
    processed_df['ended_at'] = pd.to_datetime(processed_df['ended_at'])
    
    # B. Compute Travel Metrics
    processed_df['ride_length_sec'] = (processed_df['ended_at'] - processed_df['started_at']).dt.total_seconds()
    processed_df['ride_length_min'] = processed_df['ride_length_sec'] / 60.0
    
    # C. Engineer Temporal Grouping Attributes (1 = Monday, 7 = Sunday)
    processed_df['day_of_the_week'] = processed_df['started_at'].dt.dayofweek + 1
    processed_df['month'] = processed_df['started_at'].dt.strftime('%Y-%m')
    
    # D. Operational Integrity Filters
    # Drops negative trip errors and short maintenance/misdocking events (< 1 minute)
    clean_df = processed_df[processed_df['ride_length_sec'] >= min_duration_sec].copy()
    
    rows_removed = len(processed_df) - len(clean_df)
    logging.info(f"Data scrub complete. Filtered out {rows_removed:,} non-operational/short rows.")
    logging.info(f"Final Cleaned Dataset Row Count: {len(clean_df):,}")
    
    return clean_df


def generate_and_export_summaries(df: pd.DataFrame, output_dir: str = './') -> None:
    """
    Groups and aggregates massive records into small summary matrices for BigQuery/Tableau.
    
    Args:
        df (pd.DataFrame): The verified analytical DataFrame.
        output_dir (str): Working output directory path.
    """
    logging.info("Constructing highly aggregated KPI tables...")
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # Matrix 1: Profile Segment Overview Metrics
        summary_user = df.groupby('member_casual').agg(
            total_rides=('ride_id', 'count'),
            avg_duration_minutes=('ride_length_min', 'mean'),
            median_duration_minutes=('ride_length_min', 'median')
        ).reset_index()

        # Matrix 2: Day-of-Week Operational Trends
        summary_weekly = df.groupby(['member_casual', 'day_of_the_week']).agg(
            total_rides=('ride_id', 'count'),
            avg_duration_minutes=('ride_length_min', 'mean')
        ).reset_index()

        # Matrix 3: Seasonal Timeline Performance Metrics
        summary_monthly = df.groupby(['member_casual', 'month']).agg(
            total_rides=('ride_id', 'count'),
            avg_duration_minutes=('ride_length_min', 'mean')
        ).reset_index()
        
        # Disk writing operations
        summary_user.to_csv(os.path.join(output_dir, 'summary_user_metrics.csv'), index=False)
        summary_weekly.to_csv(os.path.join(output_dir, 'summary_weekly_trends.csv'), index=False)
        summary_monthly.to_csv(os.path.join(output_dir, 'summary_monthly_trends.csv'), index=False)
        
        logging.info(f"✨ Success! Summary CSV outputs securely generated in: '{output_dir}'")
        
    except Exception as e:
        logging.error(f"Failed to generate summary matrices: {str(e)}")
        raise


def run_pipeline():
    """
    Main orchestrator controlling the safe linear execution of the ETL pipeline components.
    """
    logging.info("=== INITIALIZING CYCLISTIC DATA PIPELINE ===")
    try:
        data_source_folder = extract_zip_archives()
        csv_files = locate_csv_files(data_source_folder)
        raw_data = ingest_and_merge_data(csv_files)
        cleaned_data = transform_and_clean_data(raw_data)
        generate_and_export_summaries(cleaned_data)
        logging.info("=== PIPELINE EXECUTION COMPLETION STATUS: SUCCESS ===")
    except Exception as e:
        logging.critical(f"ETL Execution halted due to an unhandled system fault: {str(e)}")


# Entry point safeguard: Ensures the pipeline only 
            #executes if this script is run directly,
# preventing it from automatically triggering if 
            #its functions are imported into another notebook.

if __name__ == '__main__':
    run_pipeline()   # Calls the orchestrator function to trigger the full ETL workflow
