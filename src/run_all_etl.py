# src/run_all_etl.py
# This script runs the entire ETL pipeline in sequence and posts a summary.

import sys
import subprocess
import time
import re
import os
from pathlib import Path
from datetime import datetime, timedelta

from loguru import logger

# Define project root for use in other modules like logging
project_root = Path(__file__).resolve().parent.parent

from shared_utils import (
    load_config, post_to_discord_webhook, LOGS_DIR, SUMMARIES_DIR
)
from loguru_setup import loguru_setup

SCRIPT_NAME = "run_all_etl"

def cleanup_old_files(directory: Path, retention_days: int):
    """Deletes files in a directory older than a specified number of days based on filename timestamp."""
    if not directory.exists():
        logger.warning(f"Cleanup directory not found, skipping: {directory}")
        return

    logger.info(f"Scanning '{directory.name}' for files to clean up...")
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    files_deleted = 0
    
    for item in directory.iterdir():
        if item.is_file():
            # Regex to find a YYYY-MM-DD date pattern in the filename
            match = re.search(r"(\d{4}-\d{2}-\d{2})", item.name)
            if match:
                try:
                    file_date_str = match.group(1)
                    file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
                    
                    if file_date < cutoff_date:
                        logger.info(f"  - Deleting old file: {item.name}")
                        item.unlink()  # Using Path.unlink() to delete the file
                        files_deleted += 1
                except ValueError:
                    logger.debug(f"Could not parse date from filename, skipping: {item.name}")
                except Exception as e:
                    logger.error(f"Error deleting file {item.name}: {e}")

    logger.info(f"--> Cleanup complete for '{directory.name}'. Deleted {files_deleted} old files.")


def run_script(script_path: Path) -> float:
    """Runs a given Python script as a subprocess and returns its execution time."""
    start_time = time.time()
    logger.info(f"{f' Starting execution of {script_path.name} ':=^80}")
    try:
        # Use sys.executable to ensure the same Python interpreter is used
        result = subprocess.run(
            [sys.executable, str(script_path)],
            check=True,
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        logger.success(f"--- Finished {script_path.name} successfully ---")
        logger.debug(f"Output from {script_path.name}:\n{result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.critical(f"--- FATAL ERROR during execution of {script_path.name} ---")
        logger.error(f"Return Code: {e.returncode}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        # Re-raise the exception to stop the entire pipeline
        raise e
    
    end_time = time.time()
    logger.info(f"{f' Finished execution of {script_path.name} ':=^80}")
    return end_time - start_time

def main():
    """Main function to run all ETL scripts."""
    config = load_config()
    loguru_setup(config, project_root)
    logger.info(f"{' Starting Full ETL Pipeline ':=^80}")

    # --- Run Cleanup ---
    cleanup_config = config.get('cleanup_settings', {})
    retention_days = cleanup_config.get('log_retention_days', 0)
    
    if retention_days > 0:
        logger.info(f"--- Running cleanup for files older than {retention_days} days ---")
        cleanup_old_files(LOGS_DIR, retention_days)
        cleanup_old_files(SUMMARIES_DIR, retention_days)
    else:
        logger.info("--- File cleanup is disabled (log_retention_days is 0 or not set) ---")

    webhook_url = config.get('secrets', {}).get('discord_webhook_url')
    project_name = config.get('general', {}).get('project_name', 'ETL Process')
    
    # Announce the start of the pipeline
    start_message = f"**🚀 {project_name}: Full ETL Pipeline Starting...**"
    post_to_discord_webhook(webhook_url, start_message)

    execution_times = {}
    total_start_time = time.time()

    try:
        # Define the sequence of scripts to run
        scripts_to_run = [
            '1_fetch_data.py',
            '2_parse_engine.py',
            '3_transform_data.py'
        ]

        # Conditionally add the PB posting script based on the config
        # The user has renamed the script to include a '5_' prefix.
        if config.get('etl_runner', {}).get('run_post_pbs_script', True):
            scripts_to_run.append('5_post_pbs_to_discord.py')
        else:
            logger.warning("Skipping '5_post_pbs_to_discord.py' as per config setting.")

        # Execute each script
        src_path = Path(__file__).parent
        for script_name in scripts_to_run:
            duration = run_script(src_path / script_name)
            execution_times[script_name] = f"{duration:.2f} seconds"

        total_duration = time.time() - total_start_time
        
        # Format the final success message
        times_str = "\n".join([f"- `{script}`: `{duration}`" for script, duration in execution_times.items()])
        summary_message = (
            f"**✅ {project_name}: Full ETL Pipeline Complete!**\n\n"
            f"**Execution Times:**\n{times_str}\n\n"
            f"**Total Runtime:** `{total_duration:.2f} seconds`"
        )

    except Exception as e:
        total_duration = time.time() - total_start_time
        summary_message = (
            f"**❌ {project_name}: Full ETL Pipeline FAILED!**\n\n"
            f"An error occurred during the process. Please check the logs for details.\n"
            f"**Error:** `{str(e)}`\n"
            f"**Total Runtime before failure:** `{total_duration:.2f} seconds`"
        )
    
    # Post the final summary to Discord
    post_to_discord_webhook(webhook_url, summary_message)
    logger.info(f"{' Finished Full ETL Pipeline ':=^80}")


if __name__ == "__main__":
    main()