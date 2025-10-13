# src/run_all_etl.py
# This script runs the entire ETL pipeline in sequence and posts a summary.

import sys
import subprocess
import time
import json
import re
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

from loguru import logger

# Define project root for use in other modules like logging
project_root = Path(__file__).resolve().parent.parent

from shared_utils import (
    load_config, post_to_discord_webhook, LOGS_DIR, SUMMARIES_DIR, DATA_DIR
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
            # By removing `capture_output=True`, the child process's stdout and stderr
            # will be streamed directly to the console in real-time.
            text=True,
            encoding='utf-8'
        )
        logger.success(f"--- Finished {script_path.name} successfully ---")
    except subprocess.CalledProcessError as e:
        logger.critical(f"--- FATAL ERROR during execution of {script_path.name} ---")
        logger.error(f"Return Code: {e.returncode}")
        # The specific error from the script will have been printed to the console just before this.
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
        base_scripts = [
            '1_fetch_data.py',
            '2_parse_engine.py',
            '3_transform_data.py'
        ]
        scripts_to_run = []

        # --- Conditionally add 4_fetch_item_prices.py ---
        min_hours = config.get('api_settings', {}).get('min_time_between_runs', 24)
        state_file = DATA_DIR / 'price_fetch_state.json'
        should_run_prices = True

        # Only check the time if the state file exists and is not empty
        if state_file.exists() and state_file.stat().st_size > 0:
            try:
                with open(state_file, 'r') as f:
                    state = json.load(f)
                    last_run_str = state.get('last_successful_run_utc')
                    if last_run_str:
                        last_run_time = datetime.fromisoformat(last_run_str)
                        if datetime.now(timezone.utc) < last_run_time + timedelta(hours=min_hours):
                            logger.info(f"Skipping '4_fetch_item_prices.py'. Last run was less than {min_hours} hours ago.")
                            should_run_prices = False
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(f"Could not read price fetch state file due to an error. Will run the script to be safe. Error: {e}")
        
        if should_run_prices:
            # Insert before the parse engine
            scripts_to_run.append('4_fetch_item_prices.py')
        
        scripts_to_run.extend(base_scripts)

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

            # If the price fetch script ran successfully, update its state file
            if script_name == '4_fetch_item_prices.py' and should_run_prices:
                logger.info("Updating state file for successful price fetch run.")
                with open(state_file, 'w') as f:
                    json.dump({'last_successful_run_utc': datetime.now(timezone.utc).isoformat()}, f)


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