# src/4_load_data.py

import pandas as pd
import logging
import os
import math
from sqlalchemy import inspect, text, MetaData
from sqlalchemy.exc import ProgrammingError

from shared_utils import (
    setup_logging, load_config, get_db_engine, write_summary_file, PROJECT_ROOT, post_to_discord_webhook
)

SCRIPT_NAME = "4_load_data"

def format_size(size_bytes):
    """Converts bytes to a human-readable string (KB, MB, GB)."""
    if size_bytes <= 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    power = math.pow(1024, i)
    size = round(size_bytes / power, 2)
    return f"{size} {size_name[i]}"

def get_db_size(engine):
    """Gets the size of the connected database."""
    if engine.dialect.name == 'postgresql':
        with engine.connect() as connection:
            result = connection.execute(text("SELECT pg_size_pretty(pg_database_size(current_database()))"))
            return result.scalar_one_or_none() or "N/A"
    return "N/A"


def main():
    """Main execution function for the load data script."""
    setup_logging(SCRIPT_NAME)
    config = load_config()

    # --- Configuration ---
    optimised_db_uri = config['databases']['optimised_db_uri']
    production_db_uri = config['secrets'].get('production_database_uri')
    load_settings = config.get('load_settings', {})
    staging_suffix = load_settings.get('staging_suffix', '_staging')
    
    if not production_db_uri or "YOUR_PRODUCTION_DB_URI" in production_db_uri:
        logging.critical("FATAL: 'production_database_uri' is not set in secrets.toml. Cannot proceed.")
        return

    # --- Database Engines ---
    optimised_engine = get_db_engine(optimised_db_uri)
    production_engine = get_db_engine(production_db_uri)

    if not optimised_engine or not production_engine:
        return

    # --- Summary & Stats Variables ---
    summary_stats = {}
    local_db_size_str = "N/A"
    summary = ""
    
    try:
        # --- Step 1: Get table names from local optimised DB ---
        local_inspector = inspect(optimised_engine)
        table_names_to_load = local_inspector.get_table_names()
        logging.info(f"Found {len(table_names_to_load)} tables in local optimised DB to load.")

        if not table_names_to_load:
            logging.warning("No tables found in the optimised database. Nothing to load.")
            return

        # --- Step 2: Clean up any old staging tables from previous failed runs ---
        logging.info("Checking for and cleaning up old staging tables in production...")
        prod_inspector = inspect(production_engine)
        all_prod_tables = prod_inspector.get_table_names()
        old_staging_tables = [t for t in all_prod_tables if t.endswith(staging_suffix)]
        
        if old_staging_tables:
            with production_engine.connect() as connection:
                with connection.begin():
                    for table_name in old_staging_tables:
                        connection.execute(text(f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'))
                        logging.info(f"  - Dropped old staging table: {table_name}")
            logging.info(f"--> Cleaned up {len(old_staging_tables)} old staging tables.")
        else:
            logging.info("--> No old staging tables found.")

        # --- Step 3: Load data from local DB into staging tables in production ---
        logging.info("Loading data into new staging tables...")
        for table_name in table_names_to_load:
            staging_table_name = f"{table_name}{staging_suffix}"
            logging.info(f"--> Processing: {table_name} -> {staging_table_name}")
            df = pd.read_sql_table(table_name, optimised_engine)
            
            # Load dataframe into the new staging table
            df.to_sql(staging_table_name, production_engine, if_exists='replace', index=False, chunksize=1000)
            
            summary_stats[table_name] = len(df)
            logging.info(f"  - Loaded {len(df)} rows to staging table '{staging_table_name}'.")

        # --- Step 4: Atomically swap the live tables with the new staging tables ---
        logging.info("Performing atomic swap of live and staging tables...")
        with production_engine.connect() as connection:
            with connection.begin(): # This ensures the whole block is one transaction
                logging.info("  - TRANSACTION STARTED for table swap.")
                for table_name in table_names_to_load:
                    staging_table_name = f"{table_name}{staging_suffix}"
                    
                    # Drop the old live table. The 'IF EXISTS' is a safety measure.
                    # CASCADE handles dropping dependent objects like views.
                    drop_sql = f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'
                    logging.info(f"    - Executing: {drop_sql}")
                    connection.execute(text(drop_sql))

                    # Rename the new staging table to become the new live table
                    rename_sql = f'ALTER TABLE "{staging_table_name}" RENAME TO "{table_name}";'
                    logging.info(f"    - Executing: {rename_sql}")
                    connection.execute(text(rename_sql))
            logging.info("  - TRANSACTION COMMITTED successfully.")

        # --- Step 5: Get Database Sizes and final stats ---
        if optimised_db_uri.startswith('sqlite'):
            db_file_path = (PROJECT_ROOT / optimised_db_uri.split('///')[1]).resolve()
            if os.path.exists(db_file_path):
                local_db_size_bytes = os.path.getsize(db_file_path)
                local_db_size_str = format_size(local_db_size_bytes)

        prod_db_size_str = get_db_size(production_engine)
        logging.info(f"Local DB Size: {local_db_size_str} | Production DB Size: {prod_db_size_str}")

        # --- Step 6: Generate and write summary ---
        loaded_table_counts_str = "\n".join([f"- `{name}`: `{count}` rows" for name, count in summary_stats.items()])
        
        summary = (
            f"**✅ {config.get('general', {}).get('project_name', 'Unnamed Project')}: {SCRIPT_NAME} Complete**\n\n"
            f"**Load Strategy:** `Blue-Green Swap`\n"
            f"**Load to Production Results:**\n"
            f"- Tables Staged & Swapped: `{len(summary_stats)}`\n"
            f"- Local DB Size: `{local_db_size_str}`\n"
            f"- Production DB Size: `{prod_db_size_str}`\n\n"
            f"**Loaded Table Row Counts:**\n{loaded_table_counts_str}"
        )
        write_summary_file(SCRIPT_NAME, summary)
        logging.info("Script finished successfully.")

    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)
        summary = (
            f"**❌ {config.get('general', {}).get('project_name', 'Unnamed Project')}: {SCRIPT_NAME} FAILED**\n\n"
            f"**Error:**\n```\n{e}\n```"
        )
        write_summary_file(SCRIPT_NAME, summary)
    finally:
        webhook_url = config.get('secrets', {}).get('discord_webhook_url')
        if summary and webhook_url:
            post_to_discord_webhook(webhook_url, summary)
        if optimised_engine: optimised_engine.dispose()
        if production_engine: production_engine.dispose()
        logging.info("Database connections closed.")

if __name__ == "__main__":
    main()