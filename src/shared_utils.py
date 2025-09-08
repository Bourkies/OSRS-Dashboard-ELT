# src/shared_utils.py

import sys
try:
    import tomllib # For Python 3.11+
except ImportError:
    import tomli as tomllib # Fallback for older Python
from pathlib import Path
from sqlalchemy import create_engine
from datetime import datetime, timedelta, timezone
import os
import requests
import json

from loguru import logger
from loguru_setup import loguru_setup

# --- Constants & Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = PROJECT_ROOT / 'src'
DATA_DIR = PROJECT_ROOT / 'data'
LOGS_DIR = PROJECT_ROOT / 'logs'
SUMMARIES_DIR = PROJECT_ROOT / 'summaries'
CONFIG_PATH = SRC_ROOT / 'config.toml'
SECRETS_PATH = SRC_ROOT / 'secrets.toml'

# --- Ensure Directories Exist ---
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
SUMMARIES_DIR.mkdir(exist_ok=True)

def write_summary_file(script_name: str, summary_content: str):
    """Writes a summary file for the script run."""
    run_timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    summary_file_name = f"{script_name}_summary_{run_timestamp}.txt"
    summary_file_path = SUMMARIES_DIR / summary_file_name
    try:
        with open(summary_file_path, 'w', encoding='utf-8') as f:
            f.write(summary_content)
        logger.info(f"Summary written to {summary_file_path}")
    except Exception as e:
        logger.error(f"Failed to write summary file: {e}")

def load_config():
    """Loads configuration from config.toml and secrets.toml."""
    # Note: We can't use the configured logger here because the config isn't loaded yet.
    # Loguru's default logger will print to stderr, which is acceptable for this step.
    logger.info("Loading configuration files...")
    try:
        with open(CONFIG_PATH, "rb") as f:
            config = tomllib.load(f)
        logger.success("--> config.toml loaded successfully.")

        with open(SECRETS_PATH, "rb") as f:
            secrets = tomllib.load(f)
        config['secrets'] = secrets
        logger.success("--> secrets.toml loaded successfully.")

        return config
    except FileNotFoundError as e:
        logger.critical(f"FATAL: Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"FATAL: Failed to parse a config file: {e}")
        sys.exit(1)

def get_db_engine(db_uri: str):
    """Creates a SQLAlchemy engine for a given database URI."""
    try:
        if db_uri.startswith('sqlite'):
            relative_path = db_uri.split('///')[1]
            db_file_path = (PROJECT_ROOT / relative_path).resolve()
            db_file_path.parent.mkdir(parents=True, exist_ok=True)
            engine = create_engine(f"sqlite:///{db_file_path}")
            logger.info(f"SQLite database engine created for: {db_file_path}")
        else:
            engine = create_engine(db_uri)
            logger.info("Remote PostgreSQL database engine configured.")
        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine for URI {db_uri}: {e}", exc_info=True)
        return None

def get_time_periods(config, run_time=None):
    """
    Calculates all dynamic time period start and end dates.
    If run_time is provided, calculations are based on that fixed point in time.
    Otherwise, it uses the current time.
    """
    if run_time is None:
        run_time = datetime.now(timezone.utc)
        
    settings = config.get('dashboard_settings', {})
    
    # Year-to-Date
    start_of_year = run_time.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Previous Month
    end_of_last_month = run_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    start_of_last_month = (end_of_last_month - timedelta(days=1)).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Previous Week
    week_start_day_name = settings.get('week_start_day', 'Monday')
    weekday_map = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
    week_start_day_num = weekday_map.get(week_start_day_name, 0)
    
    days_since_week_start = (run_time.weekday() - week_start_day_num + 7) % 7
    start_of_current_week = (run_time - timedelta(days=days_since_week_start)).replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_previous_week = start_of_current_week
    start_of_previous_week = end_of_previous_week - timedelta(days=7)

    # Custom Lookback
    custom_days = settings.get('custom_lookback_days', 14)
    start_of_custom = (run_time - timedelta(days=custom_days)).replace(hour=0, minute=0, second=0, microsecond=0)

    # All Time (no date filter)
    all_time_start = datetime.min.replace(tzinfo=timezone.utc)

    periods = {
        "All_Time": {"start": all_time_start, "end": run_time, "label": "All-Time"},
        "YTD": {"start": start_of_year, "end": run_time, "label": f"Year-to-Date ({run_time.year})"},
        "Prev_Month": {"start": start_of_last_month, "end": end_of_last_month, "label": f"{start_of_last_month.strftime('%B %Y')}"},
        "Prev_Week": {"start": start_of_previous_week, "end": end_of_previous_week, "label": f"Week {start_of_previous_week.isocalendar()[1]}"},
        "Custom_Days": {"start": start_of_custom, "end": run_time, "label": f"Last {custom_days} Days"}
    }
    return periods

def post_to_discord_webhook(webhook_url: str, message: str):
    """Posts a message to a Discord channel using a webhook."""
    if not webhook_url or "YOUR_WEBHOOK_URL_HERE" in webhook_url:
        logger.warning("Discord webhook URL is not configured. Skipping summary post.")
        return

    if len(message) > 2000:
        message = message[:1990] + '...'

    headers = {"Content-Type": "application/json"}
    payload = json.dumps({"content": message})

    try:
        response = requests.post(webhook_url, data=payload, headers=headers, timeout=10)
        if response.status_code in [200, 204]:
            logger.info("--> Summary posted to Discord via webhook.")
        else:
            logger.error(f"Failed to post to Discord webhook. Status: {response.status_code}, Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred while sending request to Discord webhook: {e}", exc_info=True)