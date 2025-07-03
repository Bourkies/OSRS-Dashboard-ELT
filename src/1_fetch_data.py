# src/1_fetch_data.py

import discord
import pandas as pd
import re
import asyncio
from sqlalchemy import text, inspect, exc
from datetime import datetime, timedelta, timezone
import logging

from shared_utils import (
    setup_logging, load_config, get_db_engine, write_summary_file, post_to_discord_webhook
)

SCRIPT_NAME = "1_fetch_data"

def clean_discord_escapes(text: str) -> str:
    """Removes Discord's escape backslashes before punctuation."""
    return re.sub(r'\\([^\w\s])', r'\1', text)

def get_date_range(config: dict, engine) -> (datetime, datetime):
    """Determines the start and end dates for the data fetch."""
    logging.info("Determining date range for data fetch...")
    mode = config['time_settings']['mode']
    now = datetime.now(timezone.utc)

    if mode == 'custom':
        start_str = config['custom_time_range']['custom_start_date']
        end_str = config['custom_time_range']['custom_end_date']
        start_date = datetime.strptime(start_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(end_str, "%Y-%m-%d %H:%M").replace(tzinfo=timezone.utc)
        logging.info(f"--> Using CUSTOM date range: {start_date.strftime('%Y-%m-%d %H:%M')} to {end_date.strftime('%Y-%m-%d %H:%M')} UTC")
        return start_date, end_date
    
    # Automatic mode
    end_date = now - timedelta(minutes=config['time_settings']['end_time_offset_minutes'])
    
    try:
        with engine.connect() as connection:
            query = text("SELECT MAX(timestamp) FROM raw_logs")
            last_timestamp_str = connection.execute(query).scalar_one_or_none()
    except Exception as e:
        logging.warning(f"Could not query for last timestamp, maybe table doesn't exist? Error: {e}")
        last_timestamp_str = None

    if last_timestamp_str:
        last_run_date = datetime.fromisoformat(last_timestamp_str)
        start_date = last_run_date - timedelta(minutes=config['time_settings']['start_time_overlap_minutes'])
        logging.info(f"--> Last message in DB is from {last_run_date.strftime('%Y-%m-%d %H:%M')}. Fetching data since {start_date.strftime('%Y-%m-%d %H:%M')} UTC.")
    else:
        start_date = now - timedelta(days=config['time_settings']['max_lookback_days'])
        logging.info(f"--> No previous data found. Fetching data for the last {config['time_settings']['max_lookback_days']} days.")
        
    return start_date, end_date

def create_raw_table(engine):
    """Ensures the raw_logs table exists."""
    inspector = inspect(engine)
    if not inspector.has_table("raw_logs"):
        logging.info("Table 'raw_logs' not found. Creating it...")
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text("""
                    CREATE TABLE raw_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        timestamp TEXT NOT NULL,
                        raw_content TEXT NOT NULL,
                        UNIQUE(timestamp, raw_content)
                    )
                """))
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_timestamp ON raw_logs (timestamp);"))
            logging.info("--> Table 'raw_logs' created successfully with a timestamp index and UNIQUE constraint.")
    else:
        logging.info("--> Table 'raw_logs' already exists.")

class DiscordFetchBot(discord.Client):
    """The bot class responsible for fetching messages."""
    def __init__(self, config, start_date, end_date, engine):
        intents = discord.Intents.default()
        intents.messages = True
        intents.message_content = True
        super().__init__(intents=intents)
        self.config = config
        self.start_date = start_date
        self.end_date = end_date
        self.engine = engine
        self.summary = {}
        self.summary_message = ""

    async def on_ready(self):
        """Called when the bot successfully logs in."""
        logging.info(f'--> Logged in as {self.user} to fetch data.')
        try:
            await self.fetch_and_store_data()
            self.summary_message = self.format_summary(success=True)
            write_summary_file(SCRIPT_NAME, self.summary_message)
        except Exception as e:
            logging.error(f"FATAL ERROR during operation: {e}", exc_info=True)
            self.summary['error'] = str(e)
            self.summary_message = self.format_summary(success=False)
            write_summary_file(SCRIPT_NAME, self.summary_message)
        finally:
            logging.info("Operation complete. Logging out from Discord.")
            await self.close()

    async def on_disconnect(self):
        """Called when the bot disconnects; sends the summary via webhook."""
        logging.info("Disconnected from Discord. Now posting summary via webhook.")
        webhook_url = self.config.get('secrets', {}).get('discord_webhook_url')
        if self.summary_message and webhook_url:
             post_to_discord_webhook(webhook_url, self.summary_message)

    async def fetch_and_store_data(self):
        """Fetches messages and stores them in the raw SQLite database, gracefully skipping duplicates."""
        data_channel_id = self.config['secrets']['discord_data_channel_id']
        channel = self.get_channel(int(data_channel_id))
        if not channel:
            raise Exception(f"Could not find data channel with ID {data_channel_id}.")

        self.summary['guild_name'] = channel.guild.name if hasattr(channel, 'guild') else 'Direct Message'
        self.summary['data_channel_name'] = channel.name
        logging.info(f"Fetching messages from server: '{self.summary['guild_name']}', channel: #{channel.name}")

        messages_to_process = []
        fetch_counter = 0
        async for message in channel.history(limit=None, after=self.start_date, before=self.end_date, oldest_first=True):
            fetch_counter += 1
            if message.content:
                messages_to_process.append({
                    "timestamp": message.created_at.isoformat(),
                    "raw_content": clean_discord_escapes(message.content)
                })
            if fetch_counter % 500 == 0:
                logging.info(f"  - Discovered {fetch_counter} messages...")
        
        logging.info(f"--> Found {len(messages_to_process)} total messages with content.")
        self.summary['messages_found'] = len(messages_to_process)

        if not messages_to_process:
            logging.info("No new messages to add.")
            self.summary['messages_added'] = 0
            return

        df_new = pd.DataFrame(messages_to_process)
        
        # Insert rows one-by-one to gracefully handle duplicates
        rows_added = 0
        with self.engine.connect() as connection:
            with connection.begin():
                for _, row in df_new.iterrows():
                    try:
                        # Use INSERT OR IGNORE for SQLite to skip duplicates without erroring
                        insert_stmt = text("""
                            INSERT INTO raw_logs (timestamp, raw_content) 
                            VALUES (:timestamp, :raw_content)
                        """)
                        if self.engine.dialect.name == 'sqlite':
                             insert_stmt = text("""
                                INSERT OR IGNORE INTO raw_logs (timestamp, raw_content) 
                                VALUES (:timestamp, :raw_content)
                            """)
                        
                        result = connection.execute(insert_stmt, row.to_dict())
                        if result.rowcount > 0:
                            rows_added += 1
                    except exc.IntegrityError:
                        # This is a fallback for other DBs if they don't support INSERT OR IGNORE
                        # and ensures the script doesn't crash.
                        logging.debug(f"Skipping duplicate row: {row['timestamp']}")
                        continue

        self.summary['messages_added'] = rows_added
        logging.info(f"--> Successfully added {rows_added} new messages to the database. Skipped {len(df_new) - rows_added} duplicates.")

    def format_summary(self, success: bool) -> str:
        """Formats the summary message."""
        project_name = self.config.get('general', {}).get('project_name', 'Unnamed Project')
        
        if not success:
            return (f"**❌ {project_name}: {SCRIPT_NAME} FAILED**\n"
                    f"**Error:**\n```\n{self.summary.get('error', 'Unknown error')}\n```")

        start_str = self.start_date.strftime('%Y-%m-%d %H:%M')
        end_str = self.end_date.strftime('%Y-%m-%d %H:%M')
        
        return (f"**✅ {project_name}: {SCRIPT_NAME} Complete**\n\n"
                f"**Data Source:** `{self.summary.get('guild_name', 'N/A')}` / `#{self.summary.get('data_channel_name', 'N/A')}`\n"
                f"**Time Period Processed:** `{start_str}` to `{end_str}` (UTC)\n\n"
                f"**Fetch Results:**\n"
                f"- Messages Found: `{self.summary.get('messages_found', 0)}`\n"
                f"- New Messages Added to DB: `{self.summary.get('messages_added', 0)}`\n")

def main():
    """Main execution function for the fetch data script."""
    setup_logging(SCRIPT_NAME)
    config = load_config()
    
    raw_db_uri = config['databases']['raw_db_uri']
    engine = get_db_engine(raw_db_uri)
    if not engine:
        return
        
    create_raw_table(engine)
    start_date, end_date = get_date_range(config, engine)
    
    bot = DiscordFetchBot(config, start_date, end_date, engine)

    try:
        token = config['secrets']['discord_bot_token']
        if not token or "YOUR_DISCORD_BOT_TOKEN_HERE" in token:
            raise ValueError("Discord bot token is missing or has not been set in src/secrets.toml")
        
        logging.info("Starting Discord bot to fetch data...")
        bot.run(token)
        
    except ValueError as e:
        logging.critical(str(e))
    except discord.errors.LoginFailure:
        logging.critical("Login failed: Improper token provided. Check your discord_bot_token in src/secrets.toml")
    except Exception as e:
        logging.critical(f"An unexpected error occurred: {e}", exc_info=True)
    finally:
        if engine:
            engine.dispose()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
