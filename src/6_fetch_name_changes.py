# src/6_fetch_name_changes.py

import discord
import asyncio
import re
from sqlalchemy import create_engine, text, inspect, exc
from datetime import datetime, timezone
from loguru import logger

from shared_utils import load_config, post_to_discord_webhook, write_summary_file, PROJECT_ROOT
from loguru_setup import loguru_setup

SCRIPT_NAME = "6_fetch_name_changes"
DB_PATH = PROJECT_ROOT / "data" / "name_changes.db"

def setup_database(engine):
    """Ensures the name_changes table exists with the correct schema."""
    with engine.connect() as connection:
        with connection.begin():
            if not inspect(engine).has_table("name_changes"):
                logger.info("Creating 'name_changes' table for the first time.")
                connection.execute(text("""
                    CREATE TABLE name_changes (
                        discord_message_id INTEGER PRIMARY KEY,
                        old_name TEXT NOT NULL,
                        new_name TEXT NOT NULL,
                        change_timestamp TEXT NOT NULL
                    )
                """))
            if not inspect(engine).has_table("sync_metadata"):
                logger.info("Creating 'sync_metadata' table for the first time.")
                connection.execute(text("""
                    CREATE TABLE sync_metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    )
                """))

def get_last_processed_message_id(engine):
    """Retrieves the ID of the last message processed from the database."""
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT value FROM sync_metadata WHERE key = 'last_message_id'")).scalar_one_or_none()
            return int(result) if result else None
    except (exc.OperationalError, ValueError):
        # This can happen if the table or row doesn't exist yet
        return None

def update_last_processed_message_id(engine, message_id):
    """Saves the ID of the most recent message processed to the database."""
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text("""
                INSERT INTO sync_metadata (key, value) VALUES ('last_message_id', :id)
                ON CONFLICT(key) DO UPDATE SET value = :id
            """), {"id": str(message_id)})

async def fetch_and_parse_name_changes(config):
    """Connects to Discord, fetches messages, parses them, and saves to the DB."""
    bot_token = config.get('secrets', {}).get('discord_bot_token')
    channel_id = config.get('secrets', {}).get('discord_WOM_updates_channel_id')

    if not bot_token or not channel_id:
        logger.critical("Discord bot token or WOM channel ID is not configured in secrets.toml.")
        return "Discord credentials not configured.", 0

    engine = create_engine(f"sqlite:///{DB_PATH}")
    setup_database(engine)
    
    last_id = get_last_processed_message_id(engine)
    logger.info(f"Starting fetch. Last processed message ID: {last_id or 'None (full history scan)'}")

    intents = discord.Intents.default()
    intents.message_content = True # Required to read message content
    intents.messages = True # Required to read channel history

    client = discord.Client(intents=intents)
    
    new_changes_found = 0
    summary_message = ""

    try:
        await client.login(bot_token)
        channel = await client.fetch_channel(int(channel_id))
        
        latest_message_id_this_run = last_id
        
        # The 'after' parameter is exclusive, so we use the last saved ID
        async for message in channel.history(limit=None, after=discord.Object(id=last_id) if last_id else None, oldest_first=True):
            if not message.embeds:
                continue

            for embed in message.embeds:
                if embed.title == "Member Name Changed" and embed.description:
                    # Regex to capture 'old_name → new_name'
                    match = re.search(r"(.+?)\s*→\s*(.+)", embed.description)
                    if match:
                        old_name = match.group(1).strip()
                        new_name = match.group(2).strip()
                        change_time = message.created_at.replace(tzinfo=timezone.utc).isoformat()
                        
                        try:
                            with engine.connect() as connection:
                                with connection.begin():
                                    connection.execute(text("""
                                        INSERT INTO name_changes (discord_message_id, old_name, new_name, change_timestamp)
                                        VALUES (:msg_id, :old, :new, :ts)
                                    """), {"msg_id": message.id, "old": old_name, "new": new_name, "ts": change_time})
                                    
                                    logger.success(f"Found name change: {old_name} -> {new_name} at {change_time}")
                                    new_changes_found += 1
                        except exc.IntegrityError:
                            # This will happen if we re-process a message, which is fine.
                            logger.trace(f"Message ID {message.id} already processed. Ignoring.")
                            continue
            
            # Keep track of the latest message ID we've seen in this run
            if latest_message_id_this_run is None or message.id > latest_message_id_this_run:
                latest_message_id_this_run = message.id

        if latest_message_id_this_run and (last_id is None or latest_message_id_this_run > last_id):
            update_last_processed_message_id(engine, latest_message_id_this_run)
            logger.info(f"Updated last processed message ID to: {latest_message_id_this_run}")

        summary_message = f"Scan complete. Found and saved **{new_changes_found}** new name changes."

    except discord.errors.LoginFailure:
        summary_message = "Discord login failed. Please check your `discord_bot_token`."
        logger.critical(summary_message)
    except Exception as e:
        summary_message = f"An unexpected error occurred: {e}"
        logger.critical(summary_message, exc_info=True)
    finally:
        await client.close()
        engine.dispose()

    return summary_message, new_changes_found

async def main():
    config = load_config()
    loguru_setup(config, PROJECT_ROOT)
    logger.info(f"{f' Starting {SCRIPT_NAME} ':=^80}")

    summary_text, count = await fetch_and_parse_name_changes(config)
    
    summary = (
        f"**✅ {config.get('general', {}).get('project_name', 'Unnamed Project')}: {SCRIPT_NAME} Complete**\n\n"
        f"**Result:**\n{summary_text}"
    )
    write_summary_file(SCRIPT_NAME, summary)
    
    webhook_url = config.get('secrets', {}).get('discord_webhook_url')
    if summary and webhook_url:
        post_to_discord_webhook(webhook_url, summary)
    
    logger.info(f"{f' Finished {SCRIPT_NAME} ':=^80}")

if __name__ == "__main__":
    asyncio.run(main())