import asyncio
import json
import os
import sys
from pathlib import Path
import pandas as pd

import discord
from loguru import logger
try:
    import tomllib
except ImportError:
    import tomli as tomllib

from shared_utils import (
    load_config, PROJECT_ROOT, get_db_engine
)
from loguru_setup import loguru_setup

# --- Constants & Paths ---
SRC_ROOT = PROJECT_ROOT / 'src'
DATA_DIR = PROJECT_ROOT / 'data'
SECRETS_PATH = SRC_ROOT / 'secrets.toml'
STATE_FILE_PATH = DATA_DIR / 'discord_pb_message_ids.json'

# --- Ensure Directories Exist ---
DATA_DIR.mkdir(exist_ok=True)

# --- Helper Functions ---

def load_state(path: Path):
    """Loads message ID state from a JSON file."""
    if not path.exists():
        return {}
    try:
        with path.open('r', encoding='utf-8') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Could not load state file at '{path}'. Starting fresh. Error: {e}")
        return {}

def save_state(path: Path, state: dict):
    """Saves message ID state to a JSON file."""
    try:
        with path.open('w', encoding='utf-8') as f:
            json.dump(state, f, indent=4)
    except IOError as e:
        logger.error(f"Error: Could not save state to '{path}': {e}")

def create_embed_for_group(group, has_records=True, timestamp=None):
    """Creates a discord.Embed object for a given group of records."""
    embed = discord.Embed(
        title=group.get('title', 'Personal Bests'),
        color=discord.Color.blue() if has_records else discord.Color.dark_grey()
    )

    if not has_records:
        embed.description = "No records to display in this category."
        if timestamp:
            embed.timestamp = timestamp
            embed.set_footer(text="Updated")
        return embed
    
    description_parts = []
    for record in group.get('records', []):
        record_name = record.get('name', 'Unnamed Record')
        time = record.get('time', 'N/A')
        holder = record.get('holder', [])
        date = record.get('date')  # Will be None if not set
        discord_emoji = record.get('discord_emoji', '')

        holder_str = ", ".join(holder) if holder else 'N/A'

        # Build the lines for this specific record
        record_details = [
            f"**Time:** {time}",
            f"**Holder(s):** {holder_str}"
        ]
        if date:
            record_details.append(f"**Date:** {date}")

        # Build the title line, adding the emoji if it exists
        title_line = f"**{record_name}**"
        if discord_emoji:
            title_line += f" {discord_emoji}"

        part = f"{title_line}\n" + "\n".join(record_details)
        description_parts.append(part)

    description = "\n\n".join(description_parts)
    if len(description) > 4096: # Discord's description character limit
        description = description[:4090] + "\n...*truncated*"
        logger.warning(f"Embed description for '{group.get('title')}' was truncated as it exceeded the 4096 character limit.")
    
    embed.description = description
    if timestamp:
        # The official way to add a timestamp to an embed.
        # It will appear next to the footer text (e.g., "Updated • Today at 5:30 PM")
        embed.timestamp = timestamp
        embed.set_footer(text="Updated")
    return embed

# --- Main Logic ---

class PBPosterClient(discord.Client):
    def __init__(self, *, intents: discord.Intents, pb_config: dict, pb_df: pd.DataFrame, secrets: dict, state: dict):
        super().__init__(intents=intents)
        self.pb_config = pb_config
        self.state = state
        self.pb_df = pb_df
        self.secrets = secrets

    async def on_ready(self):
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')
        logger.info('------')

        try:
            await self.update_pbs()
        except Exception as e:
            logger.error(f"An unexpected error occurred during update: {e}", exc_info=True)
        finally:
            logger.info("Processing complete. Closing connection.")
            await self.close()

    async def update_pbs(self):
        """Fetches, creates, or edits PB messages in the configured channel."""
        channel_id_val = self.secrets.get('discord_pb_channel_id')
        if not channel_id_val or ("YOUR_DATA_CHANNEL_ID_HERE" in str(channel_id_val)):
            logger.error(f"Error: 'discord_pb_channel_id' not set in '{SECRETS_PATH.name}'.")
            return

        try:
            channel_id = int(channel_id_val)
        except (ValueError, TypeError):
            logger.error(f"Error: 'discord_pb_channel_id' ID '{channel_id_val}' is not a valid integer.")
            return

        channel = self.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            logger.error(f"Error: Channel with ID {channel_id} not found or is not a text channel.")
            return

        logger.info(f"Operating in channel: #{channel.name} ({channel.id})")

        # Get a single timestamp for this entire update run for consistency
        update_timestamp = pd.Timestamp.now().to_pydatetime()

        db_records_map = self.pb_df.set_index('Task').to_dict('index') if not self.pb_df.empty else {}

        # Start with the list of groups defined in the TOML config
        group_definitions = list(self.pb_config.get('groups', []))

        # --- Always add the "Miscellaneous/Other" group to the list to be processed ---
        other_group_name = self.pb_config.get('other_group_name', 'Miscellaneous PBs')
        other_group_df = self.pb_df[self.pb_df['Group'] == other_group_name]

        # Create a list of record definitions from the tasks, sorted alphabetically for consistency.
        # This will be an empty list if there are no misc records, which is the desired behavior.
        other_records = [{'name': task} for task in sorted(other_group_df['Task'].unique())]
        
        group_definitions.append({
            'title': other_group_name,
            'Image': None, # No image for the misc group
            'records': other_records
        })

        # Now loop through the potentially expanded list of groups
        for group_from_toml in group_definitions:
            group_title = group_from_toml.get('title')
            if not group_title:
                logger.warning("Skipping a group with no title.")
                continue

            # --- Build the group data for the embed, merging DB data ---
            embed_group_data = {
                'title': group_title,
                'Image': group_from_toml.get('Image'),
                'records': []
            }

            # Iterate through the records defined in the TOML to maintain order
            for record_from_toml in group_from_toml.get('records', []):
                task_name = record_from_toml.get('name')
                if not task_name:
                    continue

                # Get the emoji from the TOML record. It might be blank.
                discord_emoji = record_from_toml.get('discord_emoji', '')

                db_record = db_records_map.get(task_name)
                if db_record:
                    # Use the up-to-date data from the database
                    # The 'Holder' column is a comma-separated string, convert to list
                    holder_val = db_record.get('Holder', '')
                    holder_list = [h.strip() for h in holder_val.split(',')] if holder_val else []

                    embed_group_data['records'].append({
                        'name': task_name,
                        'time': db_record.get('Time', '0:00'),
                        'holder': holder_list,
                        'date': db_record.get('Date'),
                        'discord_emoji': discord_emoji
                    })
                else:
                    # No record in the DB for this task, use a default placeholder
                    embed_group_data['records'].append({
                        'name': task_name,
                        'time': '0:00',
                        'holder': [],
                        'date': None,
                        'discord_emoji': discord_emoji
                    })

            # Check if any of the records we are about to display have a holder.
            # This correctly handles cases where a group exists but all its records are empty (e.g. after blacklisting).
            has_db_records = any(r.get('holder') for r in embed_group_data['records'])

            message_id = self.state.get(group_title)
            embed = create_embed_for_group(embed_group_data, has_records=has_db_records, timestamp=update_timestamp)
            
            image_path_str = group_from_toml.get('Image')
            discord_file = None
            if image_path_str:
                # Image paths in config are relative to the project root
                image_path = PROJECT_ROOT / image_path_str
                if image_path.exists():
                    file_name = image_path.name
                    discord_file = discord.File(image_path, filename=file_name)
                    embed.set_thumbnail(url=f"attachment://{file_name}")
                else:
                    logger.warning(f"Warning: Image file not found at '{image_path}' for group '{group_title}'.")

            try:
                if message_id:
                    message = await channel.fetch_message(message_id)
                    # Note: Editing a message does not allow changing the attached file.
                    # If you change the image in the config, you must delete the old
                    # message in Discord to force the script to post a new one with the new image.
                    await message.edit(embed=embed)
                    logger.info(f"Updated embed for '{group_title}'.")
                else:
                    new_message = await channel.send(embed=embed, file=discord_file)
                    self.state[group_title] = new_message.id
                    logger.info(f"Posted new embed for '{group_title}' (Message ID: {new_message.id}).")

            except discord.NotFound:
                logger.warning(f"Message for '{group_title}' not found (ID: {message_id}). Posting a new one.")
                new_message = await channel.send(embed=embed, file=discord_file)
                self.state[group_title] = new_message.id
            except discord.Forbidden as e:
                logger.error(f"Error: Insufficient permissions for group '{group_title}'. Check bot permissions. Details: {e}")
            except discord.HTTPException as e:
                logger.error(f"Error: An HTTP error occurred for group '{group_title}'. Details: {e}")

async def main():
    """Main entry point for the script."""
    config = load_config()
    loguru_setup(config, PROJECT_ROOT)
    logger.info(f"{f' Starting 5_post_pbs_to_discord.py ':=^80}")

    secrets = config.get('secrets', {})
    token_val = secrets.get('discord_bot_token')
    if not token_val or "YOUR_DISCORD_BOT_TOKEN_HERE" in str(token_val):
        logger.critical(f"Error: Bot token not found or not set in '{SECRETS_PATH}'")
        sys.exit(1)

    pb_config_filename = config.get('historical_data', {}).get('personal_bests_file')
    if not pb_config_filename:
        logger.critical("Error: 'personal_bests_file' not defined in [historical_data] section of config.toml")
        sys.exit(1)
    pb_config_path = SRC_ROOT / pb_config_filename

    try:
        with open(pb_config_path, "rb") as f:
            pb_config = tomllib.load(f)
        logger.info(f"Successfully loaded PB config from {pb_config_path.name}")
    except FileNotFoundError:
        logger.critical(f"Error: PB config file not found at '{pb_config_path}'")
        sys.exit(1)
    except tomllib.TOMLDecodeError as e:
        logger.critical(f"Error: Could not parse TOML file at '{pb_config_path}': {e}")
        sys.exit(1)

    # --- Load Processed PB Data from Database ---
    optimised_db_uri = config.get('databases', {}).get('optimised_db_uri')
    if not optimised_db_uri:
        logger.critical("Error: 'optimised_db_uri' not defined in [databases] section of config.toml")
        sys.exit(1)

    optimised_engine = get_db_engine(optimised_db_uri)
    pb_df = pd.DataFrame() # Default to empty DataFrame
    if optimised_engine:
        try:
            pb_df = pd.read_sql_table('personal_bests_summary', optimised_engine)
            logger.info(f"Successfully loaded {len(pb_df)} processed PBs from the database.")
        except ValueError as e:
            # This happens if the table doesn't exist
            logger.warning(f"Could not load 'personal_bests_summary' table. It may not have been created yet. Error: {e}")
        finally:
            optimised_engine.dispose()
    else:
        logger.error("Could not create database engine for optimised DB. Proceeding without PB data.")

    state = load_state(STATE_FILE_PATH)
    
    client = PBPosterClient(
        intents=discord.Intents.default(),
        pb_config=pb_config,
        pb_df=pb_df,
        secrets=secrets,
        state=state
    )

    try:
        await client.start(token_val)
    except Exception as e:
        logger.critical(f"An unhandled exception occurred in the PB Poster client: {e}", exc_info=True)
    finally:
        save_state(STATE_FILE_PATH, client.state)
        logger.info("State saved.")
        logger.info(f"{f' Finished 5_post_pbs_to_discord.py ':=^80}")

if __name__ == '__main__':
    asyncio.run(main())
