# src/main.py

import discord
import pandas as pd
import toml
import json
import re
import asyncio
from sqlalchemy import create_engine, inspect, text, event
from pathlib import Path
from datetime import datetime, timedelta, timezone
import os
import shutil
import logging
import sys

# --- Constants & Paths ---
# Establishes the project's directory structure.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = PROJECT_ROOT / 'src'
REPORTS_DIR = PROJECT_ROOT / 'reports'
DATA_DIR = PROJECT_ROOT / 'data'
CONFIG_PATH = SRC_ROOT / 'config.toml'
SECRETS_PATH = SRC_ROOT / 'secrets.toml' # New path for secrets
STATE_PATH = DATA_DIR / 'sync_state.json'

# --- Helper Functions ---

def setup_logging(log_file_path=None):
    """Configures logging to print to console and optionally to a file."""
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)-8s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    root_logger = logging.getLogger()
    
    if not root_logger.hasHandlers():
        root_logger.setLevel(logging.INFO)

    # Close existing file handlers to prevent log file locking on re-runs
    for handler in root_logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
             handler.close()
             root_logger.removeHandler(handler)

    # Ensure a console handler is present
    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(log_formatter)
        root_logger.addHandler(console_handler)

    # Add file handler if a path is provided
    if log_file_path:
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setFormatter(log_formatter)
        root_logger.addHandler(file_handler)

def load_config():
    """Loads configuration from config.toml and secrets.toml."""
    logging.info("Loading configuration...")
    try:
        # Load main config
        config = toml.load(CONFIG_PATH)
        logging.info("--> config.toml loaded successfully.")

        # Load secrets and merge them into the main config
        secrets = toml.load(SECRETS_PATH)
        config['secrets'] = secrets
        logging.info("--> secrets.toml loaded successfully.")
        
        return config
    except FileNotFoundError as e:
        if 'secrets.toml' in str(e):
            logging.error(f"FATAL: The secrets file was not found at {SECRETS_PATH}")
            logging.error("Please copy 'secrets.example.toml' to 'secrets.toml' and fill in your details.")
        else:
            logging.error(f"FATAL: A configuration file was not found: {e}")
        return None
    except Exception as e:
        logging.error(f"Failed to parse a config file: {e}")
        return None

def validate_patterns(patterns_config):
    """Validates the new patterns structure from the config file."""
    logging.info("Validating parsing patterns from config...")
    is_config_valid = True
    for name, group_def in patterns_config.items():
        if not isinstance(group_def, dict):
            logging.warning(f"--> Config Warning: Pattern group '{name}' is not a valid section. Skipping.")
            is_config_valid = False
            continue
        
        # The 'Chat' pattern has a different structure, so we check it separately
        if name.lower() == 'chat':
            if 'type' not in group_def or 'regex' not in group_def or 'columns' not in group_def:
                logging.warning(f"--> Config Warning: The 'Chat' pattern is missing required keys (type, regex, columns).")
                is_config_valid = False
            continue

        if "broadcast_type" not in group_def:
            logging.warning(f"--> Config Warning: Pattern group '{name}' is missing 'broadcast_type'.")
            is_config_valid = False
        
        if "variants" not in group_def or not isinstance(group_def['variants'], list):
            logging.warning(f"--> Config Warning: Pattern group '{name}' is missing a list of 'variants'.")
            is_config_valid = False
            continue

        for i, variant in enumerate(group_def['variants']):
            if not isinstance(variant, dict):
                logging.warning(f"--> Config Warning: Variant #{i+1} in group '{name}' is not a valid section.")
                is_config_valid = False
                continue
            
            required_keys = ["regex", "columns"]
            missing_keys = [key for key in required_keys if key not in variant]
            if missing_keys:
                logging.warning(f"--> Config Warning: Variant #{i+1} in group '{name}' is missing required key(s): {', '.join(missing_keys)}")
                is_config_valid = False

            if "regex" in variant and variant["regex"]:
                try:
                    re.compile(variant["regex"])
                except re.error as e:
                    logging.warning(f"--> Config Warning: Variant #{i+1} in group '{name}' has an invalid regex: {e}")
                    is_config_valid = False
            elif "regex" not in variant or not variant["regex"]:
                 logging.warning(f"--> Config Warning: Variant #{i+1} in group '{name}' is missing a regex value.")
                 is_config_valid = False

    if is_config_valid:
        logging.info("--> All parsing patterns appear to be valid.")
    else:
        logging.warning("--> One or more parsing patterns have issues. The script may not parse all messages correctly.")
    return is_config_valid

def load_state():
    """Loads the last run timestamp from the state file."""
    logging.info("Loading previous run state...")
    try:
        with open(STATE_PATH, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.info("State file not found. Assuming this is the first run.")
        return {}
    except json.JSONDecodeError:
        logging.error("Could not decode state file. It might be corrupted.")
        return {}

def save_state(data):
    """Saves the current run's end time to the state file."""
    logging.info("Saving new run state...")
    DATA_DIR.mkdir(exist_ok=True)
    with open(STATE_PATH, 'w') as f:
        json.dump(data, f, indent=4)

def get_date_range(config, state):
    """Determines the start and end dates for the data fetch based on config."""
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
    
    end_date = now - timedelta(minutes=config['time_settings']['end_time_offset_minutes'])
    
    last_run_str = state.get('last_successful_run')
    if last_run_str:
        last_run_date = datetime.fromisoformat(last_run_str)
        start_date = last_run_date - timedelta(minutes=config['time_settings']['start_time_overlap_minutes'])
        logging.info(f"--> Last run was at {last_run_date.strftime('%Y-%m-%d %H:%M')}. Fetching data since {start_date.strftime('%Y-%m-%d %H:%M')} UTC.")
    else:
        start_date = now - timedelta(days=config['time_settings']['max_lookback_days'])
        logging.info(f"--> First run. Fetching data for the last {config['time_settings']['max_lookback_days']} days.")
        
    return start_date, end_date

def clean_discord_escapes(text):
    """Selectively removes Discord's escape backslashes before punctuation."""
    return re.sub(r'\\([^\w\s])', r'\1', text)

def create_db_engine(config):
    """Creates a SQLAlchemy engine based on the URI in the config."""
    logging.info("Connecting to the database...")
    try:
        db_uri = config['secrets']['database_uri']
        if db_uri.startswith('sqlite'):
            # Path is relative to the /src directory where the script is, so ../data points correctly.
            relative_path_part = db_uri.split('///')[1]
            db_file_path = (SRC_ROOT / relative_path_part).resolve()
            db_file_path.parent.mkdir(parents=True, exist_ok=True)
            engine = create_engine(f"sqlite:///{db_file_path}")
            logging.info(f"--> SQLite database configured at: {db_file_path}")
        else:
            engine = create_engine(db_uri)
            logging.info("--> Remote PostgreSQL database configured.")
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}", exc_info=True)
        return None

def get_database_size(engine, db_uri):
    """Calculates the size of the database and returns it as a formatted string."""
    logging.info("Calculating database size...")
    size_mb = 0
    try:
        if db_uri.startswith('sqlite'):
            # For SQLite, get the file size
            relative_path_part = db_uri.split('///')[1]
            db_file_path = (SRC_ROOT / relative_path_part).resolve()
            if db_file_path.exists():
                size_bytes = os.path.getsize(db_file_path)
                size_mb = size_bytes / (1024 * 1024)
                logging.info(f"--> SQLite database file size: {size_mb:.2f} MB")
            else:
                logging.warning("--> SQLite database file not found, cannot determine size.")
                return "N/A"
        else:
            # For PostgreSQL, execute a query
            with engine.connect() as connection:
                # Supabase's default database name is 'postgres'
                query = text("SELECT pg_database_size(current_database());")
                result = connection.execute(query).scalar_one()
                size_mb = result / (1024 * 1024)
                logging.info(f"--> PostgreSQL database size: {size_mb:.2f} MB")
        
        return f"{size_mb:.2f} MB"

    except Exception as e:
        logging.error(f"Could not calculate database size: {e}", exc_info=True)
        return "Error"

def create_database_tables(engine, schema_config):
    """Creates database tables dynamically based on the provided schema config."""
    logging.info("Verifying database tables exist...")
    inspector = inspect(engine)
    with engine.connect() as connection:
        for table_name, columns in schema_config.items():
            if not inspector.has_table(table_name):
                logging.info(f"Table '{table_name}' not found. Creating it...")
                cols_str = ", ".join([f'"{col_name}" {col_type}' for col_name, col_type in columns.items()])
                create_sql = text(f'CREATE TABLE "{table_name}" ({cols_str})')
                try:
                    connection.execute(create_sql)
                    connection.commit()
                    logging.info(f"--> Table '{table_name}' created successfully.")
                except Exception as e:
                     logging.error(f"Could not create table '{table_name}': {e}", exc_info=True)
                     raise
            else:
                db_cols = {col['name'] for col in inspector.get_columns(table_name)}
                config_cols = set(columns.keys())
                if db_cols != config_cols:
                    logging.warning(f"Schema mismatch for table '{table_name}'!")
                    missing_in_db = config_cols - db_cols
                    extra_in_db = db_cols - config_cols
                    if missing_in_db: logging.warning(f"--> Missing columns in database: {missing_in_db}")
                    if extra_in_db: logging.warning(f"--> Extra columns in database: {extra_in_db}")
        logging.info("--> Database tables verified.")


def cleanup_old_reports(config):
    """Deletes the oldest report folders if the total number exceeds the configured limit."""
    max_folders = config['cleanup_settings'].get('max_report_folders', 50)
    if max_folders == 0:
        logging.info("Report cleanup is disabled.")
        return

    logging.info(f"Checking for old reports to clean up (limit: {max_folders})...")
    if not REPORTS_DIR.exists(): return

    report_folders = [d for d in REPORTS_DIR.iterdir() if d.is_dir()]
    
    if len(report_folders) > max_folders:
        report_folders.sort(key=lambda x: x.name)
        
        num_to_delete = len(report_folders) - max_folders
        logging.info(f"--> Found {len(report_folders)} report folders. Deleting the oldest {num_to_delete}...")
        
        for i in range(num_to_delete):
            folder_to_delete = report_folders[i]
            try:
                shutil.rmtree(folder_to_delete)
                logging.info(f"  - Deleted: {folder_to_delete.name}")
            except OSError as e:
                logging.error(f"Could not delete {folder_to_delete.name}: {e}")

class DiscordSyncBot(discord.Client):
    """The main class that handles Discord connection, data fetching, and processing."""
    def __init__(self, config, start_date, end_date):
        intents = discord.Intents.default(); intents.messages = True; intents.message_content = True
        super().__init__(intents=intents)
        self.config = config
        self.start_date = start_date
        self.end_date = end_date
        self.results = {}
        self.debug_mode = self.config.get('general', {}).get('debug_mode', False)

    async def on_ready(self):
        """Called when the bot successfully logs in."""
        logging.info(f'--> Logged in as {self.user} to perform sync.')
        try:
            await self.fetch_and_parse_data()
            await self.save_csv_files()
            await self.update_database()
            
            db_size_str = get_database_size(self.config['db_engine'], self.config['secrets']['database_uri'])
            self.results['database_size'] = db_size_str

            summary_message = self.format_summary_message(success=True)
            await self.post_log_message(summary_message)
            await self.write_final_summary_log(summary_message)

        except Exception as e:
            logging.error(f"FATAL ERROR during operation: {e}", exc_info=True)
            self.results['error'] = str(e)
            error_message = self.format_summary_message(success=False)
            await self.post_log_message(error_message)
        finally:
            logging.info("Operation complete. Logging out.")
            await self.close()

    def _apply_mappings(self, definition, groups):
        """Applies column mappings from config to regex groups."""
        details = {}
        columns = definition.get("columns", [])
        numeric_columns = ['Item_Value', 'Pet_KC', 'New_Level']

        for i, col_name in enumerate(columns):
            if i < len(groups) and col_name != "_":
                value = groups[i]
                if col_name in numeric_columns:
                    try:
                        details[col_name] = int(str(value).replace(',', ''))
                    except (ValueError, TypeError):
                        details[col_name] = None
                else:
                    details[col_name] = value.strip() if isinstance(value, str) else value
        return details

    async def fetch_and_parse_data(self):
        """Fetches messages from Discord and parses them using the config-driven engine."""
        data_channel_id = self.config['secrets']['discord_data_channel_id']
        logging.info(f"Fetching messages from channel ID: {data_channel_id}...")
        data_channel = self.get_channel(int(data_channel_id))
        if not data_channel: raise Exception(f"Could not find data channel with ID {data_channel_id}.")
        self.results['data_channel_name'] = data_channel.name
        logging.info(f"--> Found channel: #{data_channel.name}")
        
        messages_to_process = []
        fetch_counter = 0
        async for message in data_channel.history(limit=None, after=self.start_date, before=self.end_date, oldest_first=True):
            messages_to_process.append(message)
            fetch_counter += 1
            if fetch_counter % 250 == 0: logging.info(f"  - Discovered {fetch_counter} messages...")
        
        logging.info(f"--> Found {len(messages_to_process)} total messages. Now parsing...")
        self.results['messages_found'] = len(messages_to_process)
        
        raw_dump_file = None
        if self.debug_mode and 'report_folder_path' in self.results:
            raw_dump_path = self.results['report_folder_path'] / "raw_message_dump.txt"
            logging.info(f"Debug mode is ON. Cleaned messages will be saved to {raw_dump_path}")
            raw_dump_file = open(raw_dump_path, 'w', encoding='utf-8')

        parsed_chat, parsed_broadcasts, unparsed_messages = [], [], []
        patterns = self.config.get('patterns', {})
        
        for i, msg in enumerate(messages_to_process):
            if (i + 1) % 500 == 0: logging.info(f"  - Parsed {i + 1}/{len(messages_to_process)} messages...")
            if not msg.content: continue
            
            clean_raw_content = clean_discord_escapes(msg.content)
            
            if raw_dump_file:
                raw_dump_file.write(clean_raw_content + '\n')
            
            is_parsed = False
            details = None
            failure_reason = "No matching regex pattern found."
            
            chat_def = patterns.get("Chat", {})
            chat_regex = chat_def.get("regex", "^$")
            chat_match = re.search(chat_regex, clean_raw_content)

            if chat_match:
                details = self._apply_mappings(chat_def, chat_match.groups())
                details["MessageType"] = "Chat"
                details["Timestamp"] = msg.created_at 
                parsed_chat.append(details)
                is_parsed = True
            else:
                for name, group_def in patterns.items():
                    if name.lower() == 'chat': continue
                    broadcast_type = group_def.get("broadcast_type")
                    if not broadcast_type: continue

                    for variant in group_def.get("variants", []):
                        regex_to_search = variant.get("regex")
                        if not regex_to_search: continue
                        
                        match = re.search(regex_to_search, clean_raw_content)
                        if match:
                            details = self._apply_mappings(variant, match.groups())
                            validation_passed = True
                            required_columns = variant.get("required_columns", [])
                            for col_name in required_columns:
                                value = details.get(col_name)
                                if value is None or str(value).strip() == '':
                                    failure_reason = f"Required column '{col_name}' is blank."
                                    logging.warning(f"Parse Warning: {failure_reason} | Content: '{clean_raw_content}'")
                                    validation_passed = False
                                    details = None
                                    break
                            
                            if not validation_passed:
                                continue

                            details["Broadcast_Type"] = broadcast_type
                            is_parsed = True
                            break
                    if is_parsed:
                        break
            
            if is_parsed and details and details.get("MessageType") != "Chat":
                base_details = {"Timestamp": msg.created_at, "Content": clean_raw_content}
                details.update(base_details)
                details["Parsed_Successfully"] = True
                
                if details.get("Broadcast_Type") == "Pet": details["Is_Dupe"] = "would have been" in clean_raw_content
                if details.get("Broadcast_Type") == "Total Level": details["Skill"] = "Total"
                
                parsed_broadcasts.append(details)
            elif not is_parsed:
                 unparsed_messages.append({"Timestamp": msg.created_at, "Content": clean_raw_content, "Failure_Reason": failure_reason})
                 if self.debug_mode:
                     logging.info(f"[DEBUG] FAILED to parse: '{clean_raw_content}'. Reason: {failure_reason}")
        
        if raw_dump_file:
            raw_dump_file.close()

        self.df_chat = pd.DataFrame(parsed_chat)
        self.df_broadcasts = pd.DataFrame(parsed_broadcasts)
        self.df_unparsed = pd.DataFrame(unparsed_messages)
        
        self.results['chat_found'] = len(self.df_chat)
        self.results['broadcasts_found'] = len(self.df_broadcasts)
        self.results['unparsed_found'] = len(self.df_unparsed)
        logging.info(f"--> Parsing complete. Found {self.results['unparsed_found']} unparsed messages.")


    async def save_csv_files(self):
        """Saves the fetched data to local CSV files for audit."""
        logging.info("Saving CSV data to local report folder...")
        if 'report_folder_path' not in self.results:
            raise Exception("Report folder path not set. Cannot save CSVs.")
            
        output_folder_path = self.results['report_folder_path']
        
        if not self.df_chat.empty:
            self.df_chat.to_csv(output_folder_path / "chat_only.csv", index=False, encoding='utf-8-sig')

        if not self.df_broadcasts.empty:
            broadcast_schema_cols = list(self.config.get('database_schema', {}).get('clan_broadcasts', {}).keys())
            current_cols = list(self.df_broadcasts.columns)
            ordered_cols = [col for col in broadcast_schema_cols if col in current_cols]
            ordered_cols.extend([col for col in current_cols if col not in ordered_cols])
            self.df_broadcasts[ordered_cols].to_csv(output_folder_path / "clan_broadcasts.csv", index=False, encoding='utf-8-sig')

        if not self.df_unparsed.empty:
            self.df_unparsed.to_csv(output_folder_path / "unparsed_log.csv", index=False, encoding='utf-8-sig')
            
        logging.info("--> CSV files saved.")
    
    async def write_final_summary_log(self, summary_text):
        """Writes the final summary to a text file in the report folder."""
        if 'report_folder_path' in self.results:
            summary_path = self.results['report_folder_path'] / "run_summary.txt"
            logging.info(f"Writing final summary to {summary_path}")
            with open(summary_path, "w", encoding="utf-8") as f:
                f.write(summary_text)

    async def update_database(self):
        """Connects to the database and appends new, unique records."""
        logging.info("Starting database update...")
        db_engine = self.config['db_engine']
        
        create_database_tables(db_engine, self.config.get('database_schema', {}))
        
        self.results.update({'broadcasts_added': 0, 'chat_added': 0})
        
        logging.info("--> Processing clan broadcasts...")
        if not self.df_broadcasts.empty:
            self.df_broadcasts['Timestamp'] = self.df_broadcasts['Timestamp'].astype(str)
            existing_df = pd.read_sql('SELECT "Timestamp", "Content" FROM clan_broadcasts', db_engine)
            existing_set = set(zip(existing_df['Timestamp'], existing_df['Content']))
            self.df_broadcasts['unique_id'] = list(zip(self.df_broadcasts['Timestamp'], self.df_broadcasts['Content']))
            new_df = self.df_broadcasts[~self.df_broadcasts['unique_id'].isin(existing_set)]
            
            if not new_df.empty:
                rows_to_add = new_df.drop(columns=['unique_id'])
                db_cols = self.config.get('database_schema', {}).get('clan_broadcasts', {}).keys()
                for col in db_cols: 
                    if col not in rows_to_add.columns: rows_to_add[col] = None
                rows_to_add = rows_to_add[list(db_cols)]
                rows_to_add.to_sql('clan_broadcasts', db_engine, if_exists='append', index=False)
                self.results['broadcasts_added'] = len(rows_to_add)
        
        logging.info("--> Processing chat messages...")
        if not self.df_chat.empty:
            self.df_chat['Timestamp'] = self.df_chat['Timestamp'].astype(str)
            existing_df = pd.read_sql('SELECT "Timestamp", "Content" FROM chat', db_engine)
            existing_set = set(zip(existing_df['Timestamp'], existing_df['Content']))
            self.df_chat['unique_id'] = list(zip(self.df_chat['Timestamp'], self.df_chat['Content']))
            new_df = self.df_chat[~self.df_chat['unique_id'].isin(existing_set)]
            
            if not new_df.empty:
                rows_to_add = new_df.drop(columns=['unique_id'])
                rows_to_add.to_sql('chat', db_engine, if_exists='append', index=False)
                self.results['chat_added'] = len(rows_to_add)
        
        logging.info(f"--> Database update complete: Added {self.results.get('broadcasts_added', 0)} broadcasts and {self.results.get('chat_added', 0)} chat messages.")

    def format_summary_message(self, success):
        """Formats the text for the Discord log message."""
        project_name = self.config.get('general', {}).get('project_name', 'Unnamed Project')
        
        if not success:
            return (f"**❌ {project_name}: SYNC FAILED**\n"
                    f"**Error:**\n```\n{self.results.get('error', 'Unknown error')}\n```\n")
        
        start_str = self.start_date.strftime('%Y-%m-%d %H:%M')
        end_str = self.end_date.strftime('%Y-%m-%d %H:%M')
        channel_name = self.results.get('data_channel_name', 'N/A')
        
        warning_line = ""
        if not self.results.get('patterns_valid', True):
            warning_line += f"**⚠️ Config Warning:** Invalid parsing patterns detected. Check `console_run.log`.\n\n"

        unparsed_count = self.results.get('unparsed_found', 0)
        if unparsed_count > 0:
            warning_line += f"**⚠️ Unparsed Messages:** `{unparsed_count}` (Check `unparsed_log.csv` for details)\n\n"

        return (f"**✅ {project_name}: Sync Complete**\n\n"
                f"{warning_line}"
                f"**Report Folder:** `{self.results.get('report_folder_name', 'N/A')}`\n"
                f"**Data Source:** `#{channel_name}`\n"
                f"**Time Period Processed:** `{start_str}` to `{end_str}` (UTC)\n\n"
                f"**Discord Fetch:**\n- Broadcasts Found: `{self.results.get('broadcasts_found', 0)}`\n"
                f"- Chat Messages Found: `{self.results.get('chat_found', 0)}`\n\n"
                f"**Database Update:**\n- New Broadcasts Added: `{self.results.get('broadcasts_added', 0)}`\n"
                f"- New Chat Messages Added: `{self.results.get('chat_added', 0)}`\n"
                f"- Database Size: `{self.results.get('database_size', 'N/A')}`")

    async def post_log_message(self, message):
        """Posts the summary message to the configured Discord log channel."""
        logging.info("Posting summary to Discord log channel...")
        log_channel_id = self.config['secrets']['discord_log_channel_id']
        if not log_channel_id:
            logging.warning("No log channel ID configured. Skipping summary post.")
            return

        log_channel = self.get_channel(int(log_channel_id))
        if not log_channel:
            logging.error(f"Could not find log channel with ID {log_channel_id}.")
            return
        
        try:
            if len(message) > 2000:
                await log_channel.send(message[:1990] + '...')
            else:
                await log_channel.send(message)
            logging.info("--> Summary posted.")
        except discord.errors.Forbidden:
            logging.error(f"Could not post to log channel #{log_channel.name}. Check bot permissions.")
        except Exception as e:
            logging.error(f"An unexpected error occurred while posting log message: {e}")

# --- Main Execution Block ---
if __name__ == "__main__":
    setup_logging() 
    
    # Ensure root directories for data and reports exist
    DATA_DIR.mkdir(exist_ok=True)
    REPORTS_DIR.mkdir(exist_ok=True)

    config = load_config()
    if not config:
        sys.exit(1) # Exit if config fails to load

    patterns_are_valid = validate_patterns(config.get('patterns', {}))
    
    db_engine = create_db_engine(config)
    if not db_engine:
        sys.exit(1)
    config['db_engine'] = db_engine
    
    state = load_state()
    start_date, end_date = get_date_range(config, state)
    
    base_folder_name = f"Report_{end_date.strftime('%Y-%m-%d_%H-%M')}"
    output_folder_path = REPORTS_DIR / base_folder_name
    run_counter = 1
    while output_folder_path.exists():
        run_counter += 1
        output_folder_path = REPORTS_DIR / f"{base_folder_name}_run_{run_counter}"
    output_folder_path.mkdir()
    
    # Re-setup logging to include the new report file
    setup_logging(output_folder_path / "console_run.log")
    
    bot = DiscordSyncBot(config, start_date, end_date)
    bot.results['report_folder_path'] = output_folder_path
    bot.results['report_folder_name'] = output_folder_path.name
    bot.results['patterns_valid'] = patterns_are_valid

    try:
        token = config['secrets']['discord_bot_token']
        if not token or token == "YOUR_DISCORD_BOT_TOKEN_HERE":
            raise ValueError("Discord bot token is missing or has not been set in src/secrets.toml")
        
        logging.info("Starting Discord bot...")
        bot.run(token)
        
        # After bot.run() completes
        if bot.results.get('error'):
            logging.warning("Script finished with an error. State not updated.")
        else:
            if config['time_settings']['mode'] == 'automatic':
                save_state({'last_successful_run': end_date.isoformat()})
            
            cleanup_old_reports(config)
            logging.info("Script finished successfully.")
            
    except ValueError as e:
        logging.critical(str(e))
    except discord.errors.LoginFailure:
        logging.critical("Login failed: Improper token provided. Check your discord_bot_token in src/secrets.toml")
    except Exception as e:
        logging.critical(f"An unexpected error occurred while trying to run the bot: {e}", exc_info=True)
    finally:
        if db_engine:
            db_engine.dispose()
            logging.info("Database connection closed.")