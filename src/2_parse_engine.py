# src/2_parse_engine.py

import pandas as pd
import re
from sqlalchemy import text, inspect, exc
from loguru import logger

from shared_utils import (
    load_config, get_db_engine, write_summary_file, post_to_discord_webhook, PROJECT_ROOT
)
from loguru_setup import loguru_setup

SCRIPT_NAME = "2_parse_engine"

def apply_mappings(definition: dict, groups: tuple) -> dict:
    """Applies column mappings from config to regex groups."""
    details = {}
    columns = definition.get("columns", [])
    numeric_columns = ['Item_Value', 'Pet_KC', 'New_Level']

    for i, col_name in enumerate(columns):
        if i < len(groups) and col_name != "_":
            value = groups[i]
            if col_name in numeric_columns and value is not None:
                try:
                    details[col_name] = int(str(value).replace(',', ''))
                except (ValueError, TypeError):
                    details[col_name] = None
            else:
                details[col_name] = value.strip() if isinstance(value, str) else value
    return details

def parse_raw_data(df_raw: pd.DataFrame, config: dict) -> (pd.DataFrame, pd.DataFrame, pd.DataFrame):
    """Parses a DataFrame of raw logs using patterns from the config."""
    parsed_chat, parsed_broadcasts, unparsed_logs = [], [], []
    
    # Get necessary configs at the start
    patterns_config = config.get('patterns', {})
    item_value_overrides = config.get('item_value_overrides', {})

    logger.info(f"Starting to parse {len(df_raw)} raw messages...")
    if df_raw.empty:
        logger.info("--> No messages to parse.")
        # Return empty dataframes with correct columns to prevent KeyErrors later
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    for index, row in df_raw.iterrows():
        raw_log_id = row['id']
        clean_content = row['raw_content']
        timestamp = row['timestamp']
        is_parsed = False
        failure_reason = "No matching pattern found"

        # Try to parse as Chat first
        chat_def = patterns_config.get("Chat", {})
        if chat_def:
            chat_match = re.search(chat_def.get("regex", "^$"), clean_content)
            if chat_match:
                details = apply_mappings(chat_def, chat_match.groups())
                is_valid = all(details.get(col) is not None and str(details.get(col)).strip() != '' for col in chat_def.get("required_columns", []))
                
                if is_valid:
                    details.update({"raw_log_id": raw_log_id, "MessageType": "Chat", "Timestamp": timestamp})
                    parsed_chat.append(details)
                    is_parsed = True
                else:
                    failure_reason = "Chat message failed validation on required columns."

        # If not chat, try broadcast patterns
        if not is_parsed:
            for name, group_def in patterns_config.items():
                if name.lower() == 'chat' or 'broadcast_type' not in group_def:
                    continue
                
                for variant in group_def.get("variants", []):
                    match = re.search(variant.get("regex", "^$"), clean_content)
                    if match:
                        details = apply_mappings(variant, match.groups())
                        is_valid = all(details.get(col) is not None and str(details.get(col)).strip() != '' for col in variant.get("required_columns", []))
                        
                        if is_valid:
                            # --- NEW: Apply item value override right after parsing ---
                            if not details.get('Item_Value') and details.get('Item_Name'):
                                override_value = item_value_overrides.get(details['Item_Name'])
                                if override_value:
                                    details['Item_Value'] = override_value
                                    logger.trace(f"Applied value override for '{details['Item_Name']}': {override_value}")
                            # --- END NEW LOGIC ---

                            details.update({
                                "raw_log_id": raw_log_id,
                                "Broadcast_Type": group_def["broadcast_type"],
                                "Timestamp": timestamp,
                                "Content": clean_content
                            })
                            if details.get("Broadcast_Type") == "Total Level":
                                details["Skill"] = "Total"
                            
                            # --- Generalized Multi-User Splitting Logic ---
                            raw_username_str = details.get('Username')
                            
                            # Only proceed if a username was captured by the regex
                            if raw_username_str:
                                # Pre-process to handle malformed usernames like "UserAand UserB"
                                words = raw_username_str.split(' ')
                                new_words = []
                                for word in words:
                                    if word.lower().endswith('and') and len(word) > 3:
                                        new_words.append(word[:-3])
                                        new_words.append('and')
                                    else:
                                        new_words.append(word)
                                processed_username_str = ' '.join(new_words)

                                # Now check if the processed string looks like it has multiple users
                                if ',' in processed_username_str or ' and ' in processed_username_str:
                                    logger.debug(f"Potential multi-user broadcast detected for type '{group_def['broadcast_type']}'. Processed username string: '{raw_username_str}'")
                                    logger.debug(f'Names Found: {processed_username_str}')

                                    # Normalize separators by replacing commas, then split by ' and '
                                    normalized_str = processed_username_str.replace(',', ' and ')
                                    username_list = [name.strip() for name in normalized_str.split(' and ') if name.strip()]
                                    
                                    logger.debug(f"Split usernames into: {username_list}")

                                    for user in username_list:
                                        user_details = details.copy()
                                        user_details['Username'] = user
                                        logger.trace(f"Creating record for user: '{user}' in broadcast type '{group_def['broadcast_type']}'")
                                        parsed_broadcasts.append(user_details)
                                else:
                                    # This is a standard, single-user broadcast
                                    parsed_broadcasts.append(details)
                            else:
                                # No username was captured in this broadcast, just add it
                                parsed_broadcasts.append(details)

                            is_parsed = True
                            break
                        else:
                             failure_reason = f"Required column blank for Broadcast Type '{group_def['broadcast_type']}'."
                if is_parsed:
                    break

        if not is_parsed:
            unparsed_logs.append({
                'raw_log_id': raw_log_id,
                'timestamp': timestamp,
                'raw_content': clean_content,
                'failure_reason': failure_reason
            })

    df_chat = pd.DataFrame(parsed_chat)
    df_broadcasts = pd.DataFrame(parsed_broadcasts)
    df_unparsed = pd.DataFrame(unparsed_logs)
    
    logger.info(f"--> Parsing complete. Found {len(df_chat)} chat, {len(df_broadcasts)} broadcasts, and {len(df_unparsed)} unparsed messages from this run.")
    return df_chat, df_broadcasts, df_unparsed

def get_all_ids_from_table(engine, table_name, column_name="raw_log_id"):
    """Gets all IDs from a specific column in a table."""
    inspector = inspect(engine)
    if not inspector.has_table(table_name):
        return set()
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f'SELECT {column_name} FROM {table_name}'))
            return {row[0] for row in result}
    except Exception as e:
        logger.warning(f"Could not get IDs for {table_name}: {e}. Returning empty set.")
        return set()

def save_df_with_ignore(df: pd.DataFrame, table_name: str, engine):
    """Saves a DataFrame to the database, gracefully skipping rows that violate UNIQUE constraints."""
    if df.empty:
        return 0
    
    rows_added = 0
    with engine.connect() as connection:
        with connection.begin(): # Use a transaction
            for _, row in df.iterrows():
                try:
                    row_dict = row.to_dict()
                    cols = ', '.join(f'"{c}"' for c in row_dict.keys())
                    placeholders = ', '.join(f':{c}' for c in row_dict.keys())
                    stmt = text(f'INSERT INTO "{table_name}" ({cols}) VALUES ({placeholders})')
                    
                    connection.execute(stmt, row_dict)
                    rows_added += 1
                except exc.IntegrityError:
                    # This can happen if the row is a true duplicate (e.g. re-running the parser on old data)
                    # The UNIQUE constraint (e.g., on raw_log_id or a composite) prevents it.
                    logger.trace(f"Ignoring duplicate entry for table {table_name}, raw_log_id: {row.get('raw_log_id')}")
                    continue
                except Exception:
                    logger.error(f"Failed to insert row into {table_name}: {row.to_dict()}", exc_info=True)
    return rows_added

def main():
    """Main execution function for the parse engine script."""
    config = load_config()
    loguru_setup(config, PROJECT_ROOT)
    logger.info(f"{f' Starting {SCRIPT_NAME} ':=^80}")

    parse_mode = config.get('parse_settings', {}).get('mode', 'new')

    raw_engine = get_db_engine(config['databases']['raw_db_uri'])
    parsed_engine = get_db_engine(config['databases']['parsed_db_uri'])
    if not raw_engine or not parsed_engine: return

    summary = ""
    try:
        # Ensure tables exist with UNIQUE constraint on raw_log_id
        with parsed_engine.connect() as connection:
            with connection.begin():
                for table_name, columns in config['database_schema'].items():
                    if not inspect(parsed_engine).has_table(table_name):
                        cols_str = ", ".join([f'"{col_name}" {col_type}' for col_name, col_type in columns.items()])
                        # Use a composite UNIQUE constraint for tables with a Username to allow multiple
                        # records from a single raw_log_id (for multi-user broadcasts).
                        if 'Username' in columns:
                            unique_constraint = ', UNIQUE(raw_log_id, "Username")'
                        else:
                            unique_constraint = ', UNIQUE(raw_log_id)'
                        connection.execute(text(f'CREATE TABLE "{table_name}" ({cols_str}{unique_constraint})'))
        
        df_to_parse = pd.DataFrame()
        if parse_mode == 'all':
            logger.info("Parse mode 'all' selected. Clearing parsed tables and reprocessing everything.")
            with parsed_engine.connect() as connection:
                with connection.begin():
                    for table_name in config['database_schema'].keys():
                        connection.execute(text(f'DELETE FROM "{table_name}"'))
            df_to_parse = pd.read_sql_table('raw_logs', raw_engine)
        else: # 'new' mode
            logger.info("Parse mode 'new' selected. Processing new and previously unparsed logs.")
            parsed_ids = get_all_ids_from_table(parsed_engine, 'chat') | get_all_ids_from_table(parsed_engine, 'clan_broadcasts')
            last_parsed_id = max(parsed_ids) if parsed_ids else 0
            
            df_new_raw = pd.read_sql(text(f"SELECT * FROM raw_logs WHERE id > {last_parsed_id}"), raw_engine)
            logger.info(f"Found {len(df_new_raw)} new raw messages to parse (ID > {last_parsed_id}).")

            df_unparsed_ids = pd.read_sql_table('unparsed_logs', parsed_engine, columns=['raw_log_id'])
            if not df_unparsed_ids.empty:
                id_list = df_unparsed_ids['raw_log_id'].tolist()
                if id_list:
                    id_tuple = tuple(id_list)
                    sql_in_clause = f"({id_tuple[0]})" if len(id_tuple) == 1 else str(id_tuple)
                    df_retry_raw = pd.read_sql(text(f"SELECT * FROM raw_logs WHERE id IN {sql_in_clause}"), raw_engine)
                    logger.info(f"Found {len(df_retry_raw)} previously unparsed messages to re-process.")
                    df_to_parse = pd.concat([df_new_raw, df_retry_raw]).drop_duplicates(subset=['id']).reset_index(drop=True)
                else:
                    df_to_parse = df_new_raw
            else:
                logger.info("No previously unparsed messages to re-process.")
                df_to_parse = df_new_raw

        # Pass the full config to the parse function
        df_chat, df_broadcasts, df_unparsed = parse_raw_data(df_to_parse, config)

        logger.info("Saving parsed data to the database (duplicates will be ignored)...")
        new_chats_count = save_df_with_ignore(df_chat, 'chat', parsed_engine)
        new_broadcasts_count = save_df_with_ignore(df_broadcasts, 'clan_broadcasts', parsed_engine)
        logger.success(f"--> Added {new_chats_count} new chat messages and {new_broadcasts_count} new broadcasts.")

        # FIX: Check if dataframes are empty before accessing columns
        successfully_reparsed_ids = set()
        if not df_chat.empty:
            successfully_reparsed_ids.update(df_chat['raw_log_id'].tolist())
        if not df_broadcasts.empty:
            successfully_reparsed_ids.update(df_broadcasts['raw_log_id'].tolist())
        
        if successfully_reparsed_ids:
            id_tuple = tuple(successfully_reparsed_ids)
            if id_tuple:
                sql_in_clause = f"({id_tuple[0]})" if len(id_tuple) == 1 else str(id_tuple)
                with parsed_engine.connect() as connection:
                    with connection.begin():
                        connection.execute(text(f"DELETE FROM unparsed_logs WHERE raw_log_id IN {sql_in_clause}"))

        save_df_with_ignore(df_unparsed, 'unparsed_logs', parsed_engine)

        with parsed_engine.connect() as connection:
            total_unparsed = connection.execute(text("SELECT COUNT(*) FROM unparsed_logs")).scalar_one()

        summary = (
            f"**✅ {config.get('general', {}).get('project_name', 'Unnamed Project')}: {SCRIPT_NAME} Complete**\n"
            f"**Mode:** `{parse_mode.capitalize()}`\n\n"
            f"**Parse Results (This Run):**\n"
            f"- Messages Processed: `{len(df_to_parse)}`\n"
            f"- New Chat Messages Added: `{new_chats_count}`\n"
            f"- New Broadcasts Added: `{new_broadcasts_count}`\n\n"
            f"**⚠️ Total Unparsed Messages in DB:** `{total_unparsed}`"
        )
        write_summary_file(SCRIPT_NAME, summary)
        logger.success("Script finished successfully.")

    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)
        summary = (f"**❌ {config.get('general', {}).get('project_name', 'Unnamed Project')}: {SCRIPT_NAME} FAILED**\n**Error:**\n```{e}```")
        write_summary_file(SCRIPT_NAME, summary)
    finally:
        webhook_url = config.get('secrets', {}).get('discord_webhook_url')
        if summary and webhook_url:
            post_to_discord_webhook(webhook_url, summary)
        if raw_engine: raw_engine.dispose()
        if parsed_engine: parsed_engine.dispose()
        logger.info("Database connections closed.")
        logger.info(f"{f' Finished {SCRIPT_NAME} ':=^80}")

if __name__ == "__main__":
    main()