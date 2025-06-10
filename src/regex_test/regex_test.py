import toml
import re
from pathlib import Path
import logging
import json

# --- Constants & Paths ---
# The script is in 'src/regex_test', so we navigate up to find the config.
SCRIPT_DIR = Path(__file__).resolve().parent
SRC_ROOT = SCRIPT_DIR.parent
CONFIG_PATH = SRC_ROOT / 'config.toml'
DUMP_FILE_PATH = SCRIPT_DIR / 'raw_message_dump.txt'
OUTPUT_LOG_PATH = SCRIPT_DIR / 'regex_test_failures.log'

# --- Logging Configuration ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)-8s] %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def load_config():
    """Loads configuration from config.toml."""
    logging.info(f"Loading configuration from {CONFIG_PATH}...")
    try:
        return toml.load(CONFIG_PATH)
    except FileNotFoundError:
        logging.error(f"FATAL: Configuration file not found at {CONFIG_PATH}")
        return None
    except Exception as e:
        logging.error(f"Failed to parse config file: {e}")
        return None

def _apply_mappings(definition, groups):
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

def test_regex_patterns():
    """
    Reads a raw message dump and tests each line against the regex patterns
    from the config file, logging any failures with detailed capture group info.
    """
    config = load_config()
    if not config or 'patterns' not in config:
        logging.error("Could not load config or 'patterns' section is missing. Aborting.")
        return

    patterns = config['patterns']

    if not DUMP_FILE_PATH.exists():
        logging.error(f"Dump file not found: {DUMP_FILE_PATH}")
        logging.error("--> Please run the main script (run_sync.bat) with debug_mode=true to generate it.")
        logging.error("--> Then, copy the 'raw_message_dump.txt' from the latest reports folder into this 'src/regex_test' directory and run this script again.")
        return

    with open(DUMP_FILE_PATH, 'r', encoding='utf-8') as f:
        messages_to_test = [line.strip() for line in f if line.strip()]

    logging.info(f"Loaded {len(messages_to_test)} messages from dump file. Starting test...")

    failed_messages = []
    success_count = 0

    for line_num, message in enumerate(messages_to_test, 1):
        is_parsed = False
        failure_reason = "No matching regex pattern found."
        failure_details = {}
        
        chat_def = patterns.get("Chat", {})
        if chat_def and re.search(chat_def.get("regex", "^$"), message):
            is_parsed = True
        else:
            for name, group_def in patterns.items():
                if name.lower() == 'chat': continue
                for variant in group_def.get("variants", []):
                    regex_pattern = variant.get("regex")
                    if not regex_pattern: continue
                    
                    match = re.search(regex_pattern, message)
                    if match:
                        details = _apply_mappings(variant, match.groups())
                        
                        validation_passed = True
                        required_columns = variant.get("required_columns", [])
                        for col in required_columns:
                            val = details.get(col)
                            if val is None or str(val).strip() == '':
                                failure_reason = f"Matched '{group_def.get('broadcast_type', name)}' pattern, but required column '{col}' is blank."
                                failure_details = details
                                validation_passed = False
                                break
                        
                        if validation_passed:
                            is_parsed = True
                            break
                if is_parsed:
                    break
        
        if is_parsed:
            success_count += 1
        else:
            failed_messages.append({
                'line_num': line_num,
                'message': message,
                'reason': failure_reason,
                'captured_details': failure_details
            })

    # --- Reporting Results ---
    logging.info("-" * 40)
    logging.info(f"Test complete. Success: {success_count}, Failures: {len(failed_messages)}")
    if failed_messages:
        logging.warning(f"Found {len(failed_messages)} messages that failed parsing. Writing details to {OUTPUT_LOG_PATH}")
        with open(OUTPUT_LOG_PATH, 'w', encoding='utf-8') as f:
            f.write(f"--- Regex Test Failure Report ---\n")
            f.write(f"Total Messages Tested: {len(messages_to_test)}\n")
            f.write(f"Successful Parses: {success_count}\n")
            f.write(f"Failed Parses: {len(failed_messages)}\n\n")
            f.write("----------------------------------------\n\n")
            for item in failed_messages:
                f.write(f"FAILED MESSAGE (Line {item['line_num']}):\n{item['message']}\n\n")
                f.write(f"REASON: {item['reason']}\n")
                if item['captured_details']:
                    f.write(f"CAPTURED DETAILS: {json.dumps(item['captured_details'], indent=2)}\n")
                f.write("----------------------------------------\n\n")
    else:
        logging.info("🎉 All messages were parsed successfully!")

if __name__ == "__main__":
    test_regex_patterns()
