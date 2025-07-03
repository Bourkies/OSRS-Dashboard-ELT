import pandas as pd
import logging
try:
    import tomllib
except ImportError:
    import tomli as tomllib
import os
import json
from sqlalchemy import inspect
from datetime import datetime, timedelta, timezone
from pathlib import Path
import itertools

from shared_utils import (
    setup_logging, load_config, get_db_engine, write_summary_file,
    SRC_ROOT, SUMMARIES_DIR, PROJECT_ROOT, post_to_discord_webhook, get_time_periods
)

SCRIPT_NAME = "3_transform_data"

# --- Helper Functions ---

def time_str_to_seconds(time_str):
    """Converts a time string (e.g., '1:23.4' or '1:15:45') to seconds."""
    if not isinstance(time_str, str):
        return float('inf')
    parts = time_str.split(':')
    seconds = 0
    try:
        if len(parts) == 3:  # H:M:S
            seconds += int(parts[0]) * 3600
            seconds += int(parts[1]) * 60
            seconds += float(parts[2])
        elif len(parts) == 2:  # M:S
            seconds += int(parts[0]) * 60
            seconds += float(parts[1])
        elif len(parts) == 1:  # S
            seconds += float(parts[0])
    except (ValueError, IndexError):
        return float('inf')
    return seconds

def get_records_for_period(df, start_date=None, end_date=None):
    if df.empty: return pd.DataFrame()
    df_copy = df.copy()
    if 'Timestamp' not in df_copy.columns: return pd.DataFrame()
    df_copy['Timestamp'] = pd.to_datetime(df_copy['Timestamp'], errors='coerce', utc=True)
    df_copy.dropna(subset=['Timestamp'], inplace=True)
    
    if start_date: df_copy = df_copy[df_copy['Timestamp'] >= start_date]
    if end_date: df_copy = df_copy[df_copy['Timestamp'] < end_date]
    return df_copy

def create_metadata_tables(engine, config, periods):
    """Creates tables for run metadata and dashboard config, including dynamic labels."""
    logging.info("Creating/updating metadata tables...")
    run_time_iso = periods['All_Time']['end'].isoformat()
    
    df_meta = pd.DataFrame([{'last_updated_utc': run_time_iso}])
    df_meta.to_sql('run_metadata', engine, if_exists='replace', index=False)
    
    # Load settings and group/item orders from historical files
    pb_historical_file = SRC_ROOT / config.get('historical_data', {}).get('personal_bests_file')
    with open(pb_historical_file, "rb") as f:
        pb_hist_data = tomllib.load(f)
    pb_settings = pb_hist_data.get('settings', {})
    pb_item_orders = {g.get('title'): [r.get('name') for r in g.get('records', [])] for g in pb_hist_data.get('groups', [])}

    clog_historical_file = SRC_ROOT / config.get('historical_data', {}).get('collection_log_file')
    with open(clog_historical_file, "rb") as f:
        clog_hist_data = tomllib.load(f)
    clog_settings = clog_hist_data.get('settings', {})
    clog_item_orders = {g.get('title'): [i.get('name') for i in g.get('items', [])] for g in clog_hist_data.get('groups', [])}

    df_config = pd.DataFrame([
        {'key': 'custom_lookback_days', 'value': str(config['dashboard_settings'].get('custom_lookback_days', 14))},
        {'key': 'top_drops_limit', 'value': str(config['dashboard_settings'].get('top_drops_limit', 50))},
        {'key': 'label_prev_week', 'value': periods['Prev_Week']['label']},
        {'key': 'label_prev_month', 'value': periods['Prev_Month']['label']},
        {'key': 'label_ytd', 'value': periods['YTD']['label']},
        {'key': 'label_custom_days', 'value': periods['Custom_Days']['label']},
        
        {'key': 'pb_other_group_name', 'value': pb_settings.get('other_group_name', 'Miscellaneous PBs')},
        {'key': 'pb_default_group_sort', 'value': pb_settings.get('default_group_sort', 'config')},
        {'key': 'pb_default_item_sort', 'value': pb_settings.get('default_item_sort', 'alphabetical')},
        {'key': 'pb_group_order', 'value': json.dumps(list(pb_item_orders.keys()))},
        {'key': 'pb_item_orders', 'value': json.dumps(pb_item_orders)},

        {'key': 'clog_other_group_name', 'value': clog_settings.get('other_group_name', 'Miscellaneous Drops')},
        {'key': 'clog_default_group_sort', 'value': clog_settings.get('default_group_sort', 'config')},
        {'key': 'clog_default_item_sort', 'value': clog_settings.get('default_item_sort', 'alphabetical')},
        {'key': 'clog_group_order', 'value': json.dumps(list(clog_item_orders.keys()))},
        {'key': 'clog_item_orders', 'value': json.dumps(clog_item_orders)},
    ])
    df_config.to_sql('dashboard_config', engine, if_exists='replace', index=False)
    logging.info("--> Metadata tables updated successfully.")

# --- Username Mapping Functions ---

def validate_mapping_rules(rules):
    """Checks for overlapping time ranges for the same source username and logs a warning."""
    logging.info("Validating username mapping rules for conflicts...")
    # Helper to parse dates with defaults for infinity
    def parse_date(date_str, default):
        if not date_str: return default
        return pd.to_datetime(date_str, errors='coerce', utc=True)

    # Pre-process rules to have consistent datetime objects
    processed_rules = []
    for i, rule in enumerate(rules):
        start = parse_date(rule.get('start_date'), pd.Timestamp.min.replace(tzinfo=timezone.utc))
        end = parse_date(rule.get('end_date'), pd.Timestamp.max.replace(tzinfo=timezone.utc))
        if pd.isna(start) or pd.isna(end):
            logging.warning(f"Skipping rule {i+1} due to invalid date format: {rule}")
            continue
        processed_rules.append({
            'sources': set(rule.get('source_usernames', [])),
            'start': start,
            'end': end,
            'rule_index': i + 1
        })

    # Check for conflicts
    for (r1, r2) in itertools.combinations(processed_rules, 2):
        # Check if any source usernames are shared between the two rules
        common_sources = r1['sources'].intersection(r2['sources'])
        if not common_sources:
            continue

        # Check for time overlap: (StartA < EndB) and (StartB < EndA)
        if r1['start'] < r2['end'] and r2['start'] < r1['end']:
            logging.warning(
                f"Conflict detected in username mapping! "
                f"Rule #{r1['rule_index']} and Rule #{r2['rule_index']} both apply to "
                f"'{', '.join(common_sources)}' during an overlapping time period. "
                f"The rule that appears later in the config will take precedence."
            )
    logging.info("--> Validation of mapping rules complete.")

def apply_username_mapping(df, rules, username_columns):
    """Applies username mapping rules to the specified columns in a DataFrame."""
    if not rules or df.empty:
        return df

    df_copy = df.copy()
    
    # Iterate in reverse to ensure later rules in the config file take precedence
    for i, rule in reversed(list(enumerate(rules))):
        target_name = rule.get('target_username')
        source_names = rule.get('source_usernames', [])
        if not target_name or not source_names:
            continue

        # Parse dates, defaulting to min/max if not provided
        start_date = pd.to_datetime(rule.get('start_date'), errors='coerce', utc=True)
        end_date = pd.to_datetime(rule.get('end_date'), errors='coerce', utc=True)

        # Create a boolean mask for the time period
        time_mask = pd.Series(True, index=df_copy.index)
        if pd.notna(start_date):
            time_mask &= (df_copy['Timestamp'] >= start_date)
        if pd.notna(end_date):
            time_mask &= (df_copy['Timestamp'] < end_date)

        # Apply the mapping to all relevant username columns
        for col in username_columns:
            if col in df_copy.columns:
                # Create a boolean mask for the source usernames in the current column
                name_mask = df_copy[col].isin(source_names)
                # Combine masks and apply the change
                combined_mask = time_mask & name_mask
                if combined_mask.any():
                    df_copy.loc[combined_mask, col] = target_name
    
    return df_copy

# --- Report Generators ---

def generate_leaderboard_reports(df_chat, df_broadcasts, config, periods):
    logging.info("Generating all configured leaderboard reports...")
    reports = {}
    report_configs = config['dashboard_settings'].get('leaderboard_reports', [])
    
    for rc in report_configs:
        try:
            name = rc['report_name']
            source_df_name = rc.get('source_table', 'clan_broadcasts')
            df_source = df_chat if source_df_name == 'chat' else df_broadcasts
            
            df_filtered = df_source.copy()

            if 'broadcast_type' in rc:
                df_filtered = df_filtered[df_filtered['Broadcast_Type'] == rc['broadcast_type']]
            
            if 'item_name_filter' in rc:
                 df_filtered = df_filtered[df_filtered['Item_Name'] == rc['item_name_filter']]

            if 'search_phrases' in rc:
                search_regex = '|'.join(rc['search_phrases'])
                df_filtered = df_filtered[df_filtered['Content'].str.contains(search_regex, case=False, na=False)]
            
            if df_filtered.empty:
                logging.warning(f"Skipping leaderboard report '{name}': No source data after filtering.")
                reports[name] = pd.DataFrame()
                continue
            
            group_by_col = rc['group_by_column']
            aggregations = rc.get('aggregations', {})
            
            agg_spec = {}
            if 'Count' in aggregations:
                agg_spec['Count_All_Time'] = pd.NamedAgg(column=aggregations['Count'], aggfunc='count')
            if 'Value' in aggregations:
                df_filtered[aggregations['Value']] = pd.to_numeric(df_filtered[aggregations['Value']], errors='coerce').fillna(0)
                agg_spec['Value_All_Time'] = pd.NamedAgg(column=aggregations['Value'], aggfunc='sum')

            if not agg_spec:
                logging.warning(f"No aggregations defined for report '{name}'. Skipping.")
                continue

            df_summary = df_filtered.groupby(group_by_col).agg(**agg_spec).reset_index()

            for period_key, dates in periods.items():
                if period_key == "All_Time": continue
                df_period = get_records_for_period(df_filtered, start_date=dates['start'], end_date=dates['end'])
                
                period_agg_spec = {}
                if 'Count' in aggregations: period_agg_spec[f'Count_{period_key}'] = pd.NamedAgg(column=aggregations['Count'], aggfunc='count')
                if 'Value' in aggregations: period_agg_spec[f'Value_{period_key}'] = pd.NamedAgg(column=aggregations['Value'], aggfunc='sum')
                
                if not df_period.empty and period_agg_spec:
                    period_agg = df_period.groupby(group_by_col).agg(**period_agg_spec).reset_index()
                    df_summary = pd.merge(df_summary, period_agg, on=group_by_col, how='left')
                else:
                    if 'Count' in aggregations: df_summary[f'Count_{period_key}'] = 0
                    if 'Value' in aggregations: df_summary[f'Value_{period_key}'] = 0

            for col in df_summary.columns:
                if 'Count_' in col or 'Value_' in col:
                    df_summary[col] = df_summary[col].fillna(0).astype(int)
            
            reports[name] = df_summary
            logging.info(f"--> Generated leaderboard report '{name}' with {len(df_summary)} entries.")
        except Exception as e:
            logging.error(f"Failed to generate leaderboard report '{rc.get('report_name', 'Unknown')}': {e}", exc_info=True)
            
    return reports

def generate_detailed_reports(df_broadcasts, config, periods):
    logging.info("Generating all configured detailed reports...")
    reports = {}
    report_configs = config['dashboard_settings'].get('detailed_reports', [])

    for rc in report_configs:
        try:
            name_prefix = rc['report_name_prefix']
            broadcast_types = rc['broadcast_types']
            
            df_filtered = df_broadcasts[df_broadcasts['Broadcast_Type'].isin(broadcast_types)].copy()
            
            if not df_filtered.empty:
                if 'Item_Value' in df_filtered.columns:
                     df_filtered['Item_Value'] = pd.to_numeric(df_filtered['Item_Value'], errors='coerce').fillna(0)
                df_filtered['Timestamp'] = pd.to_datetime(df_filtered['Timestamp'], utc=True)
                df_filtered = df_filtered.sort_values(by='Timestamp', ascending=False)

            for period_key, dates in periods.items():
                table_name = f"{name_prefix}_{period_key.lower()}"
                if df_filtered.empty:
                    reports[table_name] = pd.DataFrame()
                    logging.info(f"--> Generated empty detailed report '{table_name}' due to no source data.")
                else:
                    df_period = get_records_for_period(df_filtered, start_date=dates['start'], end_date=dates['end'])
                    reports[table_name] = df_period
                    logging.info(f"--> Generated detailed report '{table_name}' with {len(df_period)} rows.")

        except Exception as e:
            logging.error(f"Failed to generate detailed report for '{rc.get('report_name_prefix', 'Unknown')}': {e}", exc_info=True)
            
    return reports

def generate_timeseries_reports(df_source, config):
    logging.info("Generating all configured timeseries reports...")
    reports = {}
    report_configs = config['dashboard_settings'].get('timeseries_reports', [])
    if not report_configs: 
        logging.warning("No timeseries reports configured.")
        return reports

    df_source['Timestamp'] = pd.to_datetime(df_source['Timestamp'], errors='coerce', utc=True)
    df_source.dropna(subset=['Timestamp'], inplace=True)

    for rc in report_configs:
        try:
            name = rc['report_name']
            df_filtered = df_source[df_source['Broadcast_Type'] == rc['broadcast_type']].copy()
            
            if df_filtered.empty: 
                logging.info(f"--> No data for timeseries report '{name}', creating empty table.")
                reports[name] = pd.DataFrame()
                continue
            
            if 'Item_Value' in df_filtered.columns:
                df_filtered['Item_Value'] = pd.to_numeric(df_filtered['Item_Value'], errors='coerce').fillna(0)
            else: 
                df_filtered['Item_Value'] = 0

            all_resampled = []
            for freq in rc.get('frequencies', ['D']):
                df_resampled = df_filtered.set_index('Timestamp').resample(freq).agg(
                    Count=('Username', 'count'), 
                    Total_Value=('Item_Value', 'sum')
                ).sort_index()
                df_resampled['Cumulative_Count'] = df_resampled['Count'].cumsum()
                df_resampled['Cumulative_Value'] = df_resampled['Total_Value'].cumsum()
                
                df_resampled = df_resampled.reset_index()
                df_resampled['Frequency'] = freq
                all_resampled.append(df_resampled)
            
            if not all_resampled: 
                reports[name] = pd.DataFrame()
                continue

            df_final = pd.concat(all_resampled).rename(columns={'Timestamp': 'Date'})
            reports[name] = df_final
            logging.info(f"--> Generated timeseries report '{name}' for freqs {rc['frequencies']} with {len(df_final)} entries.")
        except Exception as e:
            logging.error(f"Failed to generate timeseries report '{rc.get('report_name', 'Unknown')}': {e}", exc_info=True)
    
    return reports

def generate_collection_log_report(df_broadcasts, config, periods):
    """Generates the collection log summary by combining historical and new data."""
    logging.info("Generating collection log report...")
    clog_config = config.get('dashboard_settings', {}).get('collection_log', {})
    
    historical_file = SRC_ROOT / config.get('historical_data', {}).get('collection_log_file')
    if not historical_file.exists():
        logging.error(f"Historical collection log file not found at {historical_file}")
        return pd.DataFrame()
        
    with open(historical_file, "rb") as f:
        hist_data = tomllib.load(f)
        
    exclude_rules = hist_data.get('exclude_rules', [])
    other_group_name = hist_data.get('settings', {}).get('other_group_name', 'Miscellaneous Drops')
    
    item_to_group_map = {}
    historical_counts = {}
    for group in hist_data.get('groups', []):
        group_title = group.get('title')
        for item in group.get('items', []):
            item_name = item.get('name')
            if item_name:
                item_to_group_map[item_name] = group_title
                historical_counts[item_name] = item.get('count', 0)

    source_types = clog_config.get('source_types', [])
    df_clog_source = df_broadcasts[df_broadcasts['Broadcast_Type'].isin(source_types)].copy()
    
    if exclude_rules:
        flat_exclude_list = [item for sublist in exclude_rules for item in sublist]
        logging.info(f"Applying {len(flat_exclude_list)} exclusion rules to collection log items...")
        initial_rows = len(df_clog_source)
        df_clog_source = df_clog_source[~df_clog_source['Item_Name'].isin(flat_exclude_list)]
        logging.info(f"--> Excluded {initial_rows - len(df_clog_source)} CLog items.")

    dedup_type = clog_config.get('deduplication_type')
    if dedup_type:
        df_to_dedup = df_clog_source[df_clog_source['Broadcast_Type'] == dedup_type]
        df_others = df_clog_source[df_clog_source['Broadcast_Type'] != dedup_type]
        
        df_deduped = df_to_dedup.drop_duplicates(subset=['Username', 'Item_Name'])
        df_clog_source = pd.concat([df_deduped, df_others])
        logging.info(f"Deduplicated {len(df_to_dedup) - len(df_deduped)} rows for broadcast type '{dedup_type}'.")

    all_db_items = df_clog_source['Item_Name'].dropna().unique()
    all_known_items = sorted(list(set(all_db_items) | set(historical_counts.keys())))
    
    df_summary = pd.DataFrame({'Item_Name': all_known_items})
    
    for period_key, dates in periods.items():
        df_period = get_records_for_period(df_clog_source, start_date=dates['start'], end_date=dates['end'])
        
        if df_period.empty:
            df_summary[f'{period_key}_Count'] = 0
        else:
            period_counts = df_period.groupby('Item_Name').size().reset_index(name=f'{period_key}_Count')
            df_summary = pd.merge(df_summary, period_counts, on='Item_Name', how='left')

    df_summary['Historical_Count'] = df_summary['Item_Name'].map(historical_counts).fillna(0)
    df_summary['All_Time_Count'] = df_summary.get('All_Time_Count', 0).fillna(0) + df_summary['Historical_Count']
    
    df_summary.drop(columns=['Historical_Count'], inplace=True)
    df_summary = df_summary.fillna(0).astype({col: int for col in df_summary.columns if 'Count' in col})

    df_summary['Group'] = df_summary['Item_Name'].map(item_to_group_map).fillna(other_group_name)
    
    logging.info(f"--> Generated collection log report with {len(df_summary)} unique items.")
    return df_summary

def generate_personal_bests_report(df_broadcasts, config):
    """
    Generates the personal bests summary table with logic for grouping team records.
    """
    logging.info("Generating personal bests report...")
    pb_config = config.get('dashboard_settings', {}).get('personal_bests', {})
    
    time_similarity_threshold = pb_config.get('pb_time_similarity_threshold_seconds', 0.6)
    grouping_window = timedelta(seconds=pb_config.get('pb_grouping_window_seconds', 5))
    allow_multiple_holders = pb_config.get('allow_multiple_holders_on_match', True)

    historical_file = SRC_ROOT / config.get('historical_data', {}).get('personal_bests_file')
    if not historical_file.exists():
        logging.error(f"Historical personal bests file not found at {historical_file}")
        return pd.DataFrame()
    
    with open(historical_file, "rb") as f:
        hist_data = tomllib.load(f)
        
    exclude_rules = hist_data.get('exclude_rules', [])
    blacklist_rules = hist_data.get('blacklist', [])
    other_group_name = hist_data.get('settings', {}).get('other_group_name', 'Miscellaneous PBs')
    
    all_pbs = []
    task_to_group_map = {}
    all_historical_tasks = set()
    
    for group in hist_data.get('groups', []):
        group_title = group.get('title')
        for record in group.get('records', []):
            task_name = record.get('name')
            if task_name:
                all_historical_tasks.add(task_name)
                task_to_group_map[task_name] = group_title
                holders = record.get('holder', [])
                # Ensure holder is a list and handle empty string case
                if isinstance(holders, str):
                    holders = [holders] if holders else []
                
                all_pbs.append({
                    'Task_Name': task_name,
                    'PB_Time': record.get('time'),
                    'Username': holders[0] if holders else "", # Use empty string for no holder
                    'All_Holders': holders,
                    'Timestamp': pd.Timestamp.min.replace(tzinfo=timezone.utc),
                    'is_historical': True
                })

    source_type = pb_config.get('broadcast_type')
    df_pbs_source = df_broadcasts[df_broadcasts['Broadcast_Type'] == source_type].copy()
    df_pbs_source['is_historical'] = False
    
    if not df_pbs_source.empty:
        all_pbs.extend(df_pbs_source.to_dict('records'))

    if not all_pbs:
        logging.warning("No historical or new personal bests found.")
        return pd.DataFrame()

    df_all_pbs = pd.DataFrame(all_pbs)
    
    # Apply blacklist rules before any other processing
    if blacklist_rules:
        logging.info(f"Applying {len(blacklist_rules)} PB blacklist rules...")
        
        globally_blacklisted_users = {
            rule['username'] for rule in blacklist_rules if 'task_name' not in rule and 'username' in rule
        }
        if globally_blacklisted_users:
            logging.info(f"  - Global blacklist for users: {', '.join(globally_blacklisted_users)}")
            # First, remove them from any group records
            df_all_pbs['All_Holders'] = df_all_pbs['All_Holders'].apply(
                lambda holders: [h for h in holders if h not in globally_blacklisted_users] if isinstance(holders, list) else holders
            )

        keep_mask = pd.Series(True, index=df_all_pbs.index)
        for rule in blacklist_rules:
            user = rule.get('username')
            if not user:
                logging.warning(f"Skipping invalid blacklist rule (missing username): {rule}")
                continue

            task = rule.get('task_name')
            max_time_str = rule.get('max_time')

            if not task and not max_time_str: # Global user blacklist
                user_mask = (df_all_pbs['Username'] == user)
                keep_mask &= ~user_mask
            elif task and max_time_str: # Specific task/time blacklist
                max_time_seconds = time_str_to_seconds(max_time_str)
                rule_mask = (df_all_pbs['Username'] == user) & (df_all_pbs['Task_Name'] == task)
                if rule_mask.any():
                    pb_times_seconds = df_all_pbs.loc[rule_mask, 'PB_Time'].apply(time_str_to_seconds)
                    blacklisted_times_mask = pb_times_seconds < max_time_seconds
                    indices_to_blacklist = df_all_pbs.loc[rule_mask][blacklisted_times_mask].index
                    keep_mask.loc[indices_to_blacklist] = False
            else:
                 logging.warning(f"Skipping invalid blacklist rule (must be global or have both task_name and max_time): {rule}")

        initial_rows = len(df_all_pbs)
        df_all_pbs = df_all_pbs[keep_mask].reset_index(drop=True)
        logging.info(f"--> Removed a total of {initial_rows - len(df_all_pbs)} blacklisted PB records.")

    if exclude_rules:
        logging.info(f"Applying {len(exclude_rules)} exclusion rules to personal bests...")
        initial_rows = len(df_all_pbs)
        exclude_mask = pd.Series(False, index=df_all_pbs.index)
        for rule_set in exclude_rules:
            current_rule_mask = pd.Series(True, index=df_all_pbs.index)
            for required_string in rule_set:
                current_rule_mask &= df_all_pbs['Task_Name'].str.contains(required_string, na=False, regex=False)
            exclude_mask |= current_rule_mask
        df_all_pbs = df_all_pbs[~exclude_mask]
        logging.info(f"--> Excluded {initial_rows - len(df_all_pbs)} PB records.")

    df_all_pbs['Timestamp'] = pd.to_datetime(df_all_pbs['Timestamp'], errors='coerce', utc=True)
    df_all_pbs['seconds'] = df_all_pbs.apply(
        lambda row: float('inf') if row['is_historical'] and row['PB_Time'] == "0:00" else time_str_to_seconds(row['PB_Time']),
        axis=1
    )
    df_all_pbs.dropna(subset=['Task_Name', 'seconds'], inplace=True)

    final_records = {}
    for task_name, task_group_df in df_all_pbs.groupby('Task_Name'):
        best_time_seconds = task_group_df['seconds'].min()
        
        if best_time_seconds == float('inf'):
            best_time_df = task_group_df.copy()
        else:
            best_time_df = task_group_df[
                abs(task_group_df['seconds'] - best_time_seconds) <= time_similarity_threshold
            ].copy()
        
        best_time_df = best_time_df.sort_values(by='Timestamp', ascending=True)
        if best_time_df.empty: continue

        first_record_timestamp = best_time_df.iloc[0]['Timestamp']
        group_cutoff_time = first_record_timestamp + grouping_window
        first_achievers_df = best_time_df[best_time_df['Timestamp'] <= group_cutoff_time]

        all_holders = []
        historical_record = first_achievers_df[first_achievers_df['is_historical']]
        if not historical_record.empty:
             all_holders.extend(historical_record.iloc[0].get('All_Holders') or [])

        db_record_holders = first_achievers_df[~first_achievers_df['is_historical']]['Username'].tolist()
        all_holders.extend(db_record_holders)

        if allow_multiple_holders:
            later_achievers_df = best_time_df[best_time_df['Timestamp'] > group_cutoff_time]
            all_holders.extend(later_achievers_df['Username'].tolist())

        unique_holders = sorted(list(set(filter(None, all_holders))))
        final_record_details = best_time_df.iloc[0]
        
        first_db_record = best_time_df[~best_time_df['is_historical']]
        record_date = first_db_record.iloc[0]['Timestamp'].strftime('%Y-%m-%d') if not first_db_record.empty else None

        final_records[task_name] = {
            'Task': task_name,
            'Holder': ', '.join(unique_holders),
            'Time': final_record_details['PB_Time'],
            'Date': record_date,
            'Group': task_to_group_map.get(task_name, other_group_name)
        }

    df_summary = pd.DataFrame.from_dict(final_records, orient='index')
    
    # Ensure all historical tasks are present in the final report
    processed_tasks = set(df_summary['Task']) if not df_summary.empty else set()
    missing_tasks = all_historical_tasks - processed_tasks
    if missing_tasks:
        missing_records = []
        for task in missing_tasks:
            missing_records.append({
                'Task': task,
                'Holder': '',
                'Time': '0:00',
                'Date': None,
                'Group': task_to_group_map.get(task, other_group_name)
            })
        df_missing = pd.DataFrame(missing_records)
        df_summary = pd.concat([df_summary, df_missing], ignore_index=True)
        logging.info(f"--> Added back {len(missing_tasks)} tasks that had no valid record holders after blacklisting.")

    logging.info(f"--> Generated personal bests report with {len(df_summary)} unique items.")
    return df_summary


def generate_recent_achievements_report(df_broadcasts, config):
    """Generates a table of recent achievements, creating special categories for maxed skills."""
    logging.info("Generating recent achievements report...")
    ra_config = config.get('dashboard_settings', {}).get('recent_achievements', {})
    
    source_types = ra_config.get('source_types', [])
    limit_per_type = ra_config.get('limit_per_type', 15)
    
    df_source = df_broadcasts[df_broadcasts['Broadcast_Type'].isin(source_types)].copy()
    
    if df_source.empty:
        logging.info("No broadcasts found for recent achievements report.")
        return pd.DataFrame()

    df_levelups = df_source[df_source['Broadcast_Type'] == 'Level Up'].copy()
    df_levelups['New_Level'] = pd.to_numeric(df_levelups['New_Level'], errors='coerce').fillna(0).astype(int)

    df_maxed_99 = df_levelups[(df_levelups['New_Level'] == 99) & (df_levelups['Skill'] != 'Combat')].copy()
    df_maxed_99['Broadcast_Type'] = 'Maxed Skill (99)'
    
    df_maxed_combat = df_levelups[(df_levelups['New_Level'] == 126) & (df_levelups['Skill'] == 'Combat')].copy()
    df_maxed_combat['Broadcast_Type'] = 'Maxed Combat'

    df_combined = pd.concat([df_source, df_maxed_99, df_maxed_combat])
    df_combined.sort_values(by='Timestamp', ascending=False, inplace=True)
    df_recent = df_combined.groupby('Broadcast_Type').head(limit_per_type)
    
    logging.info(f"--> Generated recent achievements report with {len(df_recent)} entries.")
    return df_recent


def main():
    setup_logging(SCRIPT_NAME)
    config = load_config()
    
    parsed_engine = get_db_engine(config['databases']['parsed_db_uri'])
    optimised_db_uri = config['databases']['optimised_db_uri']

    if optimised_db_uri.startswith('sqlite'):
        db_file_path = (PROJECT_ROOT / optimised_db_uri.split('///')[1]).resolve()
        if os.path.exists(db_file_path):
            logging.info(f"Existing optimised database found at '{db_file_path}'. Deleting it.")
            os.remove(db_file_path)

    optimised_engine = get_db_engine(optimised_db_uri)
    if not parsed_engine or not optimised_engine: return

    summary_stats = {}
    try:
        logging.info("Reading data from parsed database...")
        df_broadcasts = pd.read_sql_table('clan_broadcasts', parsed_engine, coerce_float=False)
        if 'New_Level' in df_broadcasts.columns:
            df_broadcasts['New_Level'] = pd.to_numeric(df_broadcasts['New_Level'], errors='coerce').astype('Int64')

        df_chat = pd.read_sql_table('chat', parsed_engine, coerce_float=False)
        
        run_time = datetime.now(timezone.utc)
        periods = get_time_periods(config, run_time=run_time)

        df_broadcasts['Timestamp'] = pd.to_datetime(df_broadcasts['Timestamp'], errors='coerce', utc=True)
        df_chat['Timestamp'] = pd.to_datetime(df_chat['Timestamp'], errors='coerce', utc=True)
        
        # --- Apply Username Mapping ---
        mapping_rules = config.get('username_mapping', {}).get('rules', [])
        if mapping_rules:
            logging.info("Username mapping rules found. Applying them now...")
            validate_mapping_rules(mapping_rules)
            
            broadcast_user_cols = ['Username', 'Action_By', 'Opponent']
            df_broadcasts = apply_username_mapping(df_broadcasts, mapping_rules, broadcast_user_cols)
            
            chat_user_cols = ['Username']
            df_chat = apply_username_mapping(df_chat, mapping_rules, chat_user_cols)
            
            logging.info("--> Username mapping applied successfully.")
        else:
            logging.info("No username mapping rules found in config. Skipping.")

        all_reports = {}
        create_metadata_tables(optimised_engine, config, periods)
        
        leaderboard_reports = generate_leaderboard_reports(df_chat, df_broadcasts, config, periods)
        all_reports.update(leaderboard_reports)
        
        detailed_reports = generate_detailed_reports(df_broadcasts, config, periods)
        all_reports.update(detailed_reports)

        timeseries_reports = generate_timeseries_reports(df_broadcasts, config)
        all_reports.update(timeseries_reports)

        clog_report = generate_collection_log_report(df_broadcasts, config, periods)
        all_reports['collection_log_summary'] = clog_report

        pb_report = generate_personal_bests_report(df_broadcasts, config)
        all_reports['personal_bests_summary'] = pb_report

        recent_achievements_report = generate_recent_achievements_report(df_broadcasts, config)
        all_reports['recent_achievements'] = recent_achievements_report


        logging.info("Saving all transformed tables to the optimised database...")
        for name, df_report in all_reports.items():
            if df_report is not None:
                if isinstance(df_report.index, pd.CategoricalIndex):
                    df_report = df_report.reset_index()
                if 'Group' in df_report.columns and isinstance(df_report['Group'].dtype, pd.CategoricalDtype):
                    df_report['Group'] = df_report['Group'].astype(str)

                df_report.to_sql(name, optimised_engine, if_exists='replace', index=False)
                summary_stats[name] = len(df_report)
        
        table_counts_str = "\n".join([f"- `{name}`: `{count}` rows" for name, count in sorted(summary_stats.items())])
        summary = (
            f"**✅ {config.get('general', {}).get('project_name', 'Unnamed Project')}: {SCRIPT_NAME} Complete**\n\n"
            f"**Run Time:** `{run_time.strftime('%Y-%m-%d %H:%M:%S UTC')}`\n"
            f"**Transformation Results:**\n"
            f"- Broadcasts Processed: `{len(df_broadcasts)}`\n"
            f"- Chat Messages Processed: `{len(df_chat)}`\n"
            f"- Optimised Tables Created: `{len(summary_stats)}`\n\n"
            f"**Created Table Row Counts:**\n{table_counts_str}"
        )
        write_summary_file(SCRIPT_NAME, summary)
        
    except Exception as e:
        summary = f"**❌ {config.get('general', {}).get('project_name', 'Unnamed Project')}: {SCRIPT_NAME} FAILED**\n**Error:**\n```{e}```"
        logging.critical(f"An unexpected error occurred in main: {e}", exc_info=True)
    finally:
        webhook_url = config.get('secrets', {}).get('discord_webhook_url')
        if 'summary' in locals() and summary and webhook_url:
            post_to_discord_webhook(webhook_url, summary)
        if parsed_engine: parsed_engine.dispose()
        if optimised_engine: optimised_engine.dispose()
        logging.info("Database connections closed.")

if __name__ == "__main__":
    main()