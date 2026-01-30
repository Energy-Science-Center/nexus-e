"""
TYNDP24 Demand Data Cleaning Script

Fixes anomalies in electricity demand data:
- Negative values: replaced with value from 168 hours ago (same hour, previous week)
- Single-hour zeros: linear interpolation
- Multi-hour zero gaps: replaced with value from 168 hours ago

Usage:
    python clean_demand_data.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json

# Configuration
INPUT_FILE = "scenario_data/TYNDP24_datasets/TYNDP24_EU/TYNDP24_demand_org_anomalies.csv"
OUTPUT_FILE = "scenario_data/TYNDP24_datasets/TYNDP24_EU/TYNDP24_demand.csv"
LOG_FILE = "scenario_data/TYNDP24_datasets/TYNDP24_EU/cleaning_log.json"

print("=" * 80)
print("TYNDP24 DEMAND DATA CLEANING")
print("=" * 80)

# Load data
print(f"\nLoading data from: {INPUT_FILE}")
df = pd.read_csv(INPUT_FILE)
print(f"Dataset shape: {df.shape}")

# Get metadata and time columns
metadata_cols = ['Name', 'year', 'type', 'resolution', 'unit', 'Country', 'Policy', 'Climate Year']
time_cols = [col for col in df.columns if col not in metadata_cols]
print(f"Time columns: {len(time_cols)}")

# Convert time columns to numeric
for col in time_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Create a copy for cleaning
df_cleaned = df.copy()

# Initialize logging
cleaning_log = {
    'total_rows': len(df),
    'rows_modified': 0,
    'corrections': {
        'negative_to_t168': 0,
        'zero_interpolation': 0,
        'zero_to_t168': 0,
        'edge_cases': 0
    },
    'details': []
}

# Helper function to find valid replacement value
def find_valid_replacement(values, t, max_iterations=5):
    """
    Find a valid (positive) replacement value for position t.

    Strategy:
    1. Try t-168, t-336, t-504, ... (going back in weekly intervals)
    2. Try t+168, t+336, t+504, ... (going forward in weekly intervals)
    3. Fallback to mean of nearby valid values within 24 hours
    4. If all methods fail, return None

    Returns: (replacement_value, source_description) or (None, error_message)
    """
    attempts = []

    # Try looking back in weekly intervals
    for i in range(1, max_iterations + 1):
        offset = 168 * i
        if t >= offset:
            candidate = values[t - offset]
            attempts.append(f"t-{offset} = {candidate:.2f}")
            if candidate > 0:
                return (candidate, f"t-{offset}")

    # Try looking forward in weekly intervals
    for i in range(1, max_iterations + 1):
        offset = 168 * i
        if t + offset < len(values):
            candidate = values[t + offset]
            attempts.append(f"t+{offset} = {candidate:.2f}")
            if candidate > 0:
                return (candidate, f"t+{offset}")

    # Fallback to nearby valid values
    valid_neighbors = [v for v in values[max(0, t-24):t] if v > 0]
    valid_neighbors += [v for v in values[t+1:min(len(values), t+25)] if v > 0]

    if valid_neighbors:
        return (np.mean(valid_neighbors), "fallback_mean")

    # No valid replacement found - return None with diagnostic info
    error_msg = (
        f"Cannot find valid replacement for position t={t}.\n"
        f"Current value: {values[t]:.2f}\n"
        f"Attempted methods:\n"
        f"  - Weekly lookback/lookahead ({max_iterations} weeks): All values were ≤0\n"
        f"    Checked: {', '.join(attempts[:5])}{'...' if len(attempts) > 5 else ''}\n"
        f"  - Nearby valid values (±24 hours): Found 0 valid values\n"
        f"\nThis indicates the data quality is extremely poor around hour {t}.\n"
        f"Please inspect the raw data and consider:\n"
        f"  1. Extending the time series with valid data\n"
        f"  2. Using a different data source for this period\n"
        f"  3. Manual intervention for this specific anomaly"
    )
    return (None, error_msg)

print("\n" + "=" * 80)
print("CLEANING DATA")
print("=" * 80)

# Process each row
for idx, row in df.iterrows():
    # Skip H2 and heatpump load rows - zeros are valid (seasonal variation)
    if 'load_H' in row['Name'] or 'load_heatpump' in row['Name']:
        continue

    country = row['Country']
    policy = row['Policy']
    climate_year = row['Climate Year']

    # Get time series values
    values = df_cleaned.loc[idx, time_cols].values.astype(float)
    original_values = values.copy()

    corrections_made = []
    row_modified = False

    # Process each time step
    t = 0
    while t < len(values):
        current_val = values[t]

        # Handle negative values
        if current_val < 0:
            replacement, source = find_valid_replacement(values, t)

            if replacement is None:
                # Failed to find valid replacement
                print("\n" + "=" * 80)
                print("ERROR: Unable to clean data")
                print(f"\n{source}")
                print("=" * 80)
                raise ValueError(f"Cannot clean data for {country}, {policy}, Climate Year {climate_year}")

            corrections_made.append({
                'hour': t,
                'type': 'negative',
                'original': float(current_val),
                'replacement': float(replacement),
                'method': source
            })
            values[t] = replacement
            cleaning_log['corrections']['negative_to_t168'] += 1
            row_modified = True
            t += 1

        # Handle zeros
        elif current_val == 0:
            # Find the length of zero sequence
            gap_start = t
            gap_length = 0
            while t < len(values) and values[t] == 0:
                gap_length += 1
                t += 1

            # Apply correction based on gap length
            if gap_length == 1:
                # Single hour: linear interpolation
                hour = gap_start
                if hour > 0 and hour < len(values) - 1 and values[hour-1] > 0 and values[hour+1] > 0:
                    replacement = (values[hour-1] + values[hour+1]) / 2
                    corrections_made.append({
                        'hour': hour,
                        'type': 'single_zero',
                        'original': 0.0,
                        'replacement': float(replacement),
                        'method': 'linear_interpolation'
                    })
                    values[hour] = replacement
                    cleaning_log['corrections']['zero_interpolation'] += 1
                    row_modified = True
                else:
                    # Edge case or surrounded by zeros: find valid replacement
                    replacement, source = find_valid_replacement(values, hour)

                    if replacement is None:
                        # Failed to find valid replacement
                        print("\n" + "=" * 80)
                        print("ERROR: Unable to clean data")
                        print("=" * 80)
                        print(f"\nFailed on row {idx}:")
                        print(f"  Country: {country}")
                        print(f"  Policy: {policy}")
                        print(f"  Climate Year: {climate_year}")
                        print(f"\n{source}")
                        print("=" * 80)
                        raise ValueError(f"Cannot clean data for {country}, {policy}, Climate Year {climate_year}")

                    corrections_made.append({
                        'hour': hour,
                        'type': 'single_zero_edge',
                        'original': 0.0,
                        'replacement': float(replacement),
                        'method': source
                    })
                    values[hour] = replacement
                    cleaning_log['corrections']['edge_cases'] += 1
                    row_modified = True

            else:
                # Multiple consecutive zeros: find valid replacement for each
                for i in range(gap_length):
                    hour = gap_start + i
                    replacement, source = find_valid_replacement(values, hour)

                    if replacement is None:
                        # Failed to find valid replacement
                        print("\n" + "=" * 80)
                        print("ERROR: Unable to clean data")
                        print("=" * 80)
                        print("The row is")
                        print(row)
                        print(f"\nFailed on row {idx}:")
                        print(f"  Country: {country}")
                        print(f"  Policy: {policy}")
                        print(f"  Climate Year: {climate_year}")
                        print(f"\n{source}")
                        print("=" * 80)
                        raise ValueError(f"Cannot clean data for {country}, {policy}, Climate Year {climate_year}")

                    corrections_made.append({
                        'hour': hour,
                        'type': f'multi_zero_gap_{gap_length}',
                        'original': 0.0,
                        'replacement': float(replacement),
                        'method': source
                    })
                    values[hour] = replacement
                    cleaning_log['corrections']['zero_to_t168'] += 1
                    row_modified = True
        else:
            t += 1

    # Update the dataframe if changes were made
    if row_modified:
        df_cleaned.loc[idx, time_cols] = values
        cleaning_log['rows_modified'] += 1

        # Log details for this row
        if corrections_made:
            cleaning_log['details'].append({
                'row_index': int(idx),
                'country': country,
                'policy': policy,
                'climate_year': int(climate_year),
                'num_corrections': len(corrections_made),
                'corrections': corrections_made[:10]  # Store first 10 to avoid huge logs
            })

    # Progress indicator
    if (idx + 1) % 100 == 0:
        print(f"Processed {idx + 1}/{len(df)} rows...")

print(f"\nProcessed all {len(df)} rows")

# Summary statistics
print("\n" + "=" * 80)
print("CLEANING SUMMARY")
print("=" * 80)
print(f"\nTotal rows: {cleaning_log['total_rows']}")
print(f"Rows modified: {cleaning_log['rows_modified']}")
print(f"\nCorrections by type:")
print(f"  - Negative values replaced with t±168: {cleaning_log['corrections']['negative_to_t168']}")
print(f"  - Single zeros interpolated: {cleaning_log['corrections']['zero_interpolation']}")
print(f"  - Multi-hour zeros replaced with t±168: {cleaning_log['corrections']['zero_to_t168']}")
print(f"  - Edge case corrections: {cleaning_log['corrections']['edge_cases']}")
print(f"\nTotal corrections: {sum(cleaning_log['corrections'].values())}")

# Validation: check for remaining anomalies
print("\n" + "=" * 80)
print("VALIDATION")
print("=" * 80)

remaining_negatives = 0
remaining_zeros = 0

for idx, row in df_cleaned.iterrows():
    values = df_cleaned.loc[idx, time_cols].values.astype(float)
    remaining_negatives += (values < 0).sum()
    remaining_zeros += (values == 0).sum()

print(f"\nRemaining anomalies in cleaned data:")
print(f"  - Negative values: {remaining_negatives}")
print(f"  - Zero values: {remaining_zeros}")

# Compare statistics
print("\n" + "=" * 80)
print("BEFORE vs AFTER STATISTICS")
print("=" * 80)

for col in ['Country']:
    print(f"\nStatistics by {col}:")
    print(f"{'Country':<10} {'Original Mean':<15} {'Cleaned Mean':<15} {'Difference':<15}")
    print("-" * 60)

    for country in sorted(df['Country'].unique()):
        country_mask = df['Country'] == country

        original_vals = df.loc[country_mask, time_cols].values.flatten()
        cleaned_vals = df_cleaned.loc[country_mask, time_cols].values.flatten()

        orig_mean = np.nanmean(original_vals)
        clean_mean = np.nanmean(cleaned_vals)
        diff = clean_mean - orig_mean

        print(f"{country:<10} {orig_mean:>14.2f} {clean_mean:>14.2f} {diff:>14.2f}")

# Save cleaned data
print("\n" + "=" * 80)
print("SAVING RESULTS")
print("=" * 80)

output_path = Path(OUTPUT_FILE)
print(f"\nSaving cleaned data to: {output_path}")
df_cleaned.to_csv(output_path, index=False)
print(f"[+] Saved: {output_path}")

# Save cleaning log
log_path = Path(LOG_FILE)
print(f"\nSaving cleaning log to: {log_path}")
with open(log_path, 'w') as f:
    json.dump(cleaning_log, f, indent=2)
print(f"[+] Saved: {log_path}")

print("\n" + "=" * 80)
print("CLEANING COMPLETE!")
print("=" * 80)
print(f"\nCleaned file: {OUTPUT_FILE}")
print(f"Log file: {LOG_FILE}")
print("\nNext steps:")
print("1. Review the cleaning_log.json for detailed corrections")
print("2. Validate the cleaned data with visualizations")
print("3. Use TYNDP24_demand.csv for your analysis")
