import schedule
import time
import pandas as pd
from datetime import datetime
import os

def generate_summary():
    print("Generating summary...")

    # Load the data
    try:
        df = pd.read_csv('daily_raw.csv', names=[
            'date', 'train_no', 'train_name', 'station', 'delay_min', 'status'
        ])
    except Exception as e:
        print(f"Error loading daily_raw.csv: {e}")
        return

    # Filter out rows without valid delay
    df = df[df['delay_min'].notna()]

    # Ensure delay_min is numeric
    df['delay_min'] = pd.to_numeric(df['delay_min'], errors='coerce')
    df = df[df['delay_min'].notna()]

    # Classify delays
    def classify_delay(mins):
        if mins <= 15:
            return 'right_time_0_15'
        elif mins <= 60:
            return 'slight_delay_15_60'
        else:
            return 'significant_delay_60+'

    df['delay_category'] = df['delay_min'].apply(classify_delay)

    # Group and aggregate
    summary = df.groupby(['train_no', 'train_name', 'station']).agg(
        average_delay_min=('delay_min', 'mean'),
        count_total=('delay_min', 'count'),
        right_time_0_15=('delay_category', lambda x: (x == 'right_time_0_15').sum()),
        slight_delay_15_60=('delay_category', lambda x: (x == 'slight_delay_15_60').sum()),
        significant_delay_60_plus=('delay_category', lambda x: (x == 'significant_delay_60+').sum())
    ).reset_index()

    # Convert counts to probabilities
    summary['right_time_0_15'] = (summary['right_time_0_15'] / summary['count_total']).round(2)
    summary['slight_delay_15_60'] = (summary['slight_delay_15_60'] / summary['count_total']).round(2)
    summary['significant_delay_60+'] = (summary['significant_delay_60_plus'] / summary['count_total']).round(2)

    # Drop unused count column
    summary = summary.drop(columns=['count_total', 'significant_delay_60_plus'])

    # Save summary to CSV
    summary.to_csv('summary.csv', index=False)
    print("✅ summary.csv updated successfully.")

# Manual execution
generate_summary()

# Schedule to run at 09:15 AM daily
schedule.every().day.at("09:15").do(generate_summary)

print("Scheduler started. Waiting for 09:15 AM daily summary update...")

# Keep running to allow scheduled task
while True:
    schedule.run_pending()
    time.sleep(1)