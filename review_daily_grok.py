import csv
import os
from datetime import datetime, timedelta

def review_daily_grok():
    log_file = "daily_trades.csv"
    if not os.path.exists(log_file):
        print("No daily_trades.csv found. Run one_time.py first.")
        return

    trades = []
    with open(log_file, 'r') as f:
        reader
