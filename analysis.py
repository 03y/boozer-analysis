"""
Boozer data analysis

1. Read in json files which have data for items, users & consumptions
2. Analyse this data and generate interesting stats
3. Export out to json files (one generic stats file and a file for each user)
"""

import pandas as pd
import json
from pathlib import Path

"""
Load a JSON file into a pandas DataFrame.

Args:
    json_file: Path to JSON file
    parse_dates: List of column names to parse as dates
"""


def load_json_to_dataframe(json_file, parse_dates=None):
    df = pd.read_json(json_file)

    if parse_dates:
        for col in parse_dates:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], unit="s")

    return df


def load_data():
    items = load_json_to_dataframe("items.json", parse_dates=["added"])
    users = load_json_to_dataframe("users.json", parse_dates=["created"])
    consumptions = load_json_to_dataframe("consumptions.json", parse_dates=["time"])

    return items, users, consumptions


def main():
    print("Loading data...")

    items, users, consumptions = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        items, users, consumptions = load_data()
    except e:
        print(e)

    for table in [items, users, consumptions]:
        if len(table) == 0:
            print("Loading data failed")
            exit(1)
    print("Loaded data successfully")

    for table in [items, users, consumptions]:
        print(table.head(3))


main()
