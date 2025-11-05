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
    items = load_json_to_dataframe("items_classified.json", parse_dates=["added"])
    users = load_json_to_dataframe("users.json", parse_dates=["created"])
    consumptions = load_json_to_dataframe("consumptions.json", parse_dates=["time"])

    return items, users, consumptions


def get_user_consumption_count(user_id: int, consumptions) -> int:
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    return len(user_consumptions)


def get_user_top_items(user_id: int, consumptions, items, top_n=5):
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    top_items_counts = user_consumptions["item_id"].value_counts().head(top_n)
    top_items = top_items_counts.index.tolist()
    
    top_item_details = items[items["item_id"].isin(top_items)]
    top_item_details = top_item_details.set_index("item_id").loc[top_items].reset_index()
    
    names = top_item_details["name"].tolist()
    counts = top_items_counts.tolist()
    
    return list(zip(names, counts))


def get_user_top_categories(user_id: int, consumptions, items, top_n=5):
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    merged = pd.merge(
        user_consumptions,
        items[["item_id", "category"]],
        on="item_id",
        how="left"
    )
    top_categories = (
        merged["category"]
        .value_counts()
        .head(top_n)
        .index.tolist()
    )
    return top_categories


def get_user_variety(user_id: int, consumptions) -> int:
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    unique_items = user_consumptions["item_id"].nunique()
    return unique_items


def main():
    items, users, consumptions = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        items, users, consumptions = load_data()
    except e:
        print(e)

    for table in [items, users, consumptions]:
        if len(table) == 0:
            print("Loading data failed")
            exit(1)

    user_id = 8

    print("Consumption count:", get_user_consumption_count(user_id, consumptions))
    print("Top items:", get_user_top_items(user_id, consumptions, items, top_n=5))
    print("Top categories:", get_user_top_categories(user_id, consumptions, items, top_n=5))
    print("Variety:", get_user_variety(user_id, consumptions))

if __name__ == "__main__":
    main()

