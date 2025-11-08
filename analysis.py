"""
Boozer data analysis

1. Read in json files which have data for items, users & consumptions
2. Analyse this data and generate interesting stats
3. Export out to json files (one generic stats file and a file for each user)
"""

import pandas as pd
import requests
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

"""
Load all data files into pandas DataFrames.
"""
def load_data():
    items = load_json_to_dataframe("items_classified.json", parse_dates=["added"])
    users = load_json_to_dataframe("users.json", parse_dates=["created"])
    consumptions = load_json_to_dataframe("consumptions.json", parse_dates=["time"])

    return items, users, consumptions

"""
return number of consumptions by user
"""
def get_user_consumption_count(user_id: int, consumptions) -> int:
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    return len(user_consumptions)

"""
return top items consumed by user
"""
def get_user_top_items(user_id: int, consumptions, items, top_n=5):
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    top_items_counts = user_consumptions["item_id"].value_counts().head(top_n)
    top_items = top_items_counts.index.tolist()
    
    top_item_details = items[items["item_id"].isin(top_items)]
    top_item_details = top_item_details.set_index("item_id").loc[top_items].reset_index()
    
    names = top_item_details["name"].tolist()
    counts = top_items_counts.tolist()
    
    return list(zip(names, counts))

"""
return top categories consumed by user
"""
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

    # return cateogries with counts
    top_category_counts = (
        merged["category"]
        .value_counts()
        .head(top_n)
        .tolist()
        )
    return list(zip(top_categories, top_category_counts))

"""
return number of unique items consumed by user
"""
def get_user_variety(user_id: int, consumptions) -> int:
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    unique_items = user_consumptions["item_id"].nunique()
    return unique_items

"""
generate recap for a user id

Args:
    user_id: user id to generate recap for
    consumptions: consumptions DataFrame
    items: items DataFrame
"""
def gen_user_recap(user_id: int, consumptions=None, items=None) -> dict:
    serialised_recap = {
        "user_id": user_id,
        "recap": {
            "consumption_count": get_user_consumption_count(user_id, consumptions),
            "top_items": get_user_top_items(user_id, consumptions, items, top_n=5),
            "top_categories": get_user_top_categories(user_id, consumptions, items, top_n=5),
            "variety": get_user_variety(user_id, consumptions)
        }
    }

    return serialised_recap

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

    for user_id in [5, 6, 8]:
        serialised_recap = gen_user_recap(
            user_id,
            consumptions=consumptions,
            items=items
        )

        print(serialised_recap)

if __name__ == "__main__":
    main()

