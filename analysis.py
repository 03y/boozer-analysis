"""
Boozer data analysis

1. Read in json files which have data for items, users & consumptions
2. Analyse this data and generate interesting stats
3. Export out to json files (one generic stats file and a file for each user)
"""

import pandas as pd
import json

def load_json_to_dataframe(json_file, parse_dates=None):
    """
    Load a JSON file into a pandas DataFrame.

    Args:
        json_file: Path to JSON file
        parse_dates: List of column names to parse as dates
    """
    df = pd.read_json(json_file)

    if parse_dates:
        for col in parse_dates:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], unit="s")

    return df

def load_data():
    """
    Load all data files into pandas DataFrames.
    """
    items = load_json_to_dataframe("items_classified.json", parse_dates=["added"])
    users = load_json_to_dataframe("users.json", parse_dates=["created"])
    consumptions = load_json_to_dataframe("consumptions.json", parse_dates=["time"])

    return items, users, consumptions

def get_user_consumption_count(user_id: int, consumptions) -> int:
    """
    return number of consumptions by user
    """
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    return len(user_consumptions)

def get_user_top_items(user_id: int, consumptions, items, top_n=5):
    """
    return top items consumed by user
    """
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    top_items_counts = user_consumptions["item_id"].value_counts().head(top_n)
    top_items = top_items_counts.index.tolist()

    top_item_details = items[items["item_id"].isin(top_items)]
    top_item_details = top_item_details.set_index("item_id").loc[top_items].reset_index()

    names = top_item_details["name"].tolist()
    counts = top_items_counts.tolist()

    return list(zip(names, counts))

def get_user_top_categories(user_id: int, consumptions, items, top_n=5):
    """
    return top categories consumed by user
    """
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

def get_user_variety(user_id: int, consumptions) -> int:
    """
    return number of unique items consumed by user
    """
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    unique_items = user_consumptions["item_id"].nunique()
    return unique_items

def get_percentile(value: int, all_values: list[int]) -> float:
    """
    returns the percentile of a value

    e.g. if a user has 100 consumptions, and 90 other users in a list of 100 have equal or less, then they are top 10%
    """
    n_total = len(all_values)

    if n_total == 0:
        return 0.0

    # N_LE: Number of users whose consumption is Less than or Equal to the target 'value'.
    # This count determines the rank of the value.
    n_le = sum(1 for v in all_values if v <= value)

    # Formula: Percentile Rank = (N_LE / N_Total) * 100
    percentile_rank = (n_le / n_total) * 100

    return 100-min(percentile_rank, 100.0)

def gen_user_recap(user_id: int, consumptions=None, items=None) -> dict:
    """
    generate recap for a user id

    Args:
        user_id: user id to generate recap for
        consumptions: consumptions DataFrame
        items: items DataFrame
    """
    serialised_recap = {
        "user_id": user_id,
        "recap": {}
    }

    serialised_recap["recap"]["consumption_count"] = get_user_consumption_count(user_id, consumptions)
    serialised_recap["recap"]["top_items"] = get_user_top_items(user_id, consumptions, items, top_n=5)
    serialised_recap["recap"]["categories"] = get_user_top_categories(user_id, consumptions, items, top_n=50)
    serialised_recap["recap"]["variety"] = get_user_variety(user_id, consumptions)

    return serialised_recap

def main():
    items, users, consumptions = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    recaps = []

    try:
        items, users, consumptions = load_data()
    except Exception as e:
        print(e)

    for table in [items, users, consumptions]:
        if len(table) == 0:
            print("Loading data failed")
            exit(1)

    # first pass
    for user_id in users["user_id"].tolist():
        # TODO: remove this
        if user_id == 1:
            continue

        recaps.append(
            gen_user_recap(
                user_id,
                consumptions=consumptions,
                items=items
            )
        )

    # second pass
    # serialised_recap["recap"]["consumption_count_percentile"] = ...

    recaps_json = json.dumps(recaps)
    print(recaps_json)

if __name__ == "__main__":
    main()
