"""
Boozer data analysis

1. Read in json files which have data for items, users & consumptions
2. Analyse this data and generate interesting stats
3. Export out to json files (one generic stats file and a file for each user)
"""

import pandas as pd
import json
import time

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

def get_top_items(consumptions, items, top_n=5, user_id: int=None):
    """
    return top items

    will filter for user_id if provided

    Args:
        user_id: int
    """

    if user_id != None:
        consumptions = consumptions[consumptions["user_id"] == user_id]

    top_items_counts = consumptions["item_id"].value_counts().head(top_n)
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

def get_user_consumptions(user_id: int, consumptions: list):
    return consumptions[consumptions["user_id"] == user_id]

def get_day_distribution(timestamps: list[pd.Timestamp]) -> str:
    """
    return each day of the week and how many consumptions
    """
    counts = {"Monday": 0, "Tuesday": 0, "Wednesday": 0,
            "Thursday": 0, "Friday": 0, "Saturday": 0, "Sunday": 0}
    top_day = {
        "day": "",
        "consumptions":  -1
    }

    for ts in timestamps:
        day = ts.day_name()
        counts[day] += 1

    return counts

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


def get_user_weekly_consumptions(
    user_id: int,
    consumptions,
    week_freq: str = "W-MON",
    include_empty_weeks: bool = True,
    as_dicts: bool = False,
) -> list:
    """
    Return weekly consumption totals for a given user.

    Args:
        user_id: user id to filter consumptions by
        consumptions: pandas DataFrame with at least columns ['user_id', 'time']
                      where 'time' is a datetime-like column
        week_freq: pandas offset alias for week frequency. Default "W-MON"
                   (weekly bins anchored to Mondays). Change to "W-SUN" etc. as needed.
        include_empty_weeks: if True, fills in weeks with zero counts between
                             the first and last week for that user.
        as_dicts: if True, returns a list of dicts: 
                  [{"week_start": "YYYY-MM-DD", "consumptions": int}, ...]

    Returns:
        If as_dicts is False: a list of ints (counts) ordered chronologically.
        If as_dicts is True: a list of dicts with 'week_start' (ISO date string) and 'consumptions'.
    """
    # filter for the user
    user_c = consumptions[consumptions["user_id"] == user_id].copy()
    if user_c.empty:
        return []

    # ensure 'time' is datetime
    user_c["time"] = pd.to_datetime(user_c["time"])

    # group by weekly periods
    weekly_counts = user_c.groupby(pd.Grouper(key="time", freq=week_freq)).size().sort_index()

    # optionally fill missing weeks between first and last
    if include_empty_weeks and not weekly_counts.empty:
        start = weekly_counts.index.min()
        end = weekly_counts.index.max()
        full_idx = pd.date_range(start=start, end=end, freq=week_freq)
        weekly_counts = weekly_counts.reindex(full_idx, fill_value=0)

    # return either simple list of ints or list of dicts with week start date
    if as_dicts:
        return [
            {"week_start": ts.strftime("%Y-%m-%d"), "consumptions": int(count)}
            for ts, count in weekly_counts.items()
        ]
    return [int(x) for x in weekly_counts.tolist()]
    

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
            "recap": {
                "consumptions": {},
                "variety": {},
                "days": {},
                "top_items": {},
                "categories": {}
                }
            }

    serialised_recap["recap"]["consumptions"]["consumption_count"] = get_user_consumption_count(user_id, consumptions)

    # if user has no consumptions then skip
    if serialised_recap["recap"]["consumptions"]["consumption_count"] == 0:
        return

    # weekly consumption totals (list of dicts with week_start + consumptions)
    serialised_recap["recap"]["consumptions"]["weekly_counts"] = get_user_weekly_consumptions(
        user_id, consumptions, as_dicts=True
    )

    serialised_recap["recap"]["top_items"] = []
    for (item, count) in get_top_items(consumptions, items, top_n=5, user_id=user_id):
        serialised_recap["recap"]["top_items"].append({"name": item, "consumptions": count})
    serialised_recap["recap"]["categories"] = []
    for (category, count) in get_user_top_categories(user_id, consumptions, items, top_n=1000):
        serialised_recap["recap"]["categories"].append({"category": category, "consumptions": count})
    serialised_recap["recap"]["variety"] = get_user_variety(user_id, consumptions)

    timestamps = consumptions[consumptions["user_id"] == user_id]["time"].tolist()
    serialised_recap["recap"]["days"] = get_day_distribution(timestamps)

    return serialised_recap

def gen_global_recap(consumptions=None, items=None, users=None) -> dict:
    """
    generate global recap

    Args:
        consumptions: consumptions DataFrame
        items: items DataFrame
    """
    serialised_recap = {
        "recap": {
            "consumptions": {
                "count": len(consumptions)
            },
            "items": {
                "count": len(items),
                "top_items": []
            },
            "users": {
                "count": len(users)
            },
        }
    }

    for (item, count) in get_top_items(consumptions, items, top_n=5):
        serialised_recap["recap"]["items"]["top_items"].append({"name": item, "consumptions": count})

    return serialised_recap

def main():
    # ********************************************************************************
    # load data
    # ********************************************************************************

    items, users, consumptions = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    recaps = []

    # vars for second pass
    consumption_counts = []

    try:
        items, users, consumptions = load_data()
    except Exception as e:
        print(e)

    for table in [items, users, consumptions]:
        if len(table) == 0:
            print("Loading data failed")
            exit(1)
    print(f"Loaded {len(items)} items, {len(users)} users and {len(consumptions)} consumptions")

    # ********************************************************************************
    # global recap
    # ********************************************************************************
    print("\nGenerating global recap")

    global_recap = gen_global_recap(consumptions, items, users)

    with open("global.json", "w") as f:
        json.dump(global_recap, f)

    print("Generated global recap")

    # ********************************************************************************
    # user recaps
    # ********************************************************************************
    print("\nGenerating user recaps")

    # first pass
    print("Running first pass...")
    for user_id in users["user_id"].tolist():
        # TODO: remove this
        if user_id == 1:
            continue

        recap = gen_user_recap(user_id, consumptions=consumptions, items=items)
        if recap != None:
            recaps.append(recap)
            consumption_counts.append(recap["recap"]["consumptions"]["consumption_count"])

    # second pass
    print("Running second pass...")
    for user in recaps:
        user["recap"]["consumptions"]["percentile"] = round(get_percentile(
                user["recap"]["consumptions"]["consumption_count"],
                consumption_counts
                ))

    print(f"Generated recaps for {len(recaps)} users")

    with open("recaps.json", "w") as f:
        json.dump(recaps, f)
    print("Exported recaps to recaps.json")

if __name__ == "__main__":
    main()

