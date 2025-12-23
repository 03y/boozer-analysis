import psycopg2
import psycopg2.extras
import os
import json
from dotenv import load_dotenv
import pandas as pd
import time
import sys
import requests
import argparse

# Load environment variables from .env file
load_dotenv()

# --- Configuration ---
DATABASE_URL = os.environ.get('DATABASE_URL')
API_KEY = os.environ.get('GEMINI_API_KEY')
CLASSIFIED_ITEMS_CACHE = 'items_classified.json'

# --- Database Functions ---

def export_data():
    """Exports data from the database to pandas DataFrames."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    tables = {}
    for table_name in ['items', 'users', 'consumptions']:
        print(f'Exporting {table_name}... ', end='')
        query = f'SELECT * FROM {table_name}'
        if table_name == 'users':
            query = f'SELECT user_id, username, created FROM {table_name}'
        
        cur.execute(query)
        rows = cur.fetchall()
        tables[table_name] = pd.DataFrame(rows)
        print('Done!')

    cur.close()
    conn.close()
    
    # Parse dates
    tables['items']['added'] = pd.to_datetime(tables['items']['added'], unit="s")
    tables['users']['created'] = pd.to_datetime(tables['users']['created'], unit="s")
    tables['consumptions']['time'] = pd.to_datetime(tables['consumptions']['time'], unit="s")

    return tables['items'], tables['users'], tables['consumptions']

def import_recaps_to_db(recaps):
    """Imports the generated recaps back into the database."""
    if not DATABASE_URL:
        raise ValueError("DATABASE_URL environment variable is not set")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    for item in recaps:
        user_id = item['user_id']
        recap_data = json.dumps(item['recap'])
        
        print(f'Updating user {user_id}... ', end='')
        
        query = "UPDATE users SET recap_2025 = %s WHERE user_id = %s"
        cur.execute(query, (recap_data, user_id))
        
        print('Done!')

    conn.commit()
    cur.close()
    conn.close()

    print('\nImport complete!')

# --- Classification Functions ---

def load_classified_items_cache():
    """Loads the classified items cache from a JSON file."""
    if os.path.exists(CLASSIFIED_ITEMS_CACHE):
        with open(CLASSIFIED_ITEMS_CACHE, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return [] # Return empty list if file is empty or corrupt
    return [] # Return empty list if file doesn't exist

def save_classified_items_cache(cache_list):
    """Saves the classified items cache to a JSON file."""
    with open(CLASSIFIED_ITEMS_CACHE, 'w') as f:
        json.dump(cache_list, f, indent=2)

def classify_items(items_df, cache_list):
    """Classifies items using a generative AI model, with caching."""
    cache_dict = {str(item['item_id']): item for item in cache_list}

    if not API_KEY:
        print("GEMINI_API_KEY environment variable is not set. Using cache and marking new items as 'uncategorised'.")
        classified_items = []
        for _, item in items_df.iterrows():
            item_id = item['item_id']
            name = item['name']
            if str(item_id) in cache_dict:
                classification = cache_dict[str(item_id)].get('category') or cache_dict[str(item_id)].get('classification')
            else:
                classification = 'uncategorised'

            classified_items.append({
                'item_id': item_id,
                'name': name,
                'category': classification
            })
        return pd.DataFrame(classified_items)

    url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent"
    headers = {
        "Content-Type": "application/json",
        "x-goog-api-key": API_KEY
    }

    classified_items = []
    newly_classified = False
    for _, item in items_df.iterrows():
        item_id = item['item_id']
        name = item['name']
        
        if str(item_id) in cache_dict:
            classification = cache_dict[str(item_id)].get('category') or cache_dict[str(item_id)].get('classification')
        else:
            newly_classified = True
            print(f"Classifying '{name}'...")
            data = {
                "contents": [{
                    "parts": [{
                        "text": f"Classify the type of drink this item fits into. Examples of type are [Lager, Pilsner, Stout, Weissbeir, Amber Ale, Cider, Red Wine]. You MUST only respond with your classification and no other words. The text must be capatilsed at the start 'Lager', not 'lager'. Item name: {name}"
                    }]
                }]
            }
            try:
                response = requests.post(url, headers=headers, json=data)
                response.raise_for_status()
                response_json = response.json()
                if "candidates" in response_json and len(response_json["candidates"]) > 0:
                    classification = response_json["candidates"][0]["content"]["parts"][0]["text"].strip()
                else:
                    raise Exception(response_json)
                time.sleep(1) # Respect API rate limits
            except Exception as e:
                print(f"Error processing {name}: {e}", file=sys.stderr)
                classification = "error"

            cache_dict[str(item_id)] = {'item_id': item_id, 'name': name, 'category': classification}

        classified_items.append({
            'item_id': item_id,
            'name': name,
            'category': classification
        })

    if newly_classified:
        save_classified_items_cache(list(cache_dict.values()))

    return pd.DataFrame(classified_items)

# --- Analysis Functions (from analysis.py) ---

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
    top_items_ids = top_items_counts.index.tolist()

    # Get item names from the original items dataframe
    top_item_details = items[items["item_id"].isin(top_items_ids)]
    
    # Create a mapping from item_id to name
    id_to_name = pd.Series(top_item_details.name.values,index=top_item_details.item_id).to_dict()
    
    names = [id_to_name.get(item_id, "Unknown") for item_id in top_items_ids]
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
    top_categories_counts = merged["category"].value_counts().head(top_n)
    
    return list(zip(top_categories_counts.index, top_categories_counts.values))

def get_user_variety(user_id: int, consumptions) -> int:
    """
    return number of unique items consumed by user
    """
    user_consumptions = consumptions[consumptions["user_id"] == user_id]
    unique_items = user_consumptions["item_id"].nunique()
    return unique_items

def get_day_distribution(timestamps: list[pd.Timestamp]) -> str:
    """
    return each day of the week and how many consumptions
    """
    counts = {"Monday": 0, "Tuesday": 0, "Wednesday": 0,
            "Thursday": 0, "Friday": 0, "Saturday": 0, "Sunday": 0}

    for ts in timestamps:
        day = ts.day_name()
        counts[day] += 1

    return counts

def get_percentile(value: int, all_values: list[int]) -> float:
    """
    returns the percentile of a value
    """
    n_total = len(all_values)
    if n_total == 0:
        return 0.0
    n_le = sum(1 for v in all_values if v <= value)
    percentile_rank = (n_le / n_total) * 100
    return 100 - min(percentile_rank, 100.0)

def get_weekly_consumptions(consumptions, user_id: int = None, week_freq: str = "W-MON", include_empty_weeks: bool = True, as_dicts: bool = False) -> list:
    """
    Return weekly consumption totals

    Args:
        user_id: user id to filter consumptions by. If None, get for all users.
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
    if user_id is not None:
        user_c = consumptions[consumptions["user_id"] == user_id].copy()
    else:
        user_c = consumptions.copy()
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

def gen_user_recap(user_id: int, consumptions, items):
    """
    generate recap for a user id
    """
    recap = {
        "user_id": user_id,
        "recap": {
            "consumptions": {},
            "days": {},
            "top_items": [],
            "categories": []
        }
    }

    recap["recap"]["consumptions"]["consumption_count"] = get_user_consumption_count(user_id, consumptions)

    if recap["recap"]["consumptions"]["consumption_count"] == 0:
        return None

    # weekly consumption totals (list of dicts with week_start + consumptions)
    recap["recap"]["weekly_counts"] = get_weekly_consumptions(
        consumptions, user_id=user_id, as_dicts=True
    )

    for item, count in get_top_items(consumptions, items, top_n=5, user_id=user_id):
        recap["recap"]["top_items"].append({"name": item, "consumptions": count})

    for category, count in get_user_top_categories(user_id, consumptions, items, top_n=1000):
        recap["recap"]["categories"].append({"category": category, "consumptions": int(count)})

    recap["recap"]["consumptions"]["variety"] = get_user_variety(user_id, consumptions)

    timestamps = consumptions[consumptions["user_id"] == user_id]["time"].tolist()
    recap["recap"]["days"] = get_day_distribution(timestamps)

    return recap

def gen_global_recap(consumptions, items, users):
    """
    generate global recap
    """
    recap = {
        "consumptions": {"count": len(consumptions)},
        "items": {"count": len(items), "top_items": []},
        "users": {"count": len(users)},
    }

    for item, count in get_top_items(consumptions, items, top_n=5):
        recap["items"]["top_items"].append({"name": item, "consumptions": count})

    # weekly consumption totals (list of dicts with week_start + consumptions)
    recap["weekly_counts"] = get_weekly_consumptions(
        consumptions, user_id=None, as_dicts=True
    )

    return recap

# --- Main Execution ---

def main():
    parser = argparse.ArgumentParser(description='Generate Boozer recaps.')
    parser.add_argument('--import-db', action='store_true', help='Import the generated recaps back into the database.')
    args = parser.parse_args()

    # Step 1: Export data from database
    items, users, consumptions = export_data()
    print(f"Loaded {len(items)} items, {len(users)} users and {len(consumptions)} consumptions")

    # Step 2: Classify items with caching
    classified_items_cache = load_classified_items_cache()
    classified_items_df = classify_items(items, classified_items_cache)
    
    # Merge classified categories into items dataframe
    items = pd.merge(items, classified_items_df[['item_id', 'category']], on='item_id', how='left')


    # Step 3: Generate recaps
    print("\nGenerating global recap")
    global_recap = gen_global_recap(consumptions, items, users)
    with open("global.json", "w") as f:
        json.dump(global_recap, f, indent=2)
    print("Exported global recap to global.json")

    print("\nGenerating user recaps")
    recaps = []
    consumption_counts = []

    # First pass
    for user_id in users["user_id"].tolist():
        recap = gen_user_recap(user_id, consumptions=consumptions, items=items)
        if recap:
            recaps.append(recap)
            consumption_counts.append(recap["recap"]["consumptions"]["consumption_count"])

    # Second pass for percentile
    for user_recap in recaps:
        user_recap["recap"]["consumptions"]["percentile"] = round(get_percentile(
            user_recap["recap"]["consumptions"]["consumption_count"],
            consumption_counts
        ))

    print(f"Generated recaps for {len(recaps)} users")
    with open("recaps.json", "w") as f:
        json.dump(recaps, f, indent=2)
    print("Exported recaps to recaps.json")

    # Step 4: Import recaps into database
    if args.import_db:
        print("\nImporting recaps to database...")
        import_recaps_to_db(recaps)


if __name__ == "__main__":
    main()
