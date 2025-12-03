#!/usr/bin/env python3

import requests
import json
import time
import sys

API_KEY = ""

with open('items.json', 'r') as f:
    items = json.load(f)

url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"

headers = {
    "Content-Type": "application/json",
    "X-goog-api-key": API_KEY
}

results = []

for item in items:
    item_id = item['item_id']
    name = item['name']

    data = {
        "contents": [
            {
                "parts": [
                    {
                        "text": f"Classify the type of drink this item fits into. Examples of type are [Lager, Pilsner, Stout, Weissbeir, Amber Ale, Cider, Red Wine]. You MUST only respond with your classification and no other words. Item name: {name}"
                    }
                ]
            }
        ]
    }

    try:
        response = requests.post(url, headers=headers, json=data)
        response_json = response.json()

                # Extract the text from the response
        if "candidates" in response_json and len(response_json["candidates"]) > 0:
            classification = response_json["candidates"][0]["content"]["parts"][0]["text"].strip()
        else:
            raise Exception(response_json)
    except Exception as e:
        print(f"Error processing {name}: {e}")
        classification = "error"
        sys.exit(1)

    results.append({
        "item_id": item_id,
        "name": name,
        "classification": classification
    })

    print(results[len(results)-1])

    time.sleep(5)

# Write results to output file
with open('classified_items.json', 'w') as f:
    json.dump(results, f, indent=2)

print(f"\nProcessed {len(results)} items. Results saved to classified_items.json")

