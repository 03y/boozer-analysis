import psycopg2
import psycopg2.extras
import os
import json
from dotenv import load_dotenv

load_dotenv()
# DATABASE_URL='postgres://username:password@localhost:5432/database_name'
DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

# connect to db
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# read recaps
with open('recaps.json', 'r') as f:
    recaps = json.load(f)

# update users table
for item in recaps:
    user_id = item['user_id']
    recap_data = json.dumps(item['recap'])
    
    print(f'Updating user {user_id}... ', end='')
    
    query = "UPDATE users SET recap_2025 = %s WHERE user_id = %s"
    cur.execute(query, (recap_data, user_id))
    
    print('Done!')

# commit changes and close connection
conn.commit()
cur.close()
conn.close()

print('\nImport complete!')
