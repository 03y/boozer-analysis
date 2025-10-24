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

# set query to return dict
cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

for table in ['items', 'users', 'consumptions']:
    print(f'Exporting {table}... ', end='')

    query = ''
    if table == 'users':
        query = f'SELECT user_id, username, created FROM {table}'
    else:
        query = f'SELECT * FROM {table}'

    # run query
    cur.execute(query)
    rows = cur.fetchall()

    if table == 'users':
        rows

    # write to file
    with open(f'{table}.json', 'w') as f:
        json.dump(rows, f, indent=2, default=str)

    print('Done!')

print('')
cur.close()
conn.close()

