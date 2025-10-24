# Boozer data analysis

## Exporting data
1. Install the requirements from `requirements.txt`.
2. Set environment variable `DATABASE_URL='postgres://username:password@localhost:5432/database_name'`.
    - if running the database from the docker container, the details can be found in `docker-compose.yml`.
3. Run `export.py`

You'll have three files: `items.json`, `users.json` and `consumptions.json`.

