import os

TEMP_DIR = os.path.join(os.getcwd(), "temp_data")
os.makedirs(TEMP_DIR, exist_ok=True)

DUCKDB_PATH = os.path.join(os.getcwd(), "worldpop.db")