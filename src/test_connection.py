from dotenv import load_dotenv
import os
from utils import postgres_db

load_dotenv()

HOST = os.getenv("PG_HOST")
PORT = os.getenv("PG_PORT")
DATABASE = os.getenv("PG_DATABASE")
USERNAME = os.getenv("PG_USER")
PASSWORD = os.getenv("PG_PASSWORD")
TABLE = os.getenv("PG_TABLE")

print(HOST, PORT, DATABASE, USERNAME, PASSWORD)
conn = postgres_db.connect_to_database(host=HOST, port=PORT, database=DATABASE, user=USERNAME, password=PASSWORD)

if conn:
    cursor = conn.cursor()
    cursor.execute(f'SELECT count(1) from {TABLE}')
    rows = cursor.fetchall()
    print(f"Number of rows in the table {TABLE} is {rows[0][0]}")
else:
    print("Could not connect to database. Please check credentials")
