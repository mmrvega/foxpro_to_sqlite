import sqlite3
import os

db_path = os.path.join('outputs', 'data_all.db')
conn = sqlite3.connect(db_path)
cursor = conn.cursor()
cursor.execute("PRAGMA table_info(FILE_ALL)")
for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]}")
conn.close()
