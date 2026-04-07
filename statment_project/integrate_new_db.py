import sqlite3
import os
import sys
from dbfread import DBF
import time

# Ensure UTF-8 output for console
if sys.stdout.encoding != 'utf-8':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

db_path = 'master.db'

def integrate_data():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Process bdate.dbf
    if os.path.exists('bdate.dbf'):
        print("Reading bdate.dbf...")
        b_table = DBF('bdate.dbf', encoding='windows-1256', char_decode_errors='replace')
        cursor.execute("DROP TABLE IF EXISTS bdate")
        cursor.execute("CREATE TABLE bdate (MON TEXT, YER TEXT)")
        for record in b_table:
            mon = str(record.get('MON', '')).strip()
            yer = str(record.get('YER', '')).strip()
            cursor.execute("INSERT INTO bdate (MON, YER) VALUES (?, ?)", (mon, yer))
            break # Only need one row
        conn.commit()

    # 2. Process apdbn.DBF
    if os.path.exists('apdbn.DBF'):
        print("Reading apdbn.DBF...")
        a_table = DBF('apdbn.DBF', encoding='windows-1256', char_decode_errors='replace')
        
        # Drop existing table to ensure schema matches DBF (using all columns)
        cursor.execute("DROP TABLE IF EXISTS apdbn")
        
        # Determine columns to keep
        cols_to_keep = [f.name for f in a_table.fields]
        print(f"Ensuring apdbn table exists with {len(cols_to_keep)} columns...")
        
        # Note: Using CREATE TABLE IF NOT EXISTS to append data if already exists
        cols_def = ", ".join([f'"{c}" TEXT' for c in cols_to_keep])
        cols_def += ', "MON" TEXT, "YER" TEXT'
        cursor.execute(f'CREATE TABLE IF NOT EXISTS apdbn ({cols_def})')

        # Get MON/YER from bdate
        cursor.execute("SELECT MON, YER FROM bdate LIMIT 1")
        b_res = cursor.fetchone()
        mon_val, yer_val = b_res if b_res else (None, None)

        # Prepare insert
        placeholders = ", ".join(["?" for _ in range(len(cols_to_keep) + 2)])
        insert_sql = f'INSERT INTO apdbn ({", ".join([f'"{c}"' for c in cols_to_keep])}, "MON", "YER") VALUES ({placeholders})'
        print(f"Importing and appending {len(a_table)} rows...")
        count = 0
        batch = []
        for record in a_table:
            row = [str(record.get(c)).strip() if record.get(c) is not None else None for c in cols_to_keep]
            row.append(mon_val)
            row.append(yer_val)
            batch.append(row)
            count += 1
            if len(batch) >= 500:
                cursor.executemany(insert_sql, batch)
                conn.commit()
                batch = []
                print(f"  Processed {count} rows...")
        
        if batch:
            cursor.executemany(insert_sql, batch)
            conn.commit()
        
        print(f"Completed apdbn import: {count} rows.")

        print("Cleaning up temporary tables...")
        cursor.execute("DROP TABLE IF EXISTS bdate")
        conn.commit()

    conn.close()
    print("\nIntegration finished successfully thank you for using me ^_^.")

if __name__ == "__main__":
    integrate_data()
