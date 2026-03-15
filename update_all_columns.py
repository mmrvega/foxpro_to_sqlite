import sqlite3
import os
import time
from multiprocessing import Pool, cpu_count

# Database path
DB_PATH = 'data_all.db'

def get_db_connection():
    """Create a connection with a long timeout for concurrent access."""
    conn = sqlite3.connect(DB_PATH, timeout=120)  # High timeout for concurrent writes
    # Enable WAL mode for better performance in multi-process environments
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def update_column_values(cursor, table_name, column_name, mapping_dict):
    try:
        # Build the WHEN clauses from the dictionary
        case_clauses = " ".join([
            f"WHEN {column_name} = '{k}' THEN '{v}'" 
            for k, v in mapping_dict.items()
        ])
        
        sql = f"""
            UPDATE {table_name} 
            SET {column_name} = CASE 
                {case_clauses} 
                ELSE ''
            END;
        """
        cursor.execute(sql)
    except Exception as e:
        pass

def update_table_column(cursor, prefix, column_name, table_name):
    try:
        cursor.execute("SELECT CODE, [DESC] FROM RC WHERE CODE LIKE ?", (f"{prefix}%",))
        rows = cursor.fetchall()
        mapping = {}
        prefix_len = len(str(prefix))
        for code, desc in rows:
            if code and len(code) > prefix_len:
                suffix = code[prefix_len:]
                mapping[suffix] = desc
        if not mapping: return 0
        updated_rows = 0
        for suffix, description in mapping.items():
            cursor.execute(f"UPDATE {table_name} SET {column_name} = ? WHERE TRIM({column_name}) = ?", (description, suffix))
            updated_rows += cursor.rowcount
        return updated_rows
    except: return 0

def process_table(table, status_dict=None):
    """Worker function to process a single table."""
    try:
        if status_dict: status_dict[table] = "Processing mappings..."
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.execute("BEGIN TRANSACTION")
        
        if table == "FILE_ALL":
            update_table_column(cursor, "44", "SECTION", table)
            update_table_column(cursor, "3", "DES", table)
            update_table_column(cursor, "3", "DES2", table)
            update_table_column(cursor, "5", "OLD_DES", table)
            update_table_column(cursor, "45", "UNIT", table)
            update_table_column(cursor, "210", "LOC", table)
            update_table_column(cursor, "130", "DEP", table)
            update_table_column(cursor, "0210", "DIV", table)
            update_table_column(cursor, "65", "UNV", table)
            update_table_column(cursor, "66", "COL", table)
            update_table_column(cursor, "677", "MOH", table)
            update_table_column(cursor, "7", "IKTE", table)
            update_table_column(cursor, "757", "NAG", table)
            update_table_column(cursor, "7535", "M_STATUS", table)
            update_table_column(cursor, "754", "CONCE", table)
            update_table_column(cursor, "756", "NE", table)
            update_column_values(cursor, table, "SIND", {"1": "فعال", "4": "غير فعال"})
            update_column_values(cursor, table, "SEX", {"1": "ذكر", "2": "انثى"})
            update_column_values(cursor, table, "KHOM", {"1": "صباحي", "2": "مسائي"})
            update_column_values(cursor, table, "DG2", {"1": "أمومة", "2": "أجازة طويلة"})
        
        if table == "F_SHHD":
            update_table_column(cursor, "65", "UNV", table)
            update_table_column(cursor, "66", "COL", table)
            update_table_column(cursor, "677", "MOH", table)
            update_table_column(cursor, "7", "IKTE", table)
            
        if table == "F_DES":
            update_table_column(cursor, "3", "OLD_DES", table)
            update_table_column(cursor, "3", "DES_ALL", table)
            update_column_values(cursor, table, "TYPE", {"1": "عادي", "2": "مؤرشف"})
            
        if table == "F_CONG":
            update_table_column(cursor, "130", "DEP", table)
            update_table_column(cursor, "0210", "DIV", table)
            update_column_values(cursor, table, "SIND", {"1": "فعال", "4": "غير فعال"})
            
        if table == "F_FRIEND":
            update_table_column(cursor, "0011", "TYP_TKR", table)
        
        if table == "F_REP":
            update_column_values(cursor, table, "RE", {"1": "أمتياز", "2": "جيد جدا", "3": "جيد", "4": "متوسط", "5": "مقبول", "6": "ضعيف"})
            
        if table == "F_TRAINI":
            update_column_values(cursor, table, "TYPE", {"1": "مركزية", "2": "موقعية", "3": "خارج القطاع", "4": "أيفاد", "5": "داخل القطاع", "6": "مؤتمر", "7": "ندوة", "8": "محاظر", "9": "نشاط"})

        conn.commit()
        conn.close()
        if status_dict: status_dict[table] = "✔ Finished"
        return True
    except Exception as e:
        if status_dict: status_dict[table] = f"✘ Error: {str(e)}"
        return False

def get_tables():
    if not os.path.exists(DB_PATH):
        return []
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != 'RC'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        return tables
    except Exception:
        return []

def main():
    tables = get_tables()
    if not tables: return
    with Pool(processes=cpu_count()) as pool:
        pool.map(process_table, tables)

if __name__ == "__main__":
    main()
