import sqlite3
import os
import time
import logging
from multiprocessing import Pool, cpu_count

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database path
DB_PATH = 'data_all.db'

def get_db_connection(retries=5):
    """Create a connection with a long timeout and retry mechanism."""
    for i in range(retries):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=120)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            return conn
        except sqlite3.OperationalError as e:
            if "locked" in str(e).lower() and i < retries - 1:
                time.sleep(1)
                continue
            raise
    return sqlite3.connect(DB_PATH, timeout=120)

def update_column_values(cursor, table_name, column_name, mapping_dict):
    """Update values using a CASE statement based on a simple dictionary."""
    if not mapping_dict: return
    try:
        # Build the WHEN clauses from the dictionary
        case_clauses = " ".join([
            f"WHEN TRIM({column_name}) = '{k}' THEN '{v}'" 
            for k, v in mapping_dict.items()
        ])
        
        sql = f"""
            UPDATE {table_name} 
            SET {column_name} = CASE 
                {case_clauses} 
                ELSE {column_name}
            END
            WHERE {column_name} IN ({','.join([f"'{k}'" for k in mapping_dict.keys()])})
        """
        cursor.execute(sql)
        if cursor.rowcount > 0:
            logging.info(f"  [{table_name}] Updated {column_name} (rows: {cursor.rowcount})")
    except Exception as e:
        logging.error(f"  [{table_name}] Error updating {column_name}: {e}")

def update_from_rc(cursor, prefix, column_name, table_name):
    """Update column values by stripping a prefix from RC table codes."""
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
        
        # Build batch update using executemany for efficiency
        update_data = [(v, k) for k, v in mapping.items()]
        cursor.executemany(
            f"UPDATE {table_name} SET {column_name} = ? WHERE TRIM({column_name}) = ?", 
            update_data
        )
        
        if cursor.rowcount > 0:
            logging.info(f"  [{table_name}] Updated {column_name} via RC prefix {prefix}")
        return cursor.rowcount
    except Exception as e:
        logging.error(f"  [{table_name}] Error in update_from_rc for {column_name}: {e}")
        return 0

def update_from_msol(cursor, column_name, table_name):
    """Update column values using descriptions from the MSOL table."""
    try:
        cursor.execute("SELECT CODE, [DESC] FROM MSOL")
        mapping = {str(code).strip(): str(desc).strip() for code, desc in cursor.fetchall() if code}
        if not mapping: return 0
        
        update_data = [(v, k) for k, v in mapping.items()]
        cursor.executemany(
            f"UPDATE {table_name} SET {column_name} = ? WHERE TRIM({column_name}) = ?", 
            update_data
        )
        
        if cursor.rowcount > 0:
            logging.info(f"  [{table_name}] Updated {column_name} from MSOL")
        return cursor.rowcount
    except Exception as e:
        logging.error(f"  [{table_name}] Error in update_from_msol for {column_name}: {e}")
        return 0

def process_table(table, status_dict=None):
    """Worker function to process a single table."""
    try:
        logging.info(f"Starting processing for table: {table}")
        if status_dict: status_dict[table] = "Processing mappings..."
        conn = get_db_connection()
        cursor = conn.cursor()
        conn.execute("BEGIN TRANSACTION")
        
        if table == "FILE_ALL":
            update_from_rc(cursor, "44", "SECTION", table)
            update_from_rc(cursor, "3", "DES", table)
            update_from_rc(cursor, "3", "DES2", table)
            update_from_rc(cursor, "5", "OLD_DES", table)
            update_from_rc(cursor, "45", "UNIT", table)
            update_from_rc(cursor, "210", "LOC", table)
            update_from_rc(cursor, "130", "DEP", table)
            update_from_rc(cursor, "0210", "DIV", table)
            update_from_rc(cursor, "65", "UNV", table)
            update_from_rc(cursor, "66", "COL", table)
            update_from_rc(cursor, "677", "MOH", table)
            update_from_rc(cursor, "7", "IKTE", table)
            update_from_rc(cursor, "757", "NAG", table)
            update_from_rc(cursor, "7535", "M_STATUS", table)
            update_from_rc(cursor, "754", "CONCE", table)
            update_from_rc(cursor, "756", "NE", table)
            update_column_values(cursor, table, "SIND", {"1": "فعال", "4": "غير فعال"})
            update_column_values(cursor, table, "SEX", {"1": "ذكر", "2": "انثى"})
            update_column_values(cursor, table, "KHOM", {"1": "صباحي", "2": "مسائي"})
            update_column_values(cursor, table, "DG2", {"1": "أمومة", "2": "أجازة طويلة"})
            update_from_msol(cursor, "DES3", table)
            cursor.execute(f"UPDATE {table} SET DES3 = 'بدون منصب' WHERE DES3 IS NULL OR TRIM(DES3) = ''")
        
        elif table == "F_SHHD":
            update_from_rc(cursor, "65", "UNV", table)
            update_from_rc(cursor, "66", "COL", table)
            update_from_rc(cursor, "677", "MOH", table)
            update_from_rc(cursor, "7", "IKTE", table)
            
        elif table == "F_DES":
            update_from_rc(cursor, "3", "OLD_DES", table)
            update_from_rc(cursor, "3", "DES_ALL", table)
            update_column_values(cursor, table, "TYPE", {"1": "عادي", "3": "مؤرشف"})
            
        elif table == "F_CONG":
            update_from_rc(cursor, "130", "DEP", table)
            update_from_rc(cursor, "0210", "DIV", table)
            update_column_values(cursor, table, "SIND", {"1": "فعال", "4": "غير فعال"})
            
        elif table == "F_FRIEND":
            update_from_rc(cursor, "0011", "TYP_TKR", table)
        
        elif table == "F_REP":
            update_column_values(cursor, table, "RE", {"1": "أمتياز", "2": "جيد جدا", "3": "جيد", "4": "متوسط", "5": "مقبول", "6": "ضعيف"})
            
        elif table == "F_TRAINI":
            update_column_values(cursor, table, "RESULT", {"1": "أمتياز", "2": "جيد جدا", "3": "جيد", "4": "متوسط", "5": "مقبول", "6": "ضعيف"})
            update_column_values(cursor, table, "TYPE", {"0": "الكترونية","1": "مركزية", "2": "موقعية", "3": "خارج القطاع", "4": "أيفاد", "5": "داخل القطاع", "6": "مؤتمر", "7": "ندوة", "8": "محاظر", "9": "نشاط"})
        
        elif table == "F_THANKS":
            update_from_rc(cursor, "0000", "CODE", table)
        
        elif table == "F_RESER":
            update_column_values(cursor, table, "TKEEM", {"1": "أمتياز", "2": "جيد جدا", "3": "جيد", "4": "متوسط", "5": "مقبول", "6": "ضعيف"})
        
        elif table == "F_MSOL":
            update_from_msol(cursor, "DES_ALL", table)
        conn.commit()
        conn.close()
        if status_dict: status_dict[table] = "✔ Finished"
        logging.info(f"Finished processing for table: {table}")
        return True
    except Exception as e:
        logging.error(f"Error processing table {table}: {e}")
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
    logging.info("Starting update_all_columns.py process")
    tables = get_tables()
    if not tables:
        logging.warning("No tables found to process.")
        return
    logging.info(f"Found {len(tables)} tables to process.")
    for table in tables:
        process_table(table)
    logging.info("update_all_columns.py process completed.")

if __name__ == "__main__":
    main()
