import sqlite3
import os
from multiprocessing import Pool, cpu_count

# Database path
DB_PATH = os.path.join('outputs', 'data_all.db')

def get_db_connection():
    """Create a connection with a long timeout for concurrent access."""
    conn = sqlite3.connect(DB_PATH, timeout=120)  # High timeout for concurrent writes
    # Enable WAL mode for better performance in multi-process environments
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def update_column_values(cursor, table_name, column_name, mapping_dict):
    try:
        print(f"Executing update logic for {table_name}.{column_name}...")
        
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
        print(f"Rows affected in {table_name}.{column_name}: {cursor.rowcount}")
        
    except Exception as e:
        print(f"Failed to update {table_name}.{column_name}: {e}")

def update_table_column(cursor, prefix, column_name, table_name):
    """
    General function to update a column in a table based on a code prefix in RC table.
    """
    try:
        print(f"\n--- Checking Prefix '{prefix}' for column '{table_name}.{column_name}' ---")
        
        # We search in RC for codes starting with the prefix
        cursor.execute("SELECT CODE, [DESC] FROM RC WHERE CODE LIKE ?", (f"{prefix}%",))
        rows = cursor.fetchall()
        
        mapping = {}
        prefix_len = len(str(prefix))
        for code, desc in rows:
            if code and len(code) > prefix_len:
                # Suffix is everything after the prefix
                suffix = code[prefix_len:]
                mapping[suffix] = desc

        if not mapping:
            print(f"No mappings found for prefix '{prefix}'.")
            return 0

        print(f"Found {len(mapping)} mappings for prefix '{prefix}'.")

        updated_rows = 0
        # Process updates
        for suffix, description in mapping.items():
            # Using TRIM to handle potential trailing spaces
            cursor.execute(f"""
                UPDATE {table_name} 
                SET {column_name} = ? 
                WHERE TRIM({column_name}) = ?
            """, (description, suffix))
            updated_rows += cursor.rowcount

        print(f"Updated {updated_rows} rows in {table_name}.{column_name}")
        return updated_rows
    except Exception as e:
        print(f"Failed to update {table_name}.{column_name} with prefix {prefix}: {e}")
        return 0

def process_table(table):
    """Worker function to process a single table."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Start transaction for this table
        conn.execute("BEGIN TRANSACTION")
        
        print(f"\n{'='*20}\nUpdating Table: {table}\n{'='*20}")
        if table == "FILE_ALL":
        # Mapping based updates (from RC table)
            update_table_column(cursor, "44", "SECTION", table) # الشعبة
            update_table_column(cursor, "3", "DES", table)     # العنوان الوظيفي
            update_table_column(cursor, "3", "DES2", table)    # العنوان الوظيفي في الملاك
            update_table_column(cursor, "5", "OLD_DES", table) # المهنة للحرفيين
            update_table_column(cursor, "45", "UNIT", table)   # الوحدة
            update_table_column(cursor, "210", "LOC", table)   # الموقع
            update_table_column(cursor, "130", "DEP", table)   # القسم
            update_table_column(cursor, "0210", "DIV", table)  # هيئة
            update_table_column(cursor, "65", "UNV", table)    # الجامعة
            update_table_column(cursor, "66", "COL", table)    # الكلية
            update_table_column(cursor, "677", "MOH", table)   # الشهادة
            update_table_column(cursor, "7", "IKTE", table)    # الاختصاص 
            update_table_column(cursor, "757", "NAG", table)   # القومية
            update_table_column(cursor, "7535", "M_STATUS", table) #الحالة الزوجية
            update_table_column(cursor, "754", "CONCE", table) #ذوي الشهداء
            update_table_column(cursor, "756", "NE", table) #سبب ايقاف الموظف
            update_column_values(cursor, table, "SIND", {"1": "فعال", "4": "غير فعال"})
            update_column_values(cursor, table, "SEX", {"1": "ذكر", "2": "انثى"})
            update_column_values(cursor, table, "KHOM", {"1": "صباحي", "2": "مسائي"})
            update_column_values(cursor, table, "DG2", {"1": "أمومة", "2": "أجازة طويلة"})
        if table == "F_SHHD":
            update_table_column(cursor, "65", "UNV", table)    # الجامعة
            update_table_column(cursor, "66", "COL", table)    # الكلية
            update_table_column(cursor, "677", "MOH", table)   # الشهادة
            update_table_column(cursor, "7", "IKTE", table)    # الاختصاص 
        if table == "F_DES":
            update_table_column(cursor, "3", "OLD_DES", table)
            update_table_column(cursor, "3", "DES_ALL", table)
            update_column_values(cursor, table, "TYPE", {"1": "عادي", "2": "مؤرشف"})
        # if table == "F_THANKS":
        #     update_table_column(cursor, "0011", "CODE", table)
        if table == "F_CONG":
            update_table_column(cursor, "130", "DEP", table)
            update_table_column(cursor, "0210", "DIV", table)  # هيئة
            #update_column_values(cursor, table, "NA", {"1": "اكملهم", "2": "لجنة "})
            update_column_values(cursor, table, "SIND", {"1": "فعال", "4": "غير فعال"})
        if table == "F_FRIEND":
            update_table_column(cursor, "0011", "TYP_TKR", table)
        
        if table == "F_REP":
            update_column_values(cursor, table, "RE", {"1": "أمتياز", "2": "جيد جدا", "3": "جيد", "4": "متوسط", "5": "مقبول", "6": "ضعيف"})
            #ضعيف \ متوسط \ جيد \ جيد جدا \ امتياز
        if table == "F_TRAINI":
            update_column_values(cursor, table, "TYPE", {"1": "مركزية", "2": "موقعية", "3": "خارج القطاع", "4": "أيفاد", "5": "داخل القطاع", "6": "مؤتمر", "7": "ندوة", "8": "محاظر", "9": "نشاط"})
            #1- مركزية \ 2- موقعية\ 3- خارج القطاع\ أيفاد \ داخل القطاع\ مؤتمر\ ندوة\محاظر\نشاط
        # Direct value updates (CASE statements)
        conn.commit()
        print(f"Finished table {table}")
        conn.close()
    except Exception as e:
        print(f"An unexpected error occurred while processing table {table}: {e}")

def main():
    if not os.path.exists(DB_PATH):
        print(f"Error: Database file not found at {DB_PATH}")
        return

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Get all table names automatically
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != 'RC'")
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print(f"Found {len(tables)} tables to process. Starting parallel pool...")

        # Use multiprocessing pool to process tables
        with Pool(processes=cpu_count()) as pool:
            pool.map(process_table, tables)

        print("\nAll updates completed successfully for all tables.")

    except Exception as e:
        print(f"A critical error occurred: {e}")

if __name__ == "__main__":
    main()
