import sqlite3
import os



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

def main():
    db_path = os.path.join('outputs', 'data_all.db')
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all table names automatically
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != 'RC'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Found {len(tables)} tables to process: {', '.join(tables)}")

        # Start transaction
        conn.execute("BEGIN TRANSACTION")

        for table in tables:
            try:
                print(f"\n{'='*20}\nUpdating Table: {table}\n{'='*20}")
                
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
                update_table_column(cursor, "7535", "M_STATUS", table)
                update_table_column(cursor, "754", "CONCE", table)
                update_table_column(cursor, "756", "NE", table)
                
                # Direct value updates (CASE statements)
                update_column_values(cursor, table, "SIND", {"1": "فعال", "4": "غير فعال"})
                update_column_values(cursor, table, "SEX", {"1": "ذكر", "2": "انثى"})
                update_column_values(cursor, table, "KHOM", {"1": "صباحي", "2": "مسائي"})
                update_column_values(cursor, table, "KMOH", {"1": "أمومة", "2": "أجازة طويلة"})
            except Exception as e:
                print(f"An unexpected error occurred while processing table {table}: {e}")
                continue

        conn.commit()
        print("\nAll updates committed successfully for all tables.")

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        print(f"A critical database error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
