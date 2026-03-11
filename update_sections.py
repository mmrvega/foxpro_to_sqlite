import sqlite3
import os

def update_column_values(cursor, column_name, mapping_dict):
    try:
        print(f"Executing update logic for {column_name}...")
        
        # Build the WHEN clauses from the dictionary
        case_clauses = " ".join([
            f"WHEN {column_name} = '{k}' THEN '{v}'" 
            for k, v in mapping_dict.items()
        ])
        
        sql = f"""
            UPDATE FILE_ALL 
            SET {column_name} = CASE 
                {case_clauses} 
                ELSE {column_name} 
            END;
        """
        
        cursor.execute(sql)
        print(f"Rows affected in {column_name}: {cursor.rowcount}")
        
    except Exception as e:
        print(f"Failed to update {column_name}: {e}")

def update_table_column(cursor, prefix, column_name, table_name="FILE_ALL"):
    """
    General function to update a column in a table based on a code prefix in RC table.
    """
    print(f"\n--- Checking Prefix '{prefix}' for column '{column_name}' ---")
    
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
           # print(f"  Mapping: '{suffix}' -> '{desc}'")

    print(f"Found {len(mapping)} mappings for prefix '{prefix}'.")

    updated_rows = 0
    # Process updates
    for suffix, description in mapping.items():
        # Using TRIM to handle potential trailing spaces from DBF format
        cursor.execute(f"""
            UPDATE {table_name} 
            SET {column_name} = ? 
            WHERE TRIM({column_name}) = ?
        """, (description, suffix))
        updated_rows += cursor.rowcount

    print(f"Updated {updated_rows} rows in {table_name}.{column_name}")
    return updated_rows

def main():
    db_path = os.path.join('outputs', 'data_all.db')
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Start transaction
        conn.execute("BEGIN TRANSACTION")
        # start update
        update_table_column(cursor, "44", "SECTION") #الشعبة
        update_table_column(cursor, "3", "DES") #العنوان الوظيفي
        update_table_column(cursor, "3", "DES2") #العنوان الوظيفي في الملاك
        update_table_column(cursor, "5", "OLD_DES")# المهنة للحرفيين
        update_table_column(cursor, "45", "UNIT") #الوحدة
        update_table_column(cursor, "210", "LOC") #الموقع
        update_table_column(cursor, "130", "DEP") #القسم
        update_table_column(cursor, "0210", "DIV") #هيئة
        update_table_column(cursor, "65", "UNV") #الجامعة
        update_table_column(cursor, "66", "COL") #الكلية
        update_table_column(cursor, "677", "MOH") #الشهادة
        update_table_column(cursor, "7", "IKTE")# الاختصاص 
        update_table_column(cursor, "757", "NAG") # القومية
        update_table_column(cursor, "7535", "M_STATUS")
        update_table_column(cursor, "754", "CONCE")
        update_table_column(cursor, "756", "NE")
        update_column_values("SIND", {"1": "فعال", "4": "غير فعال"})
        update_column_values("SEX", {"1": "ذكر", "2": "انثى"})
        update_column_values("KHOM", {"1": "صباحي", "2": "مسائي"})
        update_column_values("KMOH", {"1": "أمومة", "2": "أجازة طويلة"})
        conn.commit()
        print("\nAll updates committed successfully.")

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
