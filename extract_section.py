import sqlite3
import os

def extract_section():
    # Path to the database
    db_path = os.path.join('outputs', 'data_all.db')
    
    if not os.path.exists(db_path):
        print(f"Error: Database file not found at {db_path}")
        return

    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Query to get the SECTION column from FILE_ALL table
        # Based on previous checks, FILE_ALL is the table containing this column
        cursor.execute("SELECT SECTION FROM FILE_ALL")
        
        # Fetch all results
        results = cursor.fetchall()
        
        #print(f"Total rows found: {len(results)}")
        #print("\nFirst 20 sections:")
        for i, row in enumerate(results[:20]):
            if i == 4:
                print(f"{i+1}: {row[0]}")
        print("codes section -------------")
        cursor.execute("SELECT CODE, [DESC] FROM RC WHERE CODE LIKE '44%' ORDER BY CODE ASC;")
        codes = cursor.fetchall()
        print(codes)
        # Optional: Save to a text file if there are many entries
        # with open('extracted_sections.txt', 'w', encoding='utf-8') as f:
        #     for row in results:
        #         f.write(f"{row[0]}\n")
        # print("\nAll sections have been saved to extracted_sections.txt")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    extract_section()
