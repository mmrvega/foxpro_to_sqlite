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

def looks_arabic(s):
    ranges = [('\u0600', '\u06FF'), ('\u0750', '\u077F'), ('\u08A0', '\u08FF'), ('\uFB50', '\uFDFF'), ('\uFE70', '\uFEFF')]
    for ch in s:
        for a, b in ranges:
            if a <= ch <= b:
                return True
    return False

def repair_field(s):
    if not isinstance(s, str) or s == '':
        return s
    
    def arabic_count(x):
        score = 0
        for ch in x:
            if '\u0621' <= ch <= '\u064A': # Standard letters
                score += 2
            elif looks_arabic(ch):
                score += 1
        return score

    orig_ar_score = arabic_count(s)
    if orig_ar_score > len(s) and not any(c in s for c in '¤¢§¬'):
        return s

    candidates = []
    enc_from_list = ('cp1252', 'latin1', 'cp850', 'cp437', 'cp1256', 'cp720')
    dec_to_list = ('cp1256', 'windows-1256', 'cp720', 'iso-8859-6')
    
    for enc_from in enc_from_list:
        try:
            b = s.encode(enc_from, errors='strict')
        except:
            continue
        for dec_to in dec_to_list:
            try:
                candidate = b.decode(dec_to, errors='replace')
                candidates.append(candidate)
            except:
                continue
    
    best = s
    best_score = (orig_ar_score, -s.count('?'))
    for cand in set(candidates):
        score = (arabic_count(cand), -cand.count('?'))
        if score > best_score:
            best_score = score
            best = cand
    return best

def get_requested_cols(all_fields):
    # Base columns
    base_cols = [
        'TTNO', 'SNO', 'PMAST', 'TNO', 'GRADE', 'NSTEPN2', 
        'NAME', 'DEPART', 'LOC', 'DRIL', 'ATT', 
        'TMPBASIC', 'BASIC', 'APTOT', 'DEDTOT', 'R47TOT', 
        'GIFT_AMT', 'BROUF', 'CARRF', 'BANK', 'BANKACC', 
        'BIG', 'SML', 'COSTNO', 'TOTAL'
    ]
    
    # Series prefixes
    prefixes = {
        'AP': 16, 'APF': 16, 'APT': 16,
        'DED': 17, 'DEDF': 17, 'DEDT': 17,
        'RD': 16, 'R': 16
    }
    
    requested = base_cols.copy()
    for pref, count in prefixes.items():
        for i in range(1, count + 1):
            requested.append(f"{pref}{i:02d}")
    
    # Filter only those that exist in the DBF
    final_cols = [f for f in requested if f.upper() in [af.upper() for af in all_fields]]
    return final_cols

def integrate_data():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Process bdate.dbf
    if os.path.exists('bdate.dbf'):
        print("Reading bdate.dbf...")
        b_table = DBF('bdate.dbf', encoding='latin-1', char_decode_errors='replace')
        cursor.execute("DROP TABLE IF EXISTS bdate")
        cursor.execute("CREATE TABLE bdate (MON TEXT, YER TEXT)")
        for record in b_table:
            mon = repair_field(str(record.get('MON', '')))
            yer = repair_field(str(record.get('YER', '')))
            cursor.execute("INSERT INTO bdate (MON, YER) VALUES (?, ?)", (mon, yer))
            break # Only need one row
        conn.commit()

    # 2. Process apdbn.DBF
    if os.path.exists('apdbn.DBF'):
        print("Reading apdbn.DBF...")
        a_table = DBF('apdbn.DBF', encoding='latin-1', char_decode_errors='replace')
        
        # Determine columns to keep
        dbf_fields = [f.name for f in a_table.fields]
        cols_to_keep = get_requested_cols(dbf_fields)
        
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

        print(f"Importing and appending {len(a_table)} rows (fixing Arabic)...")

        count = 0
        batch = []
        for record in a_table:
            row = [repair_field(str(record.get(c))) if record.get(c) is not None else None for c in cols_to_keep]
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
