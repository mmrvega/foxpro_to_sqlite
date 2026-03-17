import sqlite3
import os
import uuid
from typing import List, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BASE_PATH = os.path.dirname(os.path.abspath(__file__))
MASTER_DB = os.path.join(BASE_PATH, 'master.db')
DATA_ALL_DB = os.path.join(BASE_PATH, 'data_all.db')
# We will use an in-memory database as a temporary staging area
NEW_MASTER_DB = ':memory:'

def setup_staging_area():
    logging.info("Step 1: Setting up in-memory staging area with master.db schema")
    
    # Clone schema from master to in-memory staging
    conn_master = sqlite3.connect(MASTER_DB)
    conn_staging = sqlite3.connect(':memory:')
    
    cursor_master = conn_master.cursor()
    cursor_master.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    for name, sql in cursor_master.fetchall():
        if sql:
            conn_staging.execute(sql)
            
    conn_master.close()
    conn_staging.commit()
    return conn_staging

def safe_val(val):
    if val is None: return None
    s = str(val).strip()
    return s if s else None

def ensure_column_exists(conn, table_name, col_name, col_type="TEXT"):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    if col_name.lower() == 'id': return
    if col_name not in columns:
        logging.info(f"  Schema Auto-Fix: Adding missing column `{col_name}` to `{table_name}`")
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
        conn.commit()

def sync_schema(conn_src, conn_dest, table_name):
    """Ensure destination table has all columns that source table has."""
    c_src = conn_src.cursor()
    c_dest = conn_dest.cursor()
    
    c_src.execute(f"PRAGMA table_info({table_name})")
    src_cols = {r[1]: r[2] for r in c_src.fetchall()}
    
    c_dest.execute(f"PRAGMA table_info({table_name})")
    dest_cols = {r[1]: r[2] for r in c_dest.fetchall()}
    
    for col, col_type in src_cols.items():
        if col not in dest_cols:
            logging.info(f"  Schema Sync: Adding `{col}` ({col_type}) to `{table_name}` in master.db")
            c_dest.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {col_type}")
    conn_dest.commit()

def migrate_table(conn_data, conn_new, target_table, source_table, mapping_dict):
    logging.info(f"Step 2: Migrating data from {source_table} -> {target_table}")
    c_data = conn_data.cursor()
    c_new = conn_new.cursor()
    
    try:
        c_data.execute(f"SELECT * FROM {source_table}")
    except sqlite3.OperationalError:
        logging.warning(f"  Source table {source_table} not found, skipping.")
        return
        
    columns = [d[0] for d in c_data.description]
    
    # Ensure all target columns exist in the staging DB
    for tgt_col in mapping_dict.values():
        ensure_column_exists(conn_new, target_table, tgt_col)
    
    # fetch target table PRAGMA to handle NOT NULL constraints
    c_new.execute(f"PRAGMA table_info({target_table})")
    target_pragma = c_new.fetchall()
    
    # build a default dictionary for target columns
    default_vals_for_tgt = {}
    for colinfo in target_pragma:
        c_name = colinfo[1]
        c_type = colinfo[2].upper()
        notnull = colinfo[3]
        dflt_value = colinfo[4]
        
        # If NOT NULL and no default value provided in DB schema
        if notnull and dflt_value is None:
            if 'INT' in c_type or 'BOOL' in c_type or 'BIT' in c_type:
                default_vals_for_tgt[c_name] = 0
            else:
                default_vals_for_tgt[c_name] = ""
    
    # Pre-build query focusing ONLY on mapped targets, plus any required defaults
    tgt_cols_to_insert = list(mapping_dict.values())
    
    # Add NOT NULL fields missing
    extra_required_cols = [c for c in default_vals_for_tgt.keys() if c not in tgt_cols_to_insert and c.lower() != 'id']
    tgt_cols_to_insert.extend(extra_required_cols)
    
    placeholders = ', '.join(['?'] * len(tgt_cols_to_insert))
    query = f"INSERT INTO {target_table} ({', '.join(tgt_cols_to_insert)}) VALUES ({placeholders})"
    
    inserted = 0
    batch_data = []
    
    for row in c_data.fetchall():
        row_dict = dict(zip(columns, row))
        employee_id = safe_val(row_dict.get('ST_NO'))
        if not employee_id: continue
        
        vals = []
        # Main mapped values
        for src_col, tgt_col in mapping_dict.items():
            val = safe_val(row_dict.get(src_col))
            if val is None and tgt_col in default_vals_for_tgt:
                val = default_vals_for_tgt[tgt_col]
            vals.append(val)
        
        # Special Case: Default badgeId to employeeId if not mapped
        if target_table == 'employees' and 'badgeId' not in mapping_dict.values() and 'badgeId' in [c[1] for c in target_pragma]:
            # Add badgeId effectively to the row
            # But we need to make sure tgt_cols_to_insert includes it.
            # Actually, extra_required_cols logic below will catch it if it's NOT NULL.
            pass

        # Extra required fields
        for ext_col in extra_required_cols:
            if ext_col == 'badgeId' and target_table == 'employees':
                vals.append(employee_id)
            else:
                vals.append(default_vals_for_tgt[ext_col])
            
        batch_data.append(tuple(vals))
        
        inserted += 1
        if inserted % 5000 == 0:
            c_new.executemany(query, batch_data)
            batch_data = []
            logging.info(f"  Inserted {inserted} rows into {target_table}...")
            
    if batch_data:
        c_new.executemany(query, batch_data)
        
    conn_new.commit()
    logging.info(f"Finished migrating {inserted} records into staging table `{target_table}`.")

def batch_replace_table(conn_master, conn_staging, table_name):
    logging.info(f"Step 3: Replacing ALL records for `{table_name}` in master.db. Resetting IDs to 1.")
    
    # Sync schema to master first
    sync_schema(conn_staging, conn_master, table_name)
    
    c_master = conn_master.cursor()
    c_staging = conn_staging.cursor()

    # 1. Get all staging data
    c_staging.execute(f"SELECT * FROM {table_name}")
    new_cols = [d[0] for d in c_staging.description]
    rows = c_staging.fetchall()
    
    if not rows:
        logging.info(f"  No data in staging for {table_name}, skipping.")
        return

    # 2. Wipe the ENTIRE table in master to allow ID reset
    c_master.execute(f"DELETE FROM {table_name}")
    # Reset auto-increment sequence
    c_master.execute("DELETE FROM sqlite_sequence WHERE name=?", (table_name,))
    
    # 3. Insert all staging data into master
    # Omit 'id' to let master.db generate its own auto-increment integer IDs starting from 1
    insert_cols = [c for c in new_cols if c.lower() != 'id']
    
    # Check master schema for any extra NOT NULL columns we need to respect
    c_master.execute(f"PRAGMA table_info({table_name})")
    master_pragma = c_master.fetchall()
    default_vals_master = {}
    for colinfo in master_pragma:
        c_name = colinfo[1]
        c_type = colinfo[2].upper()
        notnull = colinfo[3]
        if notnull and colinfo[4] is None and c_name not in insert_cols and c_name.lower() != 'id':
             default_vals_master[c_name] = 0 if 'INT' in c_type or 'BOOL' in c_type or 'BIT' in c_type else ""

    final_cols = insert_cols + list(default_vals_master.keys())
    placeholders = ', '.join(['?'] * len(final_cols))
    query = f"INSERT INTO {table_name} ({', '.join([f'\"{c}\"' for c in final_cols])}) VALUES ({placeholders})"
    
    batch_data = []
    for row in rows:
        row_dict = dict(zip(new_cols, row))
        vals = [row_dict[c] for c in insert_cols]
        for dc in default_vals_master:
            vals.append(default_vals_master[dc])
        batch_data.append(tuple(vals))
    
    c_master.executemany(query, batch_data)
    conn_master.commit()
    logging.info(f"  {table_name} Replace: Table wiped and refilled with {len(rows)} records. IDs reset.")

def compare_and_sync_table(conn_master, conn_staging, table_name, force_replace=False):
    logging.info(f"Step 3: Syncing `{table_name}` (Force Replace: {force_replace})")
    
    # Sync schema to master first
    sync_schema(conn_staging, conn_master, table_name)
    
    c_master = conn_master.cursor()
    c_staging = conn_staging.cursor()
    
    # Read staging
    c_staging.execute(f"SELECT * FROM {table_name}")
    new_cols = [d[0] for d in c_staging.description]
    new_rows = c_staging.fetchall()
    
    if not new_rows:
        return # nothing to sync
        
    # Read Master
    try:
        c_master.execute(f"SELECT * FROM {table_name}")
        master_cols = [d[0] for d in c_master.description]
        master_rows = c_master.fetchall()
    except sqlite3.OperationalError:
        logging.warning(f"  Table {table_name} not in master.db.")
        return

    # Check mapping
    if 'employeeId' not in new_cols:
        logging.warning(f"  No employeeId in {table_name}, skipping precise sync.")
        return

    # Create dict mapped by employeeId (This assumes 1 row per employee for simplicity, 
    # but for F_CONG etc.. there can be multiple rows. For now, we will just insert missings based on a unique hash or employeeId.)
    # We will use simple logic: if employeeId is missing entirely in master, insert all their rows.
    # Note: If it's a 1-to-many table (like committee), it's harder to just "update" without a natural key.
    # To keep it safe and avoid duplicating 1-to-many, we'll sync standard columns on 'employees', 
    # and for 1-to-many, we'll wipe their records and insert new, OR just insert if the employee has 0 records.
    
    # For now, let's just do a sync based on employeeId existence in Master to avoid duplicates
    master_emp_ids = set()
    for r in master_rows:
        d = dict(zip(master_cols, r))
        if d.get('employeeId'): master_emp_ids.add(str(d.get('employeeId')))
        
    inserted_count = 0
    updated_count = 0
    
    # we group new rows by employee id
    from collections import defaultdict
    new_emp_rows = defaultdict(list)
    for r in new_rows:
        d = dict(zip(new_cols, r))
        emp_id = str(d.get('employeeId'))
        if emp_id: new_emp_rows[emp_id].append(d)
        
    # Fetch missing required columns in master DB to inject safely during insert
    c_master.execute(f"PRAGMA table_info({table_name})")
    master_pragma = c_master.fetchall()
    default_vals_master = {}
    for colinfo in master_pragma:
        c_name = colinfo[1]
        c_type = colinfo[2].upper()
        notnull = colinfo[3]
        if notnull and colinfo[4] is None:
             default_vals_master[c_name] = 0 if 'INT' in c_type or 'BOOL' in c_type or 'BIT' in c_type else ""

    for emp_id, rows in new_emp_rows.items():
        # If force_replace is on, wipe existing records for this employee first
        is_new_employee = emp_id not in master_emp_ids
        
        if force_replace and table_name != 'employees' and not is_new_employee:
            c_master.execute(f'DELETE FROM {table_name} WHERE employeeId = ?', (emp_id,))
            is_new_employee = True # Treat as new to trigger the insert block
            
        if is_new_employee:
            # Insert all rows for this employee
            for new_data in rows:
                cols_to_insert = [c for c in new_cols if new_data.get(c) is not None and c.lower() != 'id']
                # Enforce NOT NULL safety
                for mc in default_vals_master:
                    if mc not in cols_to_insert and mc.lower() != 'id':
                        cols_to_insert.append(mc)
                        new_data[mc] = default_vals_master[mc]
                
                vals_to_insert = [new_data[c] for c in cols_to_insert]
                placeholders = ', '.join(['?'] * len(cols_to_insert))
                col_names = ', '.join([f'"{c}"' for c in cols_to_insert])
                
                c_master.execute(f'INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})', vals_to_insert)
                inserted_count += 1
        else:
            # If it's the `employees` table (1-to-1) we can UPDATE
            if table_name == 'employees' and len(rows) == 1:
                new_data = rows[0]
                # Find the old row
                master_emp_row = next((r for r in master_rows if str(dict(zip(master_cols, r)).get('employeeId')) == emp_id), None)
                if master_emp_row:
                    old_data = dict(zip(master_cols, master_emp_row))
                    updates = []
                    vals = []
                    for c in new_cols:
                        if c == 'id' or c == 'badgeId': continue # user: 'keep the badgeid' - do not overwrite in master if already exists
                        if new_data.get(c) is not None and str(new_data.get(c)) != str(old_data.get(c)):
                            updates.append(f'"{c}" = ?')
                            vals.append(new_data[c])
                    
                    if updates:
                        vals.append(emp_id)
                        query = f'UPDATE {table_name} SET {", ".join(updates)} WHERE employeeId = ?'
                        c_master.execute(query, vals)
                        updated_count += 1

    conn_master.commit()
    if inserted_count > 0 or updated_count > 0:
        logging.info(f"  {table_name} Sync: Inserted {inserted_count} new records, Updated {updated_count} existing records.")
    else:
        logging.info(f"  {table_name} Sync: No differences found, nothing updated.")

def main():
    logging.info("Starting Conversion and Comparison Process (In-Memory Sync)...")
    
    # 1. Setup in-memory staging area
    conn_staging = setup_staging_area()
    conn_data = sqlite3.connect(DATA_ALL_DB)
    
    # 2. Migrate Data from raw DB to Staging
    # 1. Employees Mapping
    migrate_table(conn_data, conn_staging, target_table='employees', source_table='FILE_ALL', mapping_dict={
        'ST_NO': 'employeeId',
        'NAME': 'name',
        'DES': 'jobTitle',
        'DES3': 'jobPosition',
        'DIV':'authority',
        'DEP': 'department',
        'SECTION': 'division',
        'UNIT': 'unit',
        'LOC': 'location',
        'MOH': 'educationalLevel',
        'IKTE': 'fieldOfStudy',
        'B_PLASE': 'address',
        'NAL': 'religion',
        'NAG': 'ethnic',
        'MATHER': 'motherName',
        'WIFE':'wifename',
        'M_STATUS': 'status',
        'SEX': 'sex'
    })
    
    # 2. Committee Mapping
    migrate_table(conn_data, conn_staging, target_table='committee', source_table='F_CONG', mapping_dict={
        'ST_NO': 'employeeId',
        'CNAME': 'title',
        'NMB': 'adminNo',
        'DATE': 'adminDate',
        'ST_DATE': 'startDate',
        'FN_DATE': 'endDate',
        'CSIDE': 'CSIDE'
    })
    
    # 3. Academic Certificate Mapping
    migrate_table(conn_data, conn_staging, target_table='academicCertificate', source_table='F_SHHD', mapping_dict={
        'ST_NO': 'employeeId',
        'UNV': 'university',
        'COL': 'collage',
        'MOH': 'educationLevel',
        'IKTE': 'specialization',
        'DM':'year'
    })

    # 4. Training Course Mapping
    migrate_table(conn_data, conn_staging, target_table='trainingCourse', source_table='F_TRAINI', mapping_dict={
        'ST_NO': 'employeeId',
        'TYPE': 'courseType',
        'RESULT': 'evaluation',
        'TRAINING': 'title',
        'ST_DATE': 'startDate',
        'FN_DATE': 'endDate',
        'TR_PLACE': 'location'
    })

    # 5. Letters Of Appreciation Mapping
    migrate_table(conn_data, conn_staging, target_table='lettersOfAppreciation', source_table='F_THANKS', mapping_dict={
        'ST_NO': 'employeeId',
        'CODE': 'title',
        'NMB': 'adminNo',
        'DATE': 'adminDate',
        'ORD_NO': 'cause',
        'ORD_SOURCE':'issuingAuthority'
    })

    # 6. Research Mapping
    migrate_table(conn_data, conn_staging, target_table='research', source_table='F_RESER', mapping_dict={
        'ST_NO': 'employeeId',
        'ADDR': 'title',
        'DATE': 'date',
        'TKEEM': 'evaluation',
        'DGREE': 'researchGrade'
    })

    # 7. Job Rank Mapping (F_DES)
    migrate_table(conn_data, conn_staging, target_table='jobRank', source_table='F_DES', mapping_dict={
        'ST_NO': 'employeeId',
        'DES_ALL': 'title',
        'NMB': 'adminNo',
        'D_ORD': 'adminDate',
        'DATE':'startDate',
        'DS': "note"
    })
    # 8. jobPosition Mapping (F_MSOL)
    migrate_table(conn_data, conn_staging, target_table='jobPosition', source_table='F_MSOL', mapping_dict={
        'ST_NO': 'employeeId',
        'DES_ALL': 'jobTitle',
        'NMB': 'adminNo',
        'D_ORD': 'adminDate',
        'DATE': 'startDate',
        'DS': 'jobPosition',
    })

    # 9. Tenure Mapping (F_SRV)
    migrate_table(conn_data, conn_staging, target_table='tenure', source_table='F_SRV', mapping_dict={
        'ST_NO': 'employeeId',
        'ALL':'totall'
    })

    # 10. Annual Performance Mapping (F_REP)
    migrate_table(conn_data, conn_staging, target_table='annualPerformance', source_table='F_REP', mapping_dict={
        'ST_NO': 'employeeId',
        'DATE': 'year',
        'RE': 'rating',
        'REDEG': 'REDEG'
    })


    conn_data.close()
    
    # 3. Use staging area to update master.db
    conn_master = sqlite3.connect(MASTER_DB)
    
    # For employees: Use comparison sync (to preserve badgeId and other manual edits)
    compare_and_sync_table(conn_master, conn_staging, 'employees')
    
    # For historical tables: Use Batch Replace (wipe and refill as per user request)
    for tbl in ['committee', 'academicCertificate', 'trainingCourse', 'lettersOfAppreciation', 'research', 'jobRank', 'jobPosition', 'tenure', 'annualPerformance']:
        batch_replace_table(conn_master, conn_staging, tbl)
        
    conn_master.close()
    conn_staging.close()
    
    logging.info("Process entirely finished. Master.db has been updated directly.")

if __name__ == '__main__':
    main()
