"""
Update master.db for a sample of employees (default N=50) by merging rows from data_all.db.
- Only processes tables in `data_all.db` that contain a column named `ST_NO` (case-insensitive match).
- For each such table:
  - If the table doesn't exist in master, creates it (schema copied) and copies only rows where ST_NO in sample.
  - If it exists, adds missing columns (as TEXT), updates existing rows for the sampled ST_NO using src values when non-NULL, and inserts rows present in src but missing in master for those ST_NO.

Usage:
    python update_master_sample_50.py [N]

"""
import os
import sqlite3
import sys

BASE = os.path.dirname(os.path.dirname(__file__))
OUTDIR = os.path.join(BASE, 'outputs')
MASTER = os.path.join(OUTDIR, 'master.db')
DATA_ALL = os.path.join(OUTDIR, 'data_all.db')

N = int(sys.argv[1]) if len(sys.argv) > 1 else 50

if not os.path.exists(MASTER):
    raise SystemExit(f"Master DB not found: {MASTER}")
if not os.path.exists(DATA_ALL):
    raise SystemExit(f"Source DB not found: {DATA_ALL}")

conn = sqlite3.connect(MASTER)
conn.execute('PRAGMA foreign_keys=OFF')
try:
    conn.execute('ATTACH DATABASE ? AS src', (DATA_ALL,))
    # get sample ST_NO from master FILE_ALL if exists, else from src.FILE_ALL
    def table_exists(db, t):
        return conn.execute("SELECT name FROM {}.sqlite_master WHERE type='table' AND name=?".format(db), (t,)).fetchone() is not None
    sample_st = []
    if table_exists('main', 'FILE_ALL'):
        rows = conn.execute("SELECT ST_NO FROM FILE_ALL WHERE ST_NO IS NOT NULL LIMIT ?", (N,)).fetchall()
        sample_st = [r[0] for r in rows]
    elif table_exists('src', 'FILE_ALL'):
        rows = conn.execute("SELECT ST_NO FROM src.FILE_ALL WHERE ST_NO IS NOT NULL LIMIT ?", (N,)).fetchall()
        sample_st = [r[0] for r in rows]
    else:
        raise SystemExit('No FILE_ALL table found in master or data_all to sample ST_NO')
    if not sample_st:
        raise SystemExit('No ST_NO values found to sample')
    # normalize to strings
    sample_st = [str(s) for s in sample_st]
    ph = ','.join(['?'] * len(sample_st))
    print(f'Sampled {len(sample_st)} ST_NO from master: first few ->', sample_st[:5])

    # list tables in src
    tables = [r[0] for r in conn.execute("SELECT name FROM src.sqlite_master WHERE type='table'")]
    print('Processing tables with ST_NO in src:', tables)
    for t in tables:
        print('\n--', t)
        # get src columns
        src_cols = [r[1] for r in conn.execute(f'PRAGMA src.table_info("{t}")')]
        # case-insensitive check for ST_NO
        src_cols_upper = [c.upper() for c in src_cols]
        if 'ST_NO' not in src_cols_upper:
            print('  skipping (no ST_NO in src table)')
            continue
        # find actual ST_NO column name (case as in src)
        st_col = src_cols[src_cols_upper.index('ST_NO')]
        # ensure table exists in master
        if not table_exists('main', t):
            # create table using src schema
            src_sql = conn.execute("SELECT sql FROM src.sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()[0]
            if not src_sql:
                print('  no create SQL found, skipping')
                continue
            conn.execute(src_sql)
            conn.commit()
            # copy only sampled rows
            insert_sql = f'INSERT INTO "{t}" SELECT * FROM src."{t}" WHERE src."{t}"."{st_col}" IN ({ph})'
            print('  created table and copying sampled rows')
            conn.execute('BEGIN')
            conn.execute(insert_sql, sample_st)
            conn.commit()
            print('  copied rows for sampled ST_NO')
            continue
        # table exists in master: get master cols
        dst_cols = [r[1] for r in conn.execute(f'PRAGMA main.table_info("{t}")')]
        # add missing columns to master
        to_add = [c for c in src_cols if c not in dst_cols]
        if to_add:
            print('  adding columns to master:', to_add)
            for c in to_add:
                try:
                    conn.execute(f'ALTER TABLE "{t}" ADD COLUMN "{c}" TEXT')
                except Exception as e:
                    print('    failed to add', c, e)
            conn.commit()
        # refresh dst_cols
        dst_cols = [r[1] for r in conn.execute(f'PRAGMA main.table_info("{t}")')]
        # perform updates for sampled ST_NO
        common_cols = [c for c in dst_cols if c in src_cols and c.upper() != 'ST_NO']
        if common_cols:
            print('  updating common columns for sampled ST_NO:', common_cols)
            for c in common_cols:
                # update only when src has non-null
                update_sql = f'''UPDATE "{t}" SET "{c}" = (
                    SELECT src."{t}"."{c}" FROM src."{t}"
                    WHERE src."{t}"."{st_col}" = main."{t}"."{st_col}" AND src."{t}"."{c}" IS NOT NULL
                ) WHERE main."{t}"."{st_col}" IN ({ph}) AND EXISTS(
                    SELECT 1 FROM src."{t}" WHERE src."{t}"."{st_col}" = main."{t}"."{st_col}" AND src."{t}"."{c}" IS NOT NULL
                )'''
                conn.execute('BEGIN')
                conn.execute(update_sql, sample_st)
                conn.commit()
            print('  updates complete')
        else:
            print('  no common columns to update')
        # insert rows for sampled ST_NO that are missing in master
        # build insert column list equal to dst_cols
        select_exprs = []
        for c in dst_cols:
            if c in src_cols:
                select_exprs.append(f'src."{t}"."{c}"')
            else:
                select_exprs.append('NULL')
        insert_sql = f'''INSERT INTO "{t}" ({', '.join([f'"{c}"' for c in dst_cols])})
            SELECT {', '.join(select_exprs)} FROM src."{t}" LEFT JOIN main."{t}" ON main."{t}"."{st_col}" = src."{t}"."{st_col}"
            WHERE src."{t}"."{st_col}" IN ({ph}) AND main."{t}"."{st_col}" IS NULL'''
        conn.execute('BEGIN')
        conn.execute(insert_sql, sample_st)
        conn.commit()
        print('  inserted new rows for sampled ST_NO where missing in master')
    conn.execute('DETACH DATABASE src')
    print('\nSample merge complete')
finally:
    conn.close()
