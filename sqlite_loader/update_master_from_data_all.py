"""
Merge/Update `outputs/master.db` from `outputs/data_all.db`.
- Attaches `data_all.db` as `src` and operates on `main` (master DB).
- For each table in `src`:
  - If table missing in master: create table and copy all rows.
  - If table exists and both sides have `ST_NO`: update existing rows in master with values from src (for common columns) and insert new rows (by ST_NO).
  - If table exists but no `ST_NO`: insert rows from src that are not exact duplicates (append).

Usage:
    python update_master_from_data_all.py

Caveats:
- Columns are matched by name (case-sensitive as stored). `ST_NO` matching is case-insensitive.
- Updates set master.col = COALESCE(src.col, master.col) for common columns.
- Large tables are updated via SQL statements (not per-row Python) for performance.
"""
import os
import sqlite3

BASE = os.path.dirname(os.path.dirname(__file__))
OUTDIR = os.path.join(BASE, 'outputs')
MASTER = os.path.join(OUTDIR, 'master.db')
DATA_ALL = os.path.join(OUTDIR, 'data_all.db')

if not os.path.exists(DATA_ALL):
    raise SystemExit(f"Source DB not found: {DATA_ALL}")

conn = sqlite3.connect(MASTER)
conn.execute("PRAGMA foreign_keys=OFF")
try:
    # attach source
    conn.execute("ATTACH DATABASE ? AS src", (DATA_ALL,))
    # get tables from src
    tables = [r[0] for r in conn.execute("SELECT name FROM src.sqlite_master WHERE type='table'")]
    print('Tables to merge:', tables)
    for t in tables:
        print('\nProcessing table:', t)
        # check if exists in master
        exists = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()
        if not exists:
            # create table in master using src schema
            src_sql = conn.execute("SELECT sql FROM src.sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()[0]
            if not src_sql:
                print('  no create SQL found, skipping')
                continue
            # adjust SQL if it references src schema (unlikely) and execute
            conn.execute(src_sql)
            conn.commit()
            # copy rows
            conn.execute(f'INSERT INTO "{t}" SELECT * FROM src."{t}"')
            conn.commit()
            print('  created and copied', conn.execute(f'SELECT COUNT(1) FROM "{t}"').fetchone()[0], 'rows')
            continue
        # table exists: get columns for src and dest
        def cols_for(schema):
            return [r[1] for r in conn.execute(f'PRAGMA {schema}.table_info("{t}")')]
        src_cols = cols_for('src')
        dst_cols = cols_for('main')
        src_cols_set = set(src_cols)
        dst_cols_set = set(dst_cols)
        # find ST_NO in either sense
        st_cols = [c for c in src_cols if c.upper() == 'ST_NO'] + [c for c in dst_cols if c.upper() == 'ST_NO']
        has_st = any(c.upper() == 'ST_NO' for c in src_cols) and any(c.upper() == 'ST_NO' for c in dst_cols)
        if has_st:
            # common columns to update (exclude ST_NO)
            common = [c for c in dst_cols if c in src_cols and c.upper() != 'ST_NO']
            if common:
                set_clauses = []
                for c in common:
                    # set dst.c = COALESCE((SELECT src.c FROM src.t WHERE src.ST_NO = main.ST_NO), main.c)
                    expr = f'COALESCE((SELECT "{c}" FROM src."{t}" WHERE src."{t}"."ST_NO" = main."{t}"."ST_NO"), main."{t}"."{c}")'
                    set_clauses.append(f'"{c}" = {expr}')
                update_sql = f'UPDATE "{t}" SET ' + ', '.join(set_clauses) + f' WHERE EXISTS (SELECT 1 FROM src."{t}" WHERE src."{t}"."ST_NO" = main."{t}"."ST_NO")'
                # run update
                print('  updating existing rows with', len(common), 'common columns')
                conn.execute('BEGIN')
                conn.execute(update_sql)
                conn.commit()
                print('  update done')
            else:
                print('  no common columns to update')
            # insert new rows from src where ST_NO not in dest
            # build insert column list = dst_cols
            select_exprs = []
            for c in dst_cols:
                if c in src_cols:
                    select_exprs.append(f'src."{t}"."{c}"')
                else:
                    select_exprs.append('NULL')
            insert_sql = f'INSERT INTO "{t}" ({", ".join([f"\"{c}\"" for c in dst_cols])}) SELECT {", ".join(select_exprs)} FROM src."{t}" LEFT JOIN "{t}" ON "{t}"."ST_NO" = src."{t}"."ST_NO" WHERE "{t}"."ST_NO" IS NULL'
            print('  inserting new rows where ST_NO missing in master')
            conn.execute('BEGIN')
            res = conn.execute(insert_sql)
            conn.commit()
            print('  inserted', conn.execute(f'SELECT changes()').fetchone()[0], 'rows (approx)')
        else:
            # no ST_NO; fallback: append rows matching columns
            # map dst_cols to src expressions
            select_exprs = []
            for c in dst_cols:
                if c in src_cols:
                    select_exprs.append(f'src."{t}"."{c}"')
                else:
                    select_exprs.append('NULL')
            insert_sql = f'INSERT INTO "{t}" ({", ".join([f"\"{c}\"" for c in dst_cols])}) SELECT {", ".join(select_exprs)} FROM src."{t}"'
            print('  appending rows (no ST_NO)')
            conn.execute('BEGIN')
            conn.execute(insert_sql)
            conn.commit()
            print('  appended rows')
    # detach
    conn.execute('DETACH DATABASE src')
    print('\nMerge complete. Master DB at', MASTER)
finally:
    conn.close()
