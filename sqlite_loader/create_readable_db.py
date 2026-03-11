"""
Create a human-readable SQLite DB by copying `data_all.db` to `data_all_readable.db`
and adding description columns for code fields based on the `RC` lookup table.

Behavior:
- Copies schema and data from `outputs/data_all.db`.
- Detects which columns in `RC` are code and description (heuristic).
- For each table/column in the copied DB, if values match RC codes, adds a new column
  named `<col>_DESC` and fills it with the corresponding RC description.
- Prints a summary of mapped columns.

Usage:
    python create_readable_db.py

"""
import os
import sqlite3
import collections
import logging

# configure logger with millisecond timestamp
logger = logging.getLogger('readable_db')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
fmt = logging.Formatter('%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
ch.setFormatter(fmt)
logger.addHandler(ch)

# paths
BASE = os.path.dirname(os.path.dirname(__file__))
OUTDIR = os.path.join(BASE, 'outputs')
SRC = os.path.join(OUTDIR, 'data_all.db')
DEST = os.path.join(OUTDIR, 'data_all_readable.db')

if not os.path.exists(SRC):
    logger.error('Source DB not found: %s', SRC)
    raise SystemExit(f'source DB not found: {SRC}')

# remove dest if exists
if os.path.exists(DEST):
    logger.info('Removing existing destination DB: %s', DEST)
    os.remove(DEST)

logger.info('Opening source DB: %s', SRC)
src_conn = sqlite3.connect(SRC)
logger.info('Creating destination DB: %s', DEST)
dest_conn = sqlite3.connect(DEST)
try:
    src_cur = src_conn.cursor()
    dest_cur = dest_conn.cursor()
    # copy table schemas and data
    logger.info('Copying table schemas and data from source to destination')
    for row in src_cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
        tname, sql = row
        logger.debug('Processing schema for table: %s', tname)
        # create table in dest
        if not sql:
            logger.warning('No SQL for table %s, skipping', tname)
            continue
        dest_cur.execute(sql)
        dest_conn.commit()
        # copy rows
        try:
            dest_cur.execute(f'INSERT INTO "{tname}" SELECT * FROM main."{tname}"')
            dest_conn.commit()
            cnt = dest_cur.execute(f'SELECT COUNT(1) FROM "{tname}"').fetchone()[0]
            logger.info('Copied table %s, rows now: %s', tname, cnt)
        except Exception as e:
            logger.warning('Failed to copy rows for %s: %s', tname, e)
    # detect RC table and choose code/desc columns
    rc_exists = dest_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='RC'").fetchone()
    if not rc_exists:
        logger.warning('RC table not found in DB; aborting mapping step')
    else:
        # get RC columns
        rc_cols = [r[1] for r in dest_cur.execute("PRAGMA table_info('RC')").fetchall()]
        rc_sample = list(dest_cur.execute(f'SELECT * FROM RC LIMIT 1000'))
        if not rc_sample:
            logger.warning('RC table empty; aborting mapping step')
        else:
            logger.info('RC table detected with %d columns and %d sample rows', len(rc_cols), len(rc_sample))
            # heuristic: code col = column with many unique short values; desc col = longest avg length
            col_stats = {}
            for idx, col in enumerate(rc_cols):
                vals = [row[idx] for row in rc_sample if row[idx] is not None]
                uniq = len(set(vals))
                avg_len = sum(len(str(v)) for v in vals) / (len(vals) or 1)
                col_stats[col] = (uniq, avg_len)
            # pick code col as max uniq, desc col as max avg_len different from code col
            code_col = max(col_stats.items(), key=lambda x: x[1][0])[0]
            desc_col = max((c for c in col_stats if c != code_col), key=lambda x: col_stats[x][1])
            logger.info('Detected RC mapping columns: %s -> %s', code_col, desc_col)
            # load mapping
            mapping = {str(row[rc_cols.index(code_col)]): str(row[rc_cols.index(desc_col)]) for row in dest_cur.execute(f'SELECT {code_col}, {desc_col} FROM RC').fetchall()}
            logger.info('Loaded %d RC mappings', len(mapping))

            # find candidate code columns in all tables
            mapped = collections.defaultdict(list)
            for trow in dest_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
                t = trow[0]
                cols = [r[1] for r in dest_cur.execute(f'PRAGMA table_info("{t}")').fetchall()]
                for c in cols:
                    # skip if column is the RC table's desc itself
                    if t == 'RC' and c in (code_col, desc_col):
                        continue
                    # sample distinct values
                    try:
                        sample = [r[0] for r in dest_cur.execute(f'SELECT DISTINCT "{c}" FROM "{t}" WHERE "{c}" IS NOT NULL LIMIT 500').fetchall()]
                    except Exception:
                        continue
                    if not sample:
                        continue
                    # compute how many values match mapping keys
                    matches = sum(1 for v in sample if str(v) in mapping)
                    if matches >= 3 and matches / len(sample) >= 0.05:
                        # consider this a code column
                        mapped[t].append((c, matches, len(sample)))
            # apply mappings: add new column and update
            for t, cols in mapped.items():
                for c, matches, total in cols:
                    newcol = f"{c}_DESC"
                    logger.info('Adding %s.%s (matches %d/%d)', t, newcol, matches, total)
                    try:
                        dest_cur.execute(f'ALTER TABLE "{t}" ADD COLUMN "{newcol}" TEXT')
                    except Exception:
                        logger.debug('Column %s already exists or failed to add', newcol)
                    # update via temp mapping table and join
                    dest_cur.execute('CREATE TEMP TABLE IF NOT EXISTS _rc_map(k TEXT, v TEXT)')
                    dest_conn.commit()
                    # clear and insert mapping rows
                    dest_cur.execute('DELETE FROM _rc_map')
                    dest_cur.executemany('INSERT INTO _rc_map(k,v) VALUES(?,?)', [(k,v) for k,v in mapping.items()])
                    dest_conn.commit()
                    # update target
                    update_sql = f'UPDATE "{t}" SET "{newcol}" = (SELECT v FROM _rc_map WHERE k = "{t}"."{c}") WHERE "{c}" IS NOT NULL'
                    dest_cur.execute(update_sql)
                    dest_conn.commit()
            # drop temp mapping table
            dest_cur.execute('DROP TABLE IF EXISTS _rc_map')
            dest_conn.commit()
            # summary
            total_mapped_cols = sum(len(v) for v in mapped.values())
            logger.info('Mapping complete. Total columns mapped: %d', total_mapped_cols)
finally:
    src_conn.close()
    dest_conn.close()

logger.info('Created %s', DEST)
