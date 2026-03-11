"""
Load RC.csv and apply code->description mappings to outputs/data_all_readable.db.
- Assumes RC.csv first column = code, second column = description (common layout).
- For each table in the DB, for each column, samples distinct values and if values match RC codes
  (>=3 matches and >=5% of sample), creates `<col>_DESC` and fills descriptions.

Usage:
    python apply_rc_csv_mappings.py
"""
import os
import csv
import sqlite3
import logging

# logging
logger = logging.getLogger('apply_rc')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(ch)

BASE = os.path.dirname(os.path.dirname(__file__))
OUTDIR = os.path.join(BASE, 'outputs')
RC_CSV = os.path.join(BASE, 'RC.csv')
DB = os.path.join(OUTDIR, 'data_all_readable.db')

if not os.path.exists(RC_CSV):
    logger.error('RC.csv not found at %s', RC_CSV)
    raise SystemExit(1)
if not os.path.exists(DB):
    logger.error('Readable DB not found at %s', DB)
    raise SystemExit(1)

# load mapping from RC.csv (first col -> second col)
mapping = {}
with open(RC_CSV, 'r', encoding='utf-8', errors='replace') as f:
    reader = csv.reader(f)
    for row in reader:
        if not row:
            continue
        code = str(row[0]).strip()
        desc = str(row[1]).strip() if len(row) > 1 else ''
        if code:
            mapping[code] = desc
logger.info('Loaded %d mappings from RC.csv', len(mapping))

conn = sqlite3.connect(DB)
cur = conn.cursor()
try:
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
    logger.info('Found %d tables', len(tables))
    for t in tables:
        cols = [r[1] for r in cur.execute(f'PRAGMA table_info("{t}")')]
        for c in cols:
            # sample distinct values
            try:
                sample = [row[0] for row in cur.execute(f'SELECT DISTINCT "{c}" FROM "{t}" WHERE "{c}" IS NOT NULL LIMIT 500').fetchall()]
            except Exception as e:
                continue
            if not sample:
                continue
            matches = sum(1 for v in sample if str(v) in mapping)
            if matches >= 3 and matches / len(sample) >= 0.05:
                newcol = f"{c}_DESC"
                logger.info('Mapping detected for %s.%s (%d/%d matches). Adding %s', t, c, matches, len(sample), newcol)
                # add column if missing
                try:
                    cur.execute(f'ALTER TABLE "{t}" ADD COLUMN "{newcol}" TEXT')
                    conn.commit()
                except Exception:
                    pass
                # create temp map table
                cur.execute('CREATE TEMP TABLE IF NOT EXISTS _rc_map(k TEXT, v TEXT)')
                cur.execute('DELETE FROM _rc_map')
                cur.executemany('INSERT INTO _rc_map(k,v) VALUES(?,?)', list(mapping.items()))
                conn.commit()
                # update rows
                update_sql = f'UPDATE "{t}" SET "{newcol}" = (SELECT v FROM _rc_map WHERE k = "{t}"."{c}") WHERE "{c}" IS NOT NULL'
                cur.execute(update_sql)
                conn.commit()
                logger.info('Updated descriptions for %s.%s', t, newcol)
    cur.execute('DROP TABLE IF EXISTS _rc_map')
    conn.commit()
    logger.info('Mapping application complete')
finally:
    conn.close()

logger.info('Done')
