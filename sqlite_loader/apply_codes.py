"""
Extract section codes from `FILE_ALL.section` and map to descriptions using `RC.csv`.
- Copies `outputs/data_all.db` -> `outputs/data_all_sectioned.db`.
- Finds `section` column in `FILE_ALL` (case-insensitive match).
- Adds columns: `section_prefix_DESC`, `section_suffix_DESC`, `section_full_DESC`.
- For each row in `FILE_ALL`, extracts digits from `section` value, prefix=first 2 digits, suffix=rest; looks up descriptions in `RC.csv` mapping (code->desc) and updates the new columns.

Usage:
    python apply_section_codes.py

"""
import os
import shutil
import sqlite3
import csv
import re
import logging

logger = logging.getLogger('section_mapper')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s,%(msecs)03d - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(ch)

BASE = os.path.dirname(os.path.dirname(__file__))
OUTDIR = os.path.join(BASE, 'outputs')
SRC = os.path.join(OUTDIR, 'data_all.db')
DEST = os.path.join(OUTDIR, 'data_all_sectioned2.db')
RC_CSV = os.path.join(BASE, 'RC.csv')

if not os.path.exists(SRC):
    logger.error('Source DB not found: %s', SRC)
    raise SystemExit(1)
if not os.path.exists(RC_CSV):
    logger.error('RC.csv not found: %s', RC_CSV)
    raise SystemExit(1)

# copy DB
if os.path.exists(DEST):
    logger.info('Removing existing destination DB: %s', DEST)
    os.remove(DEST)
shutil.copyfile(SRC, DEST)
logger.info('Copied %s -> %s', SRC, DEST)

# load RC mappings
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
logger.info('Loaded %d RC mappings', len(mapping))

conn = sqlite3.connect(DEST)
cur = conn.cursor()
try:
    # check FILE_ALL exists
    tbl = cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='FILE_ALL'").fetchone()
    if not tbl:
        logger.error('Table FILE_ALL not found in DB')
        raise SystemExit(1)
    # find section column (case-insensitive)
    cols = [r[1] for r in cur.execute('PRAGMA table_info("FILE_ALL")').fetchall()]
    section_col = None
    for c in cols:
        if c.lower() == 'section' or c.lower().endswith('section'):
            section_col = c
            break
    if not section_col:
        logger.error('No column named section found in FILE_ALL (checked: %s)', cols)
        raise SystemExit(1)
    logger.info('Found section column: %s', section_col)
    # add columns if missing
    extras = [
        ('section_prefix_DESC','TEXT'),
        ('section_suffix_DESC','TEXT'),
        ('section_full_DESC','TEXT')
    ]
    existing = set(cols)
    for name, typ in extras:
        if name not in existing:
            try:
                cur.execute(f'ALTER TABLE "FILE_ALL" ADD COLUMN "{name}" {typ}')
                logger.info('Added column FILE_ALL.%s', name)
            except Exception as e:
                logger.warning('Failed to add column %s: %s', name, e)
    conn.commit()

    # fetch rows
    rows = cur.execute(f'SELECT rowid, "{section_col}" FROM FILE_ALL').fetchall()
    logger.info('Processing %d rows in FILE_ALL', len(rows))
    update_sql = f'UPDATE FILE_ALL SET section_prefix_DESC=?, section_suffix_DESC=?, section_full_DESC=? WHERE rowid=?'
    count = 0
    for row in rows:
        rowid, val = row
        s = str(val).strip() if val is not None else ''
        # extract digits
        digits = ''.join(ch for ch in s if ch.isdigit())
        if not digits:
            # update nulls
            cur.execute(update_sql, ('', '', '', rowid))
            continue
        prefix = digits[:2] if len(digits) >= 2 else digits
        suffix = digits[2:] if len(digits) > 2 else ''
        full = digits
        # lookup descriptions
        full_desc = mapping.get(full, '')
        prefix_desc = mapping.get(prefix, '')
        suffix_desc = mapping.get(suffix, '')
        cur.execute(update_sql, (prefix_desc, suffix_desc, full_desc, rowid))
        count += 1
        if count % 5000 == 0:
            conn.commit()
            logger.info('Processed %d rows', count)
    conn.commit()
    logger.info('Completed processing %d rows', count)
finally:
    conn.close()

logger.info('Updated readable DB with section mappings: %s', DEST)
