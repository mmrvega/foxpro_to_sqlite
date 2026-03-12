"""
Convert all top-level CSV files in the workspace into SQLite tables.
- Creates `outputs/data_all.db` (overwrites if exists).
- Each CSV becomes a table named after the file (sanitized).
- All columns are created as TEXT to avoid type errors; NULLs preserved.
- Attempts encodings in order: utf-8, cp1256, windows-1256, latin-1.

Usage:
    python convert_csvs_to_sqlite.py

"""
import os
import csv
import sqlite3
import glob
import re
import argparse
import sys

BASE = os.path.dirname(__file__)  # project root
OUTDIR = BASE
DBPATH = os.path.join(OUTDIR, 'data_all.db')

ENCODINGS = ['utf-8', 'cp1256', 'windows-1256', 'latin-1']

os.makedirs(OUTDIR, exist_ok=True)

def sanitize_identifier(name):
    # keep letters, numbers and underscore
    name = re.sub(r"\s+", "_", name)
    name = re.sub(r"[^0-9A-Za-z_]", "_", name)
    # ensure doesn't start with a digit
    if re.match(r"^[0-9]", name):
        name = '_' + name
    return name

def detect_dialect_and_encoding(path):
    sample = None
    for enc in ENCODINGS:
        try:
            with open(path, 'r', encoding=enc, errors='strict') as f:
                sample = f.read(8192)
            # try sniffing delimiter
            dialect = csv.Sniffer().sniff(sample, delimiters=',;\t')
            return enc, dialect.delimiter
        except Exception:
            continue
    # fallback
    return 'utf-8', ','

def read_header(path, encoding, delimiter):
    with open(path, 'r', encoding=encoding, errors='replace') as f:
        reader = csv.reader(f, delimiter=delimiter)
        header = next(reader)
    return [h.strip() for h in header]

def iter_rows(path, encoding, delimiter):
    with open(path, 'r', encoding=encoding, errors='replace') as f:
        reader = csv.reader(f, delimiter=delimiter)
        next(reader)  # skip header
        for r in reader:
            yield r

def create_table(conn, table, columns):
    cols = [f'"{c}" TEXT' for c in columns]
    sql = f'CREATE TABLE IF NOT EXISTS "{table}" ({", ".join(cols)})'
    conn.execute(sql)

def insert_rows(conn, table, columns, rows_iter, batch=500):
    placeholders = ','.join(['?'] * len(columns))
    sql = f'INSERT INTO "{table}" ({", ".join([f'"{c}"' for c in columns])}) VALUES ({placeholders})'
    cur = conn.cursor()
    batch_rows = []
    count = 0
    for r in rows_iter:
        # pad or trim row to match columns
        if len(r) < len(columns):
            r = r + [''] * (len(columns) - len(r))
        elif len(r) > len(columns):
            r = r[:len(columns)]
        batch_rows.append([v.strip() if v is not None else None for v in r])
        if len(batch_rows) >= batch:
            cur.executemany(sql, batch_rows)
            conn.commit()
            count += len(batch_rows)
            batch_rows = []
    if batch_rows:
        cur.executemany(sql, batch_rows)
        conn.commit()
        count += len(batch_rows)
    return count


def main(args_list=None):
    parser = argparse.ArgumentParser(description='Convert CSV files into SQLite tables')
    parser.add_argument('paths', nargs='*', help='CSV file(s) import')
    parser.add_argument('--all', action='store_true', help='Import all CSV files')
    
    if args_list is not None:
        args = parser.parse_args(args_list)
    else:
        args = parser.parse_args()

    if args.all:
        csv_files = [os.path.basename(p) for p in glob.glob(os.path.join(BASE, '*.csv'))]
    elif args.paths:
        csv_files = []
        for p in args.paths:
            if os.path.exists(p):
                csv_files.append(os.path.basename(p))
            else:
                print(f"Warning: File not found: {p}")
    else:
        parser.print_help()
        sys.exit(1)

    csv_files = [f for f in csv_files if f.lower().endswith('.csv') and 'data_all.db' not in f]
    
    print('Found', len(csv_files), 'CSV files to import')
    if not csv_files:
        return

    # Check for DB existence
    if os.path.exists(DBPATH):
        try:
            # Instead of removing, we might want to append or overwrite. 
            # Original behavior was overwriting if main was called.
            os.remove(DBPATH)
        except Exception as e:
            print(f"Error removing existing database: {e}")
            return

    conn = sqlite3.connect(DBPATH)
    try:
        for fname in csv_files:
            path = os.path.join(BASE, fname)
            print('\nProcessing', fname)
            enc, delim = detect_dialect_and_encoding(path)
            print('  detected encoding:', enc, 'delimiter:', repr(delim))
            try:
                header = read_header(path, enc, delim)
            except Exception as e:
                print('  failed reading header:', e)
                continue
            clean_cols = [sanitize_identifier(c) or f'col_{i}' for i, c in enumerate(header, start=1)]
            table = sanitize_identifier(os.path.splitext(fname)[0])
            print('  creating table:', table)
            create_table(conn, table, clean_cols)
            rows_iter = iter_rows(path, enc, delim)
            n = insert_rows(conn, table, clean_cols, rows_iter)
            print(f'  inserted {n} rows into {table}')
    finally:
        conn.close()
        print('\nSQLite DB saved to', DBPATH)

if __name__ == '__main__':
    main()
