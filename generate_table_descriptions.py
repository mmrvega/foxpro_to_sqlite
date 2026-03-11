import xml.etree.ElementTree as ET
import csv
import os

BASE = r"c:/Users/mmrma/Desktop/9-3-2026"
DATA3 = os.path.join(BASE, 'data3.xml')
REL = os.path.join(BASE, 'table_column_relationships.txt')
OUTDIR = os.path.join(BASE, 'outputs')

# parse data3.xml mapping
ns = {'ss': 'urn:schemas-microsoft-com:office:spreadsheet'}

def parse_data3(path):
    tree = ET.parse(path)
    root = tree.getroot()
    ws = root.find('.//{urn:schemas-microsoft-com:office:spreadsheet}Worksheet')
    table = ws.find('.//{urn:schemas-microsoft-com:office:spreadsheet}Table')
    rows = table.findall('{urn:schemas-microsoft-com:office:spreadsheet}Row')
    if len(rows) < 2:
        return {}
    # extract cell texts for first two rows
    def row_cells(r):
        vals = []
        for c in r.findall('{urn:schemas-microsoft-com:office:spreadsheet}Cell'):
            d = c.find('{urn:schemas-microsoft-com:office:spreadsheet}Data')
            vals.append(d.text if d is not None else '')
        return vals
    keys = row_cells(rows[0])
    vals = row_cells(rows[1])
    mapping = {}
    for k, v in zip(keys, vals):
        mapping[k.strip()] = (v or '').strip()
    return mapping

mapping = parse_data3(DATA3)

# fallback descriptions from relationships file (simple heuristics)
rel_text = ''
if os.path.exists(REL):
    with open(REL, 'r', encoding='utf-8') as f:
        rel_text = f.read()

def fallback_desc(col):
    # try to find a line that starts with '-' followed by table or contains the column name and a short phrase
    # simple heuristic: search for the column in the relationships text and take surrounding 120 chars
    pos = rel_text.find(col)
    if pos != -1:
        start = max(0, pos-80)
        end = min(len(rel_text), pos+120)
        snippet = rel_text[start:end]
        # return snippet as brief description
        return 'مرجع: ' + snippet.replace('\n',' ').strip()
    return 'وصف غير متوفر — راجع data3.xml أو table_column_relationships.txt'

# list CSV files in workspace (only top-level .csv)
files = [f for f in os.listdir(BASE) if f.lower().endswith('.csv')]

for fname in files:
    path = os.path.join(BASE, fname)
    # read header
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.reader(f)
            header = next(reader)
    except Exception:
        # fallback: try cp1256
        with open(path, 'r', encoding='cp1256', errors='replace') as f:
            reader = csv.reader(f)
            header = next(reader)
    # build description row
    desc_row = []
    for col in header:
        col_key = col.strip()
        if col_key in mapping:
            desc_row.append(mapping[col_key])
        else:
            desc_row.append(fallback_desc(col_key))
    out_path = os.path.join(OUTDIR, fname.replace('.csv', '') + '_columns_description.csv')
    with open(out_path, 'w', encoding='utf-8', newline='') as out:
        writer = csv.writer(out)
        writer.writerow(header)
        writer.writerow(desc_row)

print('Generated', len(files), 'description CSVs in', OUTDIR)
