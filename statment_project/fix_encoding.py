import argparse
import csv
import sys
import os

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
        return sum(1 for ch in x if looks_arabic(ch))

    orig_ar_count = arabic_count(s)
    candidates = []
    enc_from_list = ('cp1252', 'latin1', 'iso-8859-6', 'cp1256', 'windows-1256')
    dec_to_list = ('cp1256', 'windows-1256', 'iso-8859-6')
    for enc_from in enc_from_list:
        try:
            b = s.encode(enc_from, errors='strict')
        except Exception:
            try:
                b = s.encode(enc_from, errors='replace')
            except Exception:
                continue
        for dec_to in dec_to_list:
            try:
                candidate = b.decode(dec_to, errors='strict')
            except Exception:
                try:
                    candidate = b.decode(dec_to, errors='replace')
                except Exception:
                    continue
            candidates.append(candidate)
            try:
                b2 = candidate.encode(enc_from, errors='replace')
                for dec2 in ('cp1256', 'windows-1256'):
                    try:
                        candidate2 = b2.decode(dec2, errors='replace')
                    except Exception:
                        continue
                    candidates.append(candidate2)
            except Exception:
                pass

    try_pairs = [('cp1256', 'cp720'), ('windows-1256', 'cp720'), ('cp1252', 'cp720'), ('latin1', 'cp720')]
    for enc_from, dec_to in try_pairs:
        try:
            b = s.encode(enc_from, errors='replace')
            cand = b.decode(dec_to, errors='replace')
            candidates.append(cand)
        except Exception:
            continue

    best = s
    best_score = (orig_ar_count, -s.count('?'))
    for cand in candidates:
        score = (arabic_count(cand), -cand.count('?'))
        if score > best_score:
            best_score = score
            best = cand
    return best

def repair_csv(in_path, out_path):
    """Memory-efficient streaming repair of CSV files."""
    fixed_count = 0
    row_count = 0
    
    with open(in_path, 'r', encoding='utf-8', newline='') as inf:
        reader = csv.DictReader(inf)
        fieldnames = reader.fieldnames
        
        with open(out_path, 'w', encoding='utf-8', newline='') as outf:
            writer = csv.DictWriter(outf, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in reader:
                row_count += 1
                for k, v in row.items():
                    if v:
                        repaired = repair_field(v)
                        if repaired != v:
                            row[k] = repaired
                            fixed_count += 1
                writer.writerow(row)

    return fixed_count, row_count

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('infile')
    parser.add_argument('--out', '-o', default=None)
    args = parser.parse_args()

    out = args.out or (os.path.splitext(args.infile)[0] + '_fixed.csv')
    print(f"Repairing {args.infile} -> {out}")
    fixed, total = repair_csv(args.infile, out)
    print(f"Fixed {fixed} fields across {total} rows.")

if __name__ == '__main__':
    main()
