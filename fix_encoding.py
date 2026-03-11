import argparse
import csv
import sys


def looks_arabic(s):
    # cover Arabic, Arabic Supplement, Arabic Extended-A, Presentation Forms
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
    # Try common fix: re-interpret bytes that were decoded as cp1252/latin1
    enc_from_list = ('cp1252', 'latin1', 'iso-8859-6', 'cp1256', 'windows-1256')
    dec_to_list = ('cp1256', 'windows-1256', 'iso-8859-6')
    for enc_from in enc_from_list:
        # try strict first
        try:
            b = s.encode(enc_from, errors='strict')
        except Exception:
            try:
                b = s.encode(enc_from, errors='replace')
            except Exception:
                continue
        # try decoding with common Arabic encodings
        for dec_to in dec_to_list:
            try:
                candidate = b.decode(dec_to, errors='strict')
            except Exception:
                try:
                    candidate = b.decode(dec_to, errors='replace')
                except Exception:
                    continue
            candidates.append(candidate)
            # attempt a double-repair (some files were double-encoded)
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

    # additional heuristics: some data is mapped via cp1256 -> cp720 (DOS Arabic)
    try_pairs = [('cp1256', 'cp720'), ('windows-1256', 'cp720'), ('cp1252', 'cp720'), ('latin1', 'cp720')]
    for enc_from, dec_to in try_pairs:
        try:
            b = s.encode(enc_from, errors='replace')
            cand = b.decode(dec_to, errors='replace')
            candidates.append(cand)
        except Exception:
            continue

    # choose best candidate by Arabic character count, break ties by fewer replacement '?' chars
    best = s
    best_score = (orig_ar_count, -s.count('?'))
    for cand in candidates:
        score = (arabic_count(cand), -cand.count('?'))
        if score > best_score:
            best_score = score
            best = cand
    return best
    return s


def repair_csv(in_path, out_path):
    with open(in_path, 'r', encoding='utf-8', newline='') as inf:
        reader = csv.DictReader(inf)
        rows = list(reader)
        fieldnames = reader.fieldnames

    fixed_count = 0
    for row in rows:
        for k, v in row.items():
            if v is None:
                continue
            fixed = repair_field(v)
            if fixed != v:
                row[k] = fixed
                fixed_count += 1

    with open(out_path, 'w', encoding='utf-8', newline='') as outf:
        writer = csv.DictWriter(outf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return fixed_count, len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('infile')
    parser.add_argument('--out', '-o', default=None)
    parser.add_argument('--test', '-t', help='Test a single string (quoted) and print repaired value', default=None)
    args = parser.parse_args()
    if args.test is not None:
        s = args.test
        print('Original :', s)
        print('Repaired :', repair_field(s))
        print('\n-- Candidates (enc_from -> dec_to):')
        enc_from_list = ('cp1252', 'latin1', 'iso-8859-6', 'cp1256', 'windows-1256')
        dec_to_list = ('cp1256', 'windows-1256', 'iso-8859-6', 'cp720', 'cp864', 'utf-8')
        for enc_from in enc_from_list:
            for dec_to in dec_to_list:
                try:
                    b = s.encode(enc_from, errors='strict')
                except Exception:
                    try:
                        b = s.encode(enc_from, errors='replace')
                    except Exception:
                        continue
                try:
                    cand = b.decode(dec_to, errors='strict')
                except Exception:
                    cand = b.decode(dec_to, errors='replace')
                print(f'{enc_from} -> {dec_to} :', cand)
        return

    out = args.out or (args.infile.replace('.csv', '') + '_fixed.csv')
    fixed, total = repair_csv(args.infile, out)
    print(f'Fixed {fixed} fields across {total} rows -> {out}')


if __name__ == '__main__':
    main()
