import argparse
import csv
import os
import sys
from dbfread import DBF


def looks_arabic(s):
    ranges = [('\u0600', '\u06FF'), ('\u0750', '\u077F'), ('\u08A0', '\u08FF'), ('\uFB50', '\uFDFF'), ('\uFE70', '\uFEFF')]
    for ch in s:
        for a, b in ranges:
            if a <= ch <= b:
                return True
    return False


def detect_encoding(path, encodings=('cp1256', 'cp1252', 'latin1', 'cp720', 'utf-8')):
    for enc in encodings:
        try:
            table = DBF(path, encoding=enc)
            for i, _ in enumerate(table):
                if i >= 20:
                    break
            return enc
        except UnicodeDecodeError:
            continue
        except Exception:
            # allow other exceptions to propagate
            raise
    return None


def repair_field(s):
    if not isinstance(s, str) or s == '':
        return s

    def arabic_count(x):
        return sum(1 for ch in x if looks_arabic(ch))

    orig_ar_count = arabic_count(s)
    if orig_ar_count > 0 and orig_ar_count >= len(s) // 4:
        return s

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


def convert_and_fix(path, out_path=None, src_encoding=None):
    if out_path is None:
        out_path = os.path.splitext(path)[0] + '.csv'

    encodings_to_try = (src_encoding,) if src_encoding else ('cp1256', 'cp1252', 'latin1', 'cp720', 'utf-8')
    enc = detect_encoding(path, encodings=encodings_to_try)
    if enc is None:
        raise UnicodeDecodeError('detect', b'', 0, 1, 'Could not detect a working encoding')

    table = DBF(path, encoding=enc)

    it = iter(table)
    try:
        first = next(it)
    except StopIteration:
        open(out_path, 'w', encoding='utf-8').close()
        return 0, enc

    fieldnames = list(first.keys())

    with open(out_path, 'w', encoding='utf-8', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        def write_row(rec):
            out = {}
            for k, v in rec.items():
                if isinstance(v, str):
                    out[k] = repair_field(v)
                elif v is None:
                    out[k] = ''
                else:
                    out[k] = str(v)
            writer.writerow(out)

        write_row(first)
        count = 1
        for record in it:
            write_row(record)
            count += 1

    return count, enc


def process_file(dbf, src_encoding=None):
    """Worker used by multiprocessing: convert and post-fix one DBF file."""
    try:
        out = os.path.splitext(dbf)[0] + '.csv'
        count, used_enc = convert_and_fix(dbf, out_path=out, src_encoding=src_encoding)

        # run the fixer to catch any remaining mojibake if available
        try:
            import fix_encoding
            if hasattr(fix_encoding, 'repair_csv'):
                fixed_out = os.path.splitext(out)[0] + '_fixed.csv'
                fixed_count, rows = fix_encoding.repair_csv(out, fixed_out)
                if fixed_count > 0:
                    os.replace(fixed_out, out)
                    msg = f'Wrote {count} records to {out} using encoding {used_enc}; post-fix applied ({fixed_count} fields)'
                else:
                    try:
                        os.remove(fixed_out)
                    except Exception:
                        pass
                    msg = f'Wrote {count} records to {out} using encoding {used_enc}; no post-fix changes'
            else:
                msg = f'Wrote {count} records to {out} using encoding {used_enc}; fixer not available'
        except Exception:
            msg = f'Wrote {count} records to {out} using encoding {used_enc}; fixer not available'
        return (dbf, True, msg)
    except Exception as e:
        return (dbf, False, str(e))


def process_file_star(args):
    return process_file(*args)


def main():
    parser = argparse.ArgumentParser(description='Extract DBF files to UTF-8 CSV and fix Arabic mojibake')
    parser.add_argument('paths', nargs='*', help='DBF file(s) or directory; if omitted all .DBF files in cwd are processed')
    parser.add_argument('--encoding', '-e', help='Force source encoding', default=None)
    parser.add_argument('--workers', '-w', type=int, default=None, help='Number of parallel workers (defaults to CPU count)')
    args = parser.parse_args()

    targets = []
    if not args.paths:
        targets = [f for f in os.listdir('.') if f.upper().endswith('.DBF')]
    else:
        for p in args.paths:
            if os.path.isdir(p):
                targets.extend([os.path.join(p, f) for f in os.listdir(p) if f.upper().endswith('.DBF')])
            else:
                targets.append(p)

    if not targets:
        print('No DBF files found to process.', file=sys.stderr)
        sys.exit(2)

    # Report total and start multiprocessing
    total = len(targets)
    print(f'Found {total} DBF file(s) to process; using multiprocessing')

    from multiprocessing import Pool

    workers = args.workers or os.cpu_count() or 2
    print(f'Using {workers} worker(s)')

    # prepare args for starmap: (dbf, src_encoding)
    jobs = [(dbf, args.encoding) for dbf in targets]

    # use imap_unordered to receive results as each worker finishes and update a progress bar
    bar_len = 40
    completed = 0
    with Pool(processes=workers) as pool:
        for result in pool.imap_unordered(process_file_star, jobs):
            completed += 1
            dbf, success, msg = result
            filled = int(bar_len * completed / total)
            bar = '[' + '#' * filled + '-' * (bar_len - filled) + f'] {completed}/{total}'
            if success:
                print(f'{bar} {os.path.basename(dbf)}: {msg}', flush=True)
            else:
                print(f'{bar} {os.path.basename(dbf)}: ERROR {msg}', file=sys.stderr, flush=True)


if __name__ == '__main__':
    main()
