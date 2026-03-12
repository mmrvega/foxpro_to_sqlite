import argparse
import csv
import os
import sys
import time
from multiprocessing import Pool, cpu_count, Manager
from dbfread import DBF, FieldParser

try:
    import psutil
except ImportError:
    psutil = None

# Custom DBF Parser to skip invalid dates
class SafeFieldParser(FieldParser):
    def parseD(self, *args, **kwargs):
        try:
            return super().parseD(*args, **kwargs)
        except ValueError:
            return None

def get_process_stats():
    """Returns current process CPU and Memory usage if psutil is available."""
    if psutil:
        p = psutil.Process(os.getpid())
        mem = p.memory_info().rss / (1024 * 1024) # MB
        # cpu_percent(interval=None) returns since last call. 
        # For workers, we'll just report the state at that moment.
        cpu = p.cpu_percent() 
        return f" [RAM: {mem:.1f}MB | CPU: {cpu:.1f}%]"
    return ""

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
    if orig_ar_count > 0 and orig_ar_count >= len(s) // 4:
        return s

    candidates = []
    enc_from_list = ('cp1252', 'latin1', 'iso-8859-6', 'cp1256', 'windows-1256')
    dec_to_list = ('cp1256', 'windows-1256', 'iso-8859-6')
    for enc_from in enc_from_list:
        try:
            b = s.encode(enc_from, errors='strict')
        except:
            continue
        for dec_to in dec_to_list:
            try:
                candidate = b.decode(dec_to, errors='replace')
                candidates.append(candidate)
            except:
                continue
    
    try:
        b = s.encode('cp1252', errors='replace')
        candidates.append(b.decode('cp720', errors='replace'))
    except:
        pass

    best = s
    best_score = (orig_ar_count, -s.count('?'))
    for cand in candidates:
        score = (arabic_count(cand), -cand.count('?'))
        if score > best_score:
            best_score = score
            best = cand
    return best

def detect_encoding(path):
    for enc in ('cp1256', 'cp1252', 'latin1', 'cp720', 'utf-8'):
        try:
            table = DBF(path, encoding=enc)
            for i, _ in enumerate(table):
                if i >= 10: break
            return enc
        except:
            continue
    return 'cp1256'

def worker_extract(dbf_path, status_dict):
    start_time = time.time()
    try:
        status_dict[dbf_path] = f"Initiating... {get_process_stats()}"
        enc = detect_encoding(dbf_path)
        out_path = os.path.splitext(dbf_path)[0] + '.csv'
        table = DBF(dbf_path, encoding=enc, parserclass=SafeFieldParser)
        
        with open(out_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            it = iter(table)
            try:
                first = next(it)
                writer.writerow(first.keys())
                writer.writerow(first.values())
                count = 1
                for record in it:
                    writer.writerow(record.values())
                    count += 1
                    if count % 10000 == 0:
                         status_dict[dbf_path] = f"Extracting {count} rows... {get_process_stats()}"
                
                elapsed = time.time() - start_time
                status_dict[dbf_path] = f"✔ COMPLETED ({count} rows, {elapsed:.1f}s)"
                return True
            except StopIteration:
                status_dict[dbf_path] = "✔ COMPLETED (Empty)"
                return True
    except Exception as e:
        status_dict[dbf_path] = f"✘ FAILED - {str(e)}"
        return False

def worker_fix(csv_path, status_dict):
    start_time = time.time()
    if not os.path.exists(csv_path):
        status_dict[csv_path] = "✘ Not found"
        return False
    try:
        status_dict[csv_path] = f"Initiating Fix... {get_process_stats()}"
        temp_out = csv_path + ".tmp"
        fixed_count = 0
        row_count = 0
        
        with open(csv_path, 'r', encoding='utf-8', newline='') as inf:
            reader = csv.DictReader(inf)
            fieldnames = reader.fieldnames
            with open(temp_out, 'w', encoding='utf-8', newline='') as outf:
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
                    if row_count % 10000 == 0:
                        status_dict[csv_path] = f"Fixing {row_count} rows... {get_process_stats()}"

        os.replace(temp_out, csv_path)
        elapsed = time.time() - start_time
        status_dict[csv_path] = f"✔ FIXED ({fixed_count} fields, {elapsed:.1f}s)"
        return True
    except Exception as e:
        status_dict[csv_path] = f"✘ FAILED - {str(e)}"
        return False

def main():
    parser = argparse.ArgumentParser(description='FoxPro to SQLite High Performance Pipeline')
    parser.add_argument('--all', action='store_true', help='Process all DBF files')
    parser.add_argument('--workers', '-w', type=int, default=cpu_count())
    args = parser.parse_args()

    if not args.all:
        parser.print_help()
        return

    targets = [f for f in os.listdir('.') if f.upper().endswith('.DBF') and f.upper() != 'RC.DBF']
    if os.path.exists('RC.DBF'): targets.insert(0, 'RC.DBF')

    manager = Manager()
    status_dict = manager.dict()
    for t in targets: status_dict[t] = "Pending..."

    workers = args.workers
    
    # --- PHASE 1 ---
    with Pool(workers) as pool:
        async_results = [pool.apply_async(worker_extract, (t, status_dict)) for t in targets]
        while any(not r.ready() for r in async_results):
            os.system('cls' if os.name == 'nt' else 'clear')
            print("="*80)
            print(f" PHASE 1: EXTRACTION | Workers: {workers} | {time.strftime('%H:%M:%S')}")
            print("="*80)
            # Sort by size to keep the big boys visible
            sorted_t = sorted(targets, key=lambda x: os.path.getsize(x) if os.path.exists(x) else 0, reverse=True)
            for t in sorted_t:
                print(f" {t:<15} | {status_dict[t]}")
            time.sleep(1)

    # --- PHASE 2 ---
    csv_targets = [os.path.splitext(t)[0] + '.csv' for t in targets]
    for c in csv_targets: status_dict[c] = "Pending (Waiting for extraction)..."
    
    with Pool(workers) as pool:
        async_results = [pool.apply_async(worker_fix, (c, status_dict)) for c in csv_targets]
        while any(not r.ready() for r in async_results):
            os.system('cls' if os.name == 'nt' else 'clear')
            print("="*80)
            print(f" PHASE 2: MOJIBAKE FIX | Workers: {workers} | {time.strftime('%H:%M:%S')}")
            print("="*80)
            sorted_c = sorted(csv_targets, key=lambda x: os.path.getsize(x) if os.path.exists(x) else 0, reverse=True)
            for c in sorted_c:
                print(f" {c:<15} | {status_dict[c]}")
            time.sleep(1)

    # --- PHASE 3 ---
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*80)
    print(f" PHASE 3: SQLITE IMPORT | {time.strftime('%H:%M:%S')}")
    print("="*80)
    try:
        import convert_csvs_to_sqlite
        convert_csvs_to_sqlite.main(['--all'])
    except Exception as e:
        print(f" ✘ SQLite Import failed: {e}")

if __name__ == "__main__":
    main()
