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

class SafeFieldParser(FieldParser):
    def parseD(self, *args, **kwargs):
        try:
            return super().parseD(*args, **kwargs)
        except ValueError:
            return None

def get_process_stats():
    if psutil:
        p = psutil.Process(os.getpid())
        mem = p.memory_info().rss / (1024 * 1024)
        cpu = p.cpu_percent(interval=0.1)
        return f" [RAM: {mem:.1f}MB | CPU: {cpu:.1f}%]"
    return ""

def get_system_stats():
    if psutil:
        cpu = psutil.cpu_percent()
        mem = psutil.virtual_memory().percent
        return f"TOTAL SYSTEM -> RAM: {mem}% | CPU: {cpu}%"
    return ""

def worker_extract(dbf_path, status_dict):
    start_time = time.time()
    try:
        status_dict[dbf_path] = f"Initiating... {get_process_stats()}"
        
        # Use windows-1256 encoding for Arabic natively as requested
        enc = 'windows-1256'
        out_path = os.path.splitext(dbf_path)[0] + '.csv'
        
        table = DBF(dbf_path, encoding=enc, char_decode_errors='replace', parserclass=SafeFieldParser)
        
        with open(out_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            it = iter(table)
            
            def clean_vals(vals):
                return [v.replace('\x00', ' ').replace('\x1a', '') if isinstance(v, str) else v for v in vals]
                
            try:
                first = next(it)
                writer.writerow(first.keys())
                writer.writerow(clean_vals(first.values()))
                count = 1
                for record in it:
                    writer.writerow(clean_vals(record.values()))
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
    # Extract Directly from DBF to CSV with proper Arabic encoding
    with Pool(workers) as pool:
        async_results = [pool.apply_async(worker_extract, (t, status_dict)) for t in targets]
        while any(not r.ready() for r in async_results):
            os.system('cls' if os.name == 'nt' else 'clear')
            print("="*80)
            header = f" PHASE 1: EXTRACTION | Workers: {workers} | {time.strftime('%H:%M:%S')}"
            print(header)
            print(get_system_stats())
            print("="*80)
            sorted_t = sorted(targets, key=lambda x: os.path.getsize(x) if os.path.exists(x) else 0, reverse=True)
            for t in sorted_t:
                print(f" {t:<15} | {status_dict[t]}")
            time.sleep(1)
            
    print("\nPhase 1 Complete. Sleeping 5s before SQLite Import...")
    time.sleep(5)

    # --- PHASE 2 ---
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*80)
    print(f" PHASE 2: SQLITE IMPORT | {time.strftime('%H:%M:%S')}")
    print("="*80)
    try:
        import convert_csvs_to_sqlite
        convert_csvs_to_sqlite.main(['--all'])
        print(f" ✔ Phase 2 COMPLETE")
    except Exception as e:
        print(f" ✘ SQLite Import failed: {e}")

    print("\nPhase 2 Complete. Sleeping 5s before Integration...")
    time.sleep(5)

    # --- PHASE 3 ---
    os.system('cls' if os.name == 'nt' else 'clear')
    print("="*80)
    print(f" PHASE 3: INTEGRATE NEW DB (APDBN & BDATE) | {time.strftime('%H:%M:%S')}")
    print("="*80)
    try:
        import integrate_new_db
        integrate_new_db.integrate_data()
        print(f" ✔ Phase 3 COMPLETE")
    except Exception as e:
        print(f" ✘ Phase 3 Integration failed: {e}")

    print("\n" + "="*80)
    print(" PIPELINE EXECUTION FINISHED")
    print("="*80)

if __name__ == "__main__":
    main()
