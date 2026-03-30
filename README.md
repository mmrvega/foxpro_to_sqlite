# FoxPro to SQLite High Performance Migration Pipeline

A robust, multi-phase pipeline designed to extract data from legacy FoxPro `.DBF` files, repair character encoding artifacts (Mojibake), and synchronize the refined data into a modern SQLite `master.db`.

## 🚀 Key Features

*   **High Performance**: Leverages Python's `multiprocessing` to handle large datasets across multiple CPU cores.
*   **Intelligent Repair**: Phase 2 uses a heuristic scoring system to detect and fix Mojibake (corrupted Arabic text) caused by legacy code pages (CP1256, CP720, etc.).
*   **In-Memory Synchronization**: Phase 5 uses a staging area in RAM (`:memory:`) to compare and sync data, ensuring zero-downtime updates to the production database.
*   **Data Integrity**: Protects critical manual edits in the master database (like `badgeId`) while allowing historical records (Scholarships, Committees, etc.) to be fully synchronized via a "Wipe & Refill" strategy.

---

## 🏗️ The 5-Phase Pipeline

The system operates in five distinct sequential phases:

### Phase 1: Extraction
Converts raw `.DBF` files into intermediate `.CSV` files. It uses a bit-perfect `latin-1` extraction to ensure no data is lost before the repair phase.

### Phase 2: Mojibake Fix
Analyzes every text field in the CSVs. It attempts multiple encoding/decoding combinations to maximize the "Arabic Score" of the text, effectively restoring readable Arabic from legacy artifacts. It also decrypts custom legacy ciphers (e.g., in `F_CONG`).

### Phase 3: SQLite Import
Imports the repaired CSV data into a consolidated `data_all.db` SQLite database.

### Phase 4: DB Mappings & Updates
Translates legacy numeric codes into human-readable descriptions based on the `RC` and `MSOL` mapping tables. This phase handles complex transformations for 20+ tables simultaneously.

### Phase 5: Master Sync
Synchronizes the final processed data into the production `master.db`.
*   **Selective Update**: For the `employees` table, it only updates changed fields and preserves existing `badgeId` values.
*   **Batch Replacement**: For historical tables (`committee`, `academicCertificate`, `trainingCourse`, `lettersOfAppreciation`, `research`, `jobRank`, `jobPosition`, `tenure`), it performs a bulk replacement for all active employees.

---

## 🛠️ Usage

### Prerequisites
*   Python 3.8+
*   Dependencies: `dbfread`, `psutil`

```bash
pip install dbfread psutil
```

### Running the Pipeline
To run the entire 5-phase process:

```powershell
# Run with automatic worker detection (highly recommended)
python extraction.py --all

# Manually specify worker count (e.g., 21 workers)
python extraction.py --all -w 21
```

## 📂 File Structure

*   `extraction.py`: The main orchestrator and entry point.
*   `convert_csvs_to_sqlite.py`: Logic for Phase 3.
*   `update_all_columns.py`: Logic for Phase 4 (Mappings).
*   `compare.py`: Logic for Phase 5 (Master Sync).
*   `data_all.db`: The intermediate staging database.
*   `master.db`: The final destination database.

---

## 📝 Mapping Notes

The system automatically handles mappings for:
*   **Education Levels** (MOH/IKTE)
*   **Job Titles & Positions**
*   **Geographic Locations** (LOC/DEP/DIV)
*   **Personal Status** (Gender, Marital Status)
*   **Evaluations** (Excellent, Very Good, etc.)

## datastructure that i have made the changes to it

03/09/2026  08:16 AM        10,282,926 FILE_ALL.DBF
05/19/2024  01:10 PM           390,734 F_CARD.DBF
02/26/2026  11:39 AM         9,779,786 F_CON.DBF
03/09/2026  10:44 AM        10,341,818 F_CONG.DBF
12/22/2019  10:52 AM         3,635,138 F_CONY.DBF
03/09/2026  08:09 AM         9,378,782 F_DES.DBF
03/09/2026  07:36 AM           254,012 F_FRIEND.DBF
03/09/2026  07:32 AM           233,569 F_MEL.DBF
08/12/2014  11:00 AM           171,766 F_MELOLD.DBF
03/09/2026  07:31 AM           622,047 F_MSOL.DBF
04/03/2024  08:41 AM             3,730 F_NADA.DBF
07/27/2022  11:02 AM           629,733 F_NEED.DBF
03/05/2026  11:35 AM           982,176 F_PUNSH.DBF
01/19/2026  08:18 AM        33,622,916 F_REP.DBF
03/08/2026  11:22 AM           170,374 F_RESER.DBF
03/09/2026  07:42 AM           894,515 F_SHHD.DBF
03/02/2026  10:35 AM         1,252,341 F_SRV.DBF
03/08/2026  08:59 AM        54,850,925 F_THANKS.DBF
03/09/2026  07:48 AM        16,623,191 F_TRAINI.DBF
01/13/2026  09:25 AM             8,690 MSOL.DBF
03/08/2026  10:00 AM         1,206,984 RC.DBF
