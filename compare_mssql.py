"""Sync the processed SQLite data_all.db into Microsoft SQL Server.

This is the MSSQL replacement for compare.py (the pipeline's final phase).
The source remains the local data_all.db produced by Phase 4; the destination
is an existing SQL Server database accessed through pyodbc.

Examples:
    python compare_mssql.py --server localhost --database NocPortal
    python compare_mssql.py --server localhost --database NocPortal --trusted
    python compare_mssql.py --server localhost --database NocPortal \
        --username sa --password "your-password"
"""
import argparse
import logging
import multiprocessing
import os
import sqlite3
import sys
import time
from datetime import date, datetime
from typing import Dict, Iterable, List, Sequence, Tuple


BASE_PATH = os.path.dirname(os.path.abspath(__file__))
DATA_ALL_DB = os.path.join(BASE_PATH, "data_all.db")
DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_LOG_FILE = os.path.join(BASE_PATH, "compare_mssql.log")
SQL_SERVER_DRIVER_CANDIDATES = (
    "ODBC Driver 18 for SQL Server",
    "ODBC Driver 17 for SQL Server",
    "ODBC Driver 13 for SQL Server",
    "SQL Server",
)

TABLE_MAPPINGS: Dict[str, Tuple[str, Dict[str, str]]] = {
    "employees": ("FILE_ALL", {
        "ST_NO": "employeeId", "NAME": "name", "DES": "jobTitle",
        "DES3": "jobPosition", "DIV": "authority", "DEP": "department",
        "SECTION": "division", "UNIT": "unit", "LOC": "location",
        "MOH": "educationalLevel", "IKTE": "fieldOfStudy", "B_PLASE": "address",
        "ORG": "religion", "NAG": "ethnic", "MATHER": "motherName",
        "WIFE": "wife_name", "HOS": "home_location", "M_STATUS": "status",
        "SEX": "sex",
    }),
    "committee": ("F_CONG", {
        "ST_NO": "employeeId", "CNAME": "title", "NMB": "adminNo",
        "DATE": "adminDate", "ST_DATE": "startDate", "FN_DATE": "endDate",
        "CSIDE": "CSIDE",
    }),
    "academicCertificate": ("F_SHHD", {
        "ST_NO": "employeeId", "UNV": "university", "COL": "collage",
        "MOH": "educationLevel", "IKTE": "specialization", "DM": "year",
    }),
    "trainingCourse": ("F_TRAINI", {
        "ST_NO": "employeeId", "TYPE": "courseType", "RESULT": "evaluation",
        "TRAINING": "title", "ST_DATE": "startDate", "FN_DATE": "endDate",
        "TR_PLACE": "location",
    }),
    "lettersOfAppreciation": ("F_THANKS", {
        "ST_NO": "employeeId", "CODE": "title", "NMB": "adminNo",
        "DATE": "adminDate", "ORD_NO": "cause", "ORD_SOURCE": "issuingAuthority",
    }),
    "research": ("F_RESER", {
        "ST_NO": "employeeId", "ADDR": "title", "DATE": "date",
        "TKEEM": "evaluation", "DGREE": "researchGrade",
    }),
    "jobRank": ("F_DES", {
        "ST_NO": "employeeId", "DES_ALL": "title", "NMB": "adminNo",
        "D_ORD": "adminDate", "DATE": "startDate", "DS": "note",
    }),
    "jobPosition": ("F_MSOL", {
        "ST_NO": "employeeId", "DES_ALL": "jobTitle", "NMB": "adminNo",
        "D_ORD": "adminDate", "DATE": "startDate", "DS": "jobPosition",
    }),
    "tenure": ("F_SRV", {"ST_NO": "employeeId", "ALL": "totall"}),
    "annualPerformance": ("F_REP", {
        "ST_NO": "employeeId", "DATE": "year", "RE": "rating", "REDEG": "REDEG",
    }),
}

REPLACE_TABLES = [
    "committee", "academicCertificate", "trainingCourse", "lettersOfAppreciation",
    "research", "jobRank", "jobPosition", "tenure", "annualPerformance",
]


def configure_logging(log_level: str, log_file: str):
    """Write the same detailed run log to the console and a UTF-8 log file."""
    formatter = logging.Formatter(
        "%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    root_logger.handlers.clear()

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    logging.info("=" * 90)
    logging.info("Starting MSSQL sync; detailed log: %s", os.path.abspath(log_file))


def identifier(name: str) -> str:
    """Quote a SQL Server identifier after validating it is a simple name."""
    if not name.replace("_", "").isalnum():
        raise ValueError(f"Unsafe SQL identifier: {name}")
    return f"[{name}]"


def resolve_driver(requested_driver: str) -> str:
    """Use the requested driver, or fall back to an installed SQL Server driver."""
    import pyodbc

    installed = pyodbc.drivers()
    if requested_driver in installed:
        return requested_driver
    for candidate in SQL_SERVER_DRIVER_CANDIDATES:
        if candidate in installed:
            logging.warning(
                "Configured ODBC driver '%s' is not installed; using '%s'. Installed drivers: %s",
                requested_driver,
                candidate,
                ", ".join(installed) or "none",
            )
            return candidate
    raise RuntimeError(
        "No SQL Server ODBC driver is installed. "
        "Install Microsoft ODBC Driver 17 or 18 for SQL Server, or set --driver. "
        f"Installed drivers: {', '.join(installed) or 'none'}"
    )


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def comparable_value(value):
    """Normalize SQLite and pyodbc values so equivalent rows compare equally."""
    value = clean_value(value)
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.hex()
    return str(value)


def source_rows(source_conn, source_table: str, mapping: Dict[str, str]):
    source_columns = ", ".join(identifier_sqlite(column) for column in mapping)
    query = f"SELECT {source_columns} FROM {identifier_sqlite(source_table)}"
    try:
        cursor = source_conn.execute(query)
    except sqlite3.OperationalError:
        logging.exception("Source table %s could not be read; skipping.", source_table)
        return
    for row in cursor:
        values = dict(zip(mapping.values(), (clean_value(value) for value in row)))
        if values.get("employeeId"):
            yield values


def identifier_sqlite(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_columns(cursor, table_name: str) -> List[str]:
    logging.debug("Reading target schema: dbo.%s", table_name)
    cursor.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION",
        table_name,
    )
    columns = [row[0] for row in cursor.fetchall()]
    if columns:
        logging.info("Target table dbo.%s: %d columns (%s)", table_name, len(columns), ", ".join(columns))
    else:
        logging.warning("Target table dbo.%s was not found or has no columns", table_name)
    return columns


def required_column_defaults(cursor, table_name: str, target_columns: Sequence[str]) -> Dict[str, object]:
    """Return safe values for required destination columns without defaults."""
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?",
        table_name,
    )
    target_names = {column.lower(): column for column in target_columns}
    defaults = {}
    for column_name, data_type, is_nullable, column_default in cursor.fetchall():
        if column_name.lower() == "id" or is_nullable == "YES" or column_default is not None:
            continue
        if data_type in {"bit", "tinyint", "smallint", "int", "bigint", "decimal", "numeric", "float"}:
            defaults[target_names[column_name.lower()]] = 0
        else:
            defaults[target_names[column_name.lower()]] = ""
    return defaults


def prepare_id_generation(cursor, table_name: str, target_columns: Sequence[str]) -> Tuple[bool, int]:
    """Prepare explicit IDs when dbo.<table>.id is not an IDENTITY column."""
    id_column = next((column for column in target_columns if column.lower() == "id"), None)
    if not id_column:
        return True, 0

    cursor.execute(
        "SELECT COLUMNPROPERTY(OBJECT_ID(?), ?, 'IsIdentity')",
        f"dbo.{table_name}", id_column,
    )
    identity_flag = cursor.fetchone()[0]
    if identity_flag == 1:
        logging.info("dbo.%s: id is IDENTITY; SQL Server will generate IDs", table_name)
        return True, 0

    cursor.execute(
        f"SELECT ISNULL(MAX({identifier(id_column)}), 0) FROM {identifier(table_name)}"
    )
    max_id = cursor.fetchone()[0]
    next_id = int(max_id or 0) + 1
    cursor.execute(
        f";WITH numbered AS ("
        f"SELECT {identifier(id_column)}, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS row_num "
        f"FROM {identifier(table_name)} WHERE {identifier(id_column)} IS NULL) "
        f"UPDATE numbered SET {identifier(id_column)} = ? + row_num - 1",
        next_id,
    )
    filled = cursor.rowcount
    if filled and filled > 0:
        logging.info("dbo.%s: filled %d existing NULL IDs", table_name, filled)
        next_id += filled
    logging.info("dbo.%s: explicit ID generation enabled; next ID=%d", table_name, next_id)
    return False, next_id


def insert_rows(cursor, table_name: str, rows: Iterable[dict], target_columns: Sequence[str],
                batch_size=1000, required_defaults: Dict[str, object] = None,
                identity_id: bool = True, next_id: int = 0):
    rows = list(rows)
    if not rows:
        return 0

    # Match compare.py: insert populated mapped values and fill required fields.
    if required_defaults is None:
        required_defaults = required_column_defaults(cursor, table_name, target_columns)
    columns = [
        column for column in target_columns
        if (column.lower() != "id" or not identity_id)
        and any(row.get(column) is not None for row in rows)
    ]
    if not identity_id and "id" not in [column.lower() for column in columns]:
        columns.insert(0, next(column for column in target_columns if column.lower() == "id"))
    for column in required_defaults:
        if column not in columns:
            columns.append(column)
    if not columns:
        return 0
    query = (
        f"INSERT INTO {identifier(table_name)} "
        f"({', '.join(identifier(column) for column in columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})"
    )
    batch = []
    inserted = 0
    logging.debug("Preparing inserts for dbo.%s using %d columns", table_name, len(columns))
    for row in rows:
        values = []
        for column in columns:
            if column.lower() == "id" and not identity_id:
                values.append(next_id)
                next_id += 1
            else:
                values.append(row.get(column) if row.get(column) is not None else required_defaults.get(column))
        batch.append(tuple(values))
        if len(batch) >= batch_size:
            logging.info("dbo.%s: inserting batch of %d rows (total so far: %d)", table_name, len(batch), inserted)
            cursor.fast_executemany = True
            cursor.executemany(query, batch)
            inserted += len(batch)
            batch.clear()
    if batch:
        logging.info("dbo.%s: inserting final batch of %d rows (total so far: %d)", table_name, len(batch), inserted)
        cursor.fast_executemany = True
        cursor.executemany(query, batch)
        inserted += len(batch)
    return inserted


def load_source_rows(source_conn, source_table: str, mapping: Dict[str, str]) -> List[dict]:
    rows = list(source_rows(source_conn, source_table, mapping))
    logging.info("%s: loaded %d source rows", source_table, len(rows))
    return rows


def overwrite_table(cursor, source_rows_list: Sequence[dict], table_name: str,
                    target_columns: Sequence[str]) -> int:
    """Replace one historical table with the current source contents."""
    logging.info("dbo.%s: deleting existing rows", table_name)
    cursor.execute(f"DELETE FROM {identifier(table_name)}")
    logging.info("dbo.%s: deleted %d rows", table_name, cursor.rowcount)
    required_defaults = required_column_defaults(cursor, table_name, target_columns)
    # Auto-fill isdeleted and user_edit with 0 for historical tables
    available = {column.lower(): column for column in target_columns}
    if "isdeleted" in available:
        required_defaults[available["isdeleted"]] = 0
    if "user_edit" in available:
        required_defaults[available["user_edit"]] = 0
    identity_id, next_id = prepare_id_generation(cursor, table_name, target_columns)
    if identity_id:
        id_column = next((column for column in target_columns if column.lower() == "id"), None)
        if id_column:
            try:
                cursor.execute(
                    f"DBCC CHECKIDENT ('dbo.{table_name}', RESEED, 0)"
                )
                logging.info("dbo.%s: identity reset to 0; first inserted ID will be 1", table_name)
            except Exception as error:
                logging.warning("dbo.%s: could not reset identity: %s", table_name, error)
    inserted = insert_rows(
        cursor, table_name, source_rows_list, target_columns,
        required_defaults=required_defaults,
        identity_id=identity_id,
        next_id=next_id,
    )
    logging.info("dbo.%s: inserted %d rows", table_name, inserted)
    return inserted


def sync_historical_table_worker(job):
    """Process one independent historical table in its own database transaction."""
    table_name, data_all_db, connection_string = job
    source_table, mapping = TABLE_MAPPINGS[table_name]
    source_conn = sqlite3.connect(data_all_db)
    target_conn = None
    try:
        import pyodbc

        target_conn = pyodbc.connect(connection_string, autocommit=False, timeout=30)
        target_cursor = target_conn.cursor()
        target_columns = table_columns(target_cursor, table_name)
        if not target_columns:
            target_conn.rollback()
            return table_name, {"status": "missing"}

        source_rows_list = load_source_rows(source_conn, source_table, mapping)
        inserted = overwrite_table(target_cursor, source_rows_list, table_name, target_columns)
        target_conn.commit()
        return table_name, {
            "source": len(source_rows_list),
            "inserted": inserted,
        }
    except Exception:
        if target_conn is not None:
            target_conn.rollback()
        raise
    finally:
        source_conn.close()
        if target_conn is not None:
            target_conn.close()


def sync_employees(cursor, source_conn, target_columns):
    mapping = TABLE_MAPPINGS["employees"][1]
    available = {column.lower(): column for column in target_columns}
    mapped_columns = [available[column.lower()] for column in mapping.values() if column.lower() in available]
    missing_columns = [column for column in mapping.values() if column.lower() not in available]
    logging.info("employees: mapped %d columns; missing optional columns: %s", len(mapped_columns), missing_columns or "none")
    employee_column = available.get("employeeid")
    if not employee_column:
        raise RuntimeError("dbo.employees does not contain employeeId")

    logging.info("Reading existing employees from dbo.employees")
    cursor.execute(f"SELECT * FROM {identifier('employees')}")
    master_columns = [description[0] for description in cursor.description]
    existing_employee_rows = cursor.fetchall()
    master_rows = {
        str(row[master_columns.index(employee_column)]).strip(): dict(zip(master_columns, row))
        for row in existing_employee_rows
        if row[master_columns.index(employee_column)] is not None
    }
    logging.info("Loaded %d existing employee rows from MSSQL", len(master_rows))
    required_defaults = required_column_defaults(cursor, "employees", target_columns)
    identity_id, next_id = prepare_id_generation(cursor, "employees", target_columns)
    inserted = 0
    updated = 0
    source_count = 0
    for new_data in source_rows(source_conn, "FILE_ALL", mapping):
        source_count += 1
        employee_id = str(new_data["employeeId"]).strip()
        if employee_id not in master_rows:
            row = {column: new_data.get(column) for column in mapped_columns}
            if "badgeid" in available:
                row[available["badgeid"]] = employee_id
            inserted += insert_rows(
                cursor, "employees", [row], target_columns,
                required_defaults=required_defaults,
                identity_id=identity_id,
                next_id=next_id,
            )
            if not identity_id:
                next_id += 1
            if inserted % 1000 == 0:
                logging.info("employees: processed %d source rows; inserted %d, existing updated %d", source_count, inserted, updated)
            continue

        # Update all mapped employee fields while preserving generated IDs and badgeId.
        existing = master_rows[employee_id]
        updates = []
        update_values = []
        for column in mapping.values():
            target_column = available.get(column.lower())
            if not target_column or column.lower() in {"id", "badgeid", "employeeid"}:
                continue
            incoming = new_data.get(column)
            if incoming is not None and comparable_value(incoming) != comparable_value(existing.get(target_column)):
                updates.append(f"{identifier(target_column)} = ?")
                update_values.append(incoming)
        if updates:
            update_values.append(employee_id)
            cursor.execute(
                f"UPDATE {identifier('employees')} SET {', '.join(updates)} "
                f"WHERE {identifier(employee_column)} = ?",
                update_values,
            )
            updated += 1

        if source_count % 1000 == 0:
            logging.info("employees: processed %d source rows; inserted %d, existing updated %d", source_count, inserted, updated)
    logging.info("employees: source rows=%d, inserted=%d, existing updated=%d; id and badgeId preserved",
                 source_count, inserted, updated)


def sync_database(args):
    try:
        import pyodbc
    except ImportError as error:
        raise RuntimeError("pyodbc is required. Install it with: python -m pip install pyodbc") from error

    if not os.path.exists(DATA_ALL_DB):
        raise FileNotFoundError(f"Source database not found: {DATA_ALL_DB}")

    logging.info("Source SQLite database: %s (%0.2f MB)", DATA_ALL_DB, os.path.getsize(DATA_ALL_DB) / 1024 / 1024)
    driver = resolve_driver(args.driver)
    logging.info("Destination SQL Server: server=%s, database=%s, driver=%s, authentication=%s",
                 args.server, args.database, driver, "Windows trusted" if args.trusted else "SQL login")
    connection_string = (
        f"DRIVER={{{driver}}};SERVER={args.server};DATABASE={args.database};"
        + ("Trusted_Connection=yes;TrustServerCertificate=yes;" if args.trusted else
           f"UID={args.username};PWD={args.password};TrustServerCertificate=yes;")
    )
    source_conn = None
    target_conn = None
    try:
        logging.info("Opening source SQLite database...")
        source_conn = sqlite3.connect(DATA_ALL_DB)
        logging.info("Source SQLite connection opened")
        logging.info("Opening SQL Server connection...")
        connection_started = time.monotonic()
        target_conn = pyodbc.connect(connection_string, autocommit=False, timeout=30)
        logging.info("SQL Server connection opened in %.2f seconds", time.monotonic() - connection_started)
        target_cursor = target_conn.cursor()
        target_cursor.execute("SELECT DB_NAME(), SUSER_SNAME(), @@VERSION")
        server_database, login_name, server_version = target_cursor.fetchone()
        logging.info("Connected identity: login=%s, database=%s", login_name, server_database)
        logging.debug("SQL Server version: %s", server_version.replace("\r", " ").replace("\n", " "))
        employees_columns = table_columns(target_cursor, "employees")
        if not employees_columns:
            raise RuntimeError("Required target table dbo.employees was not found")
        sync_employees(target_cursor, source_conn, employees_columns)
        logging.info("Committing employee transaction before starting worker processes...")
        target_conn.commit()

        source_conn.close()
        source_conn = None
        target_conn.close()
        target_conn = None

        worker_count = min(args.workers, len(REPLACE_TABLES))
        logging.info("Starting %d worker processes for %d historical tables", worker_count, len(REPLACE_TABLES))
        jobs = [(table_name, DATA_ALL_DB, connection_string) for table_name in REPLACE_TABLES]
        with multiprocessing.Pool(processes=worker_count) as pool:
            for table_name, summary in pool.imap_unordered(
                sync_historical_table_worker, jobs
            ):
                logging.info("Worker completed dbo.%s: %s", table_name, summary)
        logging.info("MSSQL sync completed successfully.")
    except Exception:
        if target_conn is not None:
            logging.error("MSSQL sync failed; rolling back transaction")
            target_conn.rollback()
        logging.exception("MSSQL sync failed; transaction rolled back.")
        raise
    finally:
        if source_conn is not None:
            source_conn.close()
            logging.info("Source SQLite connection closed")
        if target_conn is not None:
            target_conn.close()
            logging.info("SQL Server connection closed")


def main():
    parser = argparse.ArgumentParser(description="Sync data_all.db into a SQL Server database")
    parser.add_argument("--server", default=os.getenv("MSSQL_SERVER", r".\SQLEXPRESS2022"))
    parser.add_argument("--database", default=os.getenv("MSSQL_DATABASE", "Nocportal"), required=False)
    parser.add_argument("--driver", default=os.getenv("MSSQL_DRIVER", DEFAULT_DRIVER))
    parser.add_argument("--trusted", action="store_true", help="Use Windows integrated authentication")
    parser.add_argument("--username", default=os.getenv("MSSQL_USERNAME"))
    parser.add_argument("--password", default=os.getenv("MSSQL_PASSWORD"))
    parser.add_argument("--log-file", default=os.getenv("MSSQL_LOG_FILE", DEFAULT_LOG_FILE))
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"),
                        default=os.getenv("MSSQL_LOG_LEVEL", "INFO").upper())
    parser.add_argument("--workers", type=int,
                        default=max(1, min(multiprocessing.cpu_count(), len(REPLACE_TABLES))),
                        help="Number of processes for independent historical tables")
    args = parser.parse_args()
    if not args.database:
        parser.error("--database is required (or set MSSQL_DATABASE)")
    if not args.trusted and (not args.username or not args.password):
        parser.error("Provide --trusted, or both --username and --password")
    if args.workers < 1:
        parser.error("--workers must be at least 1")
    configure_logging(args.log_level, args.log_file)
    sync_database(args)


if __name__ == "__main__":
    main()