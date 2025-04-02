# load_cabinets.py
import sqlite3
import os
import sys
import csv
import requests # For downloading URLs
import io       # For handling downloaded text as a file
import argparse

# --- Constants for CSV Column Indices (0-based) ---
# Match these to your expected CSV structure (based on C# example)
IDX_NAME = 0
IDX_URL_CABINET = 1 # Renamed to avoid conflict with catalog URL
IDX_GAME = 2
IDX_CREATION_DATE = 3
IDX_VERSION = 4
IDX_ROM_NAME = 5
IDX_DESCRIPTION = 6
IDX_CORE = 7
IDX_CREATOR = 8
IDX_NOTES = 9

# Minimum columns expected in a valid data row (Name, Url, Game are required by DB schema)
MIN_REQUIRED_COLUMNS = max(IDX_NAME, IDX_URL_CABINET, IDX_GAME) + 1

def _get_value_or_none(row, index):
    """Safely gets a value from a list (CSV row) by index, returning None if index is out of bounds."""
    try:
        # Return None if the value is empty string, otherwise return the value
        value = row[index].strip()
        return value if value else None
    except IndexError:
        return None

def load_cabinets_from_catalogs(db_path):
    """
    Reads catalogs from the database, downloads associated CSV files,
    parses them, and loads data into the Cabinet table.

    Args:
        db_path (str): Path to the SQLite database file.

    Returns:
        bool: True if the overall process completed (though individual errors may have occurred),
              False if a critical error prevented processing (e.g., DB connection).
    """
    print(f"Starting cabinet loading process for database: {db_path}")

    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.", file=sys.stderr)
        print("Please run the database creation script first.", file=sys.stderr)
        return False

    conn = None
    overall_success = True
    catalogs_processed = 0
    catalogs_failed_download = 0

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("Database connection established.")

        # Enable Foreign Keys
        cursor.execute("PRAGMA foreign_keys = ON;")

        # --- 1. Read the Catalog table ---
        cursor.execute("SELECT CatalogName, url FROM Catalog")
        catalogs_to_process = cursor.fetchall()

        if not catalogs_to_process:
            print("No catalogs found in the Catalog table to process.")
            return True # Not an error, just nothing to do

        print(f"Found {len(catalogs_to_process)} catalogs to process.")

        # --- 2. Loop through each catalog ---
        for catalog_name, catalog_url in catalogs_to_process:
            catalogs_processed += 1
            print(f"\n--- Processing Catalog: '{catalog_name}' ---")
            print(f"  URL: {catalog_url}")

            if not catalog_url or not catalog_url.strip():
                print(f"  Warning: Skipping catalog '{catalog_name}' due to missing or empty URL.", file=sys.stderr)
                continue

            # --- 3. Download the CSV file ---
            csv_content = None
            try:
                response = requests.get(catalog_url, timeout=30) # 30 second timeout
                response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
                csv_content = response.text
                print(f"  Successfully downloaded content from URL.")
            except requests.exceptions.RequestException as e:
                print(f"  Error downloading URL for catalog '{catalog_name}': {e}", file=sys.stderr)
                catalogs_failed_download += 1
                continue # Skip to the next catalog
            except Exception as e:
                print(f"  An unexpected error occurred during download for catalog '{catalog_name}': {e}", file=sys.stderr)
                catalogs_failed_download += 1
                continue # Skip to the next catalog

            if not csv_content:
                print(f"  Warning: Downloaded content for '{catalog_name}' is empty. Skipping processing.", file=sys.stderr)
                continue

            # --- 4. Process the CSV data ---
            rows_processed = 0
            rows_inserted = 0
            rows_failed = 0
            processed_keys_in_catalog = set() # Track (CatalogName, Name) pairs within this specific CSV

            try:
                # Use io.StringIO to treat the string content like a file
                # Use csv.reader to handle CSV parsing complexities (quoting, commas in fields)
                # Assuming standard comma delimiter. Adjust delimiter=',' if needed (e.g., for TSV use '\t')
                csvfile = io.StringIO(csv_content)
                reader = csv.reader(csvfile, delimiter=',') # Specify delimiter if not comma

                sql_insert = """
                    INSERT INTO Cabinet (
                        CatalogName, Name, Game, CreationDate, Version, RomName,
                        Url, Description, Core, Creator, Notes
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """

                for i, row in enumerate(reader):
                    rows_processed += 1
                    row_number = i + 1 # 1-based index for logging

                    # Basic row validation (minimum columns)
                    if len(row) < MIN_REQUIRED_COLUMNS:
                        print(f"  Skipping row {row_number}: has only {len(row)} columns, expected at least {MIN_REQUIRED_COLUMNS}.", file=sys.stderr)
                        rows_failed += 1
                        continue

                    # Extract data using indices
                    name = _get_value_or_none(row, IDX_NAME)
                    url_cabinet = _get_value_or_none(row, IDX_URL_CABINET) # Cabinet specific URL
                    game = _get_value_or_none(row, IDX_GAME)

                    # --- Validation based on DB Schema (NOT NULL fields) ---
                    if not name:
                        print(f"  Skipping row {row_number}: Required field 'Name' (column {IDX_NAME + 1}) is missing or empty.", file=sys.stderr)
                        rows_failed += 1
                        continue
                    if not game:
                        print(f"  Skipping row {row_number}: Required field 'Game' (column {IDX_GAME + 1}) is missing or empty.", file=sys.stderr)
                        rows_failed += 1
                        continue
                    if not url_cabinet:
                         print(f"  Skipping row {row_number}: Required field 'Url' (column {IDX_URL_CABINET + 1}) is missing or empty.", file=sys.stderr)
                         rows_failed += 1
                         continue

                    # Check for duplicates within this specific catalog load
                    cabinet_key = (catalog_name, name)
                    if cabinet_key in processed_keys_in_catalog:
                        print(f"  Skipping row {row_number}: Duplicate Name '{name}' found within this CSV for catalog '{catalog_name}'.", file=sys.stderr)
                        rows_failed += 1
                        continue
                    processed_keys_in_catalog.add(cabinet_key)


                    # Extract optional fields
                    creation_date = _get_value_or_none(row, IDX_CREATION_DATE)
                    version = _get_value_or_none(row, IDX_VERSION)
                    rom_name = _get_value_or_none(row, IDX_ROM_NAME)
                    description = _get_value_or_none(row, IDX_DESCRIPTION)
                    core = _get_value_or_none(row, IDX_CORE)
                    creator = _get_value_or_none(row, IDX_CREATOR)
                    notes = _get_value_or_none(row, IDX_NOTES)

                    # Prepare data tuple for insertion (order must match SQL)
                    data_tuple = (
                        catalog_name, name, game, creation_date, version, rom_name,
                        url_cabinet, description, core, creator, notes
                    )

                    # Insert into database
                    try:
                        cursor.execute(sql_insert, data_tuple)
                        rows_inserted += 1
                    except sqlite3.IntegrityError as e:
                        # Should primarily catch PK violations if the processed_keys_in_catalog check fails,
                        # or FK violations if catalog_name somehow doesn't exist (unlikely here).
                        print(f"  DB Integrity Error on row {row_number} for Name '{name}': {e}. Skipping row.", file=sys.stderr)
                        rows_failed += 1
                        # No rollback needed here, let the transaction continue for other rows
                    except sqlite3.Error as e:
                        print(f"  DB Error on row {row_number} for Name '{name}': {e}. Skipping row.", file=sys.stderr)
                        rows_failed += 1
                        # No rollback needed here

                # --- Commit after processing all rows for the current catalog's CSV ---
                conn.commit()
                print(f"  Finished processing '{catalog_name}'. Rows processed: {rows_processed}, Inserted: {rows_inserted}, Failed/Skipped: {rows_failed}")

            except csv.Error as e:
                print(f"  Error parsing CSV data for catalog '{catalog_name}' from URL {catalog_url}: {e}", file=sys.stderr)
                conn.rollback() # Rollback any partial inserts from this file if parsing fails mid-way
                rows_failed = rows_processed # Assume all rows failed if parsing failed
                overall_success = False # Mark overall process potentially incomplete
            except Exception as e:
                print(f"  An unexpected error occurred processing CSV data for '{catalog_name}': {e}", file=sys.stderr)
                conn.rollback()
                rows_failed = rows_processed
                overall_success = False

        # --- End of catalog loop ---
        print("\n--- Cabinet Loading Summary ---")
        print(f"Catalogs found in DB: {len(catalogs_to_process)}")
        print(f"Catalogs processed attempt: {catalogs_processed}")
        print(f"Catalogs failed download: {catalogs_failed_download}")
        print("-----------------------------")


    except sqlite3.Error as e:
        print(f"A critical SQLite error occurred: {e}", file=sys.stderr)
        if conn:
            conn.rollback() # Rollback any pending transaction
        overall_success = False
    except Exception as e:
        print(f"An unexpected critical error occurred: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        overall_success = False
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

    return overall_success

# --- Main execution block ---
if __name__ == "__main__":
    # --- Set up Argument Parser ---
    parser = argparse.ArgumentParser(
        description="Load cabinet data into the marketplace database by downloading and processing CSV files specified in the Catalog table."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to the target SQLite database file (e.g., /path/to/data/marketplace.db)."
    )

    # --- Parse Arguments ---
    args = parser.parse_args()

    # --- Use Parsed Arguments ---
    database_file_path = args.db_path

    print(f"Using Database Path for Cabinet Loading: {database_file_path}")

    # Call the main loading function
    if load_cabinets_from_catalogs(database_file_path):
        print("\nCabinet loading script finished successfully.")
    else:
        print("\nCabinet loading script finished with critical errors or CSV processing issues.", file=sys.stderr)
        sys.exit(1) # Exit with error code if loading failed