# load_yaml.py
import sqlite3
import os
import sys
import yaml
import argparse 

def load_catalogs_from_yaml(db_path, yaml_path):
    """
    Loads catalog data from a YAML file into the Catalog table, assuming
    the table should be cleared before loading. Replaces any existing data.

    Args:
        db_path (str): The path to the SQLite database file.
        yaml_path (str): The path to the YAML file containing catalog data.

    Returns:
        bool: True if loading was successful, False otherwise.
    """
    print(f"\nAttempting to load catalogs from YAML: {yaml_path} into DB: {db_path}")
    print("NOTE: Existing data in the Catalog table will be deleted before loading.")

    # --- Basic Check: Ensure database file exists ---
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found.", file=sys.stderr)
        print("Please run the database creation script first.", file=sys.stderr)
        return False

    # --- Load data from YAML file ---
    catalogs_list = None
    try:
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
            catalogs_list = data.get('marketplace')
            if catalogs_list is None:
                 print(f"Error: YAML file '{yaml_path}' is missing the top-level 'marketplace' key.", file=sys.stderr)
                 return False
            if not isinstance(catalogs_list, list):
                print(f"Error: 'marketplace' key in YAML file '{yaml_path}' does not contain a list.", file=sys.stderr)
                return False
            print(f"Successfully loaded {len(catalogs_list)} catalog entries from YAML file.")

    except FileNotFoundError:
        print(f"Error: YAML file not found at '{yaml_path}'", file=sys.stderr)
        return False
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file '{yaml_path}': {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"An unexpected error occurred reading YAML file '{yaml_path}': {e}", file=sys.stderr)
        return False

    # --- Insert data into database ---
    conn = None
    inserted_count = 0
    processed_yaml_entries = 0 # Track how many YAML entries we attempt to process
    has_errors = False
    processed_keys = set() # Keep track of CatalogNames processed within this run to detect duplicates in YAML

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("Database connection established for loading catalogs.")

        # Enable foreign keys for this connection
        cursor.execute("PRAGMA foreign_keys = ON;")

        # --- Clear existing data from Catalog table ---
        print("Deleting existing data from Catalog table...")
        cursor.execute("DELETE FROM Catalog;")
        print(f"-> Deleted {cursor.rowcount} rows from Catalog.")

        # --- Insert new data ---
        sql = "INSERT INTO Catalog (CatalogName, Description, url) VALUES (?, ?, ?)"

        for item in catalogs_list:
            processed_yaml_entries += 1
            catalog_name = item.get('CatalogName')
            description = item.get('Description')
            url = item.get('Url') # Will be None if 'Url' key is missing

            if not catalog_name:
                print(f"Warning: Skipping entry {processed_yaml_entries} due to missing 'CatalogName': {item}", file=sys.stderr)
                continue # Skip this entry if primary key is missing

            # Check for duplicate CatalogName within this YAML load operation
            if catalog_name in processed_keys:
                print(f"Warning: Duplicate 'CatalogName' found in YAML: '{catalog_name}'. Skipping subsequent entry {processed_yaml_entries}.", file=sys.stderr)
                continue
            processed_keys.add(catalog_name)

            try:
                cursor.execute(sql, (catalog_name, description, url))
                inserted_count += 1

            # Catch constraint errors (like PRIMARY KEY violation, although the check above should prevent it)
            # or operational errors (like table missing).
            except sqlite3.IntegrityError as ie:
                 print(f"Error inserting catalog '{catalog_name}': {ie}. This might indicate a duplicate key despite checks.", file=sys.stderr)
                 has_errors = True
                 break # Stop processing on integrity errors
            except sqlite3.OperationalError as oe:
                 print(f"Error executing insert for '{catalog_name}': {oe}", file=sys.stderr)
                 print("Does the 'Catalog' table exist? Ensure create_db.py was run correctly.", file=sys.stderr)
                 has_errors = True
                 break # Stop processing if table is missing or other operational issue
            except sqlite3.Error as e:
                print(f"Error inserting catalog '{catalog_name}': {e}", file=sys.stderr)
                has_errors = True # Mark that an error occurred
                # Decide if you want to continue processing other items or stop

        if not has_errors:
            conn.commit()
            print(f"Catalog loading transaction committed. Inserted: {inserted_count} rows.")
            return True
        else:
            print("Rolling back changes due to errors during loading.", file=sys.stderr)
            conn.rollback()
            return False

    except sqlite3.Error as e:
        print(f"An SQLite error occurred during catalog loading connection/setup: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        print(f"An unexpected error occurred during catalog loading: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
            print("Database connection closed after loading catalogs.")


# --- Main execution block ---
if __name__ == "__main__":
    # --- Set up Argument Parser ---
    parser = argparse.ArgumentParser(
        description="Load catalog data from a YAML file into the marketplace SQLite database. WARNING: This script DELETES existing data in the Catalog table before loading."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to the target SQLite database file (e.g., /path/to/data/marketplace.db)."
    )
    parser.add_argument(
        "--yaml-path",
        required=True,
        help="Path to the input YAML file containing catalog data (e.g., /path/to/config/catalogs.yaml)."
    )

    # --- Parse Arguments ---
    args = parser.parse_args()

    # --- Use Parsed Arguments ---
    database_file_path = args.db_path
    yaml_file_path = args.yaml_path

    print(f"Using Database Path: {database_file_path}")
    print(f"Using YAML Path: {yaml_file_path}")

    # Call the function to load data from YAML into the Catalog table
    if load_catalogs_from_yaml(database_file_path, yaml_file_path):
        print("\nYAML loading script finished successfully.")
    else:
        print("\nYAML loading script finished with errors.", file=sys.stderr)
        sys.exit(1) # Exit with error code if loading failed