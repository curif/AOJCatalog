# create_db.py
import sqlite3
import os
import sys
import argparse # <--- Import argparse

def initialize_database(db_path):
    """
    Initializes the SQLite database with Catalog and Cabinet tables,
    enables foreign keys, and creates indexes using plain sqlite3 calls.

    Args:
        db_path (str): The full path to the database file.

    Returns:
        bool: True if initialization was successful or DB already exists correctly, False otherwise.
    """
    print(f"Initializing database structure at: {db_path}")
    initialized_successfully = False # Flag to track success

    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        try:
            os.makedirs(db_dir, exist_ok=True)
            print(f"Created directory: {db_dir}")
        except OSError as e:
            print(f"Error creating directory {db_dir}: {e}", file=sys.stderr)
            return False # Stop if directory creation fails

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        print("Database connection established.")

        # --- Enable Foreign Key Support ---
        cursor.execute("PRAGMA foreign_keys = ON;")
        print("-> Executed: PRAGMA foreign_keys = ON;")

        # --- Create Catalog Table ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Catalog (
                CatalogName TEXT PRIMARY KEY NOT NULL,
                Description TEXT,
                url TEXT
            );
        """)
        print("-> Executed: CREATE TABLE IF NOT EXISTS Catalog")

        # --- Create Cabinet Table ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS Cabinet (
                CatalogName TEXT NOT NULL,
                Name TEXT NOT NULL,
                Game TEXT NOT NULL,
                CreationDate TEXT,
                Version TEXT,
                RomName TEXT,
                Url TEXT NOT NULL,
                Description TEXT,
                Core TEXT,
                Creator TEXT,
                Notes TEXT,
                PRIMARY KEY (CatalogName, Name),
                FOREIGN KEY (CatalogName) REFERENCES Catalog(CatalogName) ON DELETE CASCADE
            );
        """)
        print("-> Executed: CREATE TABLE IF NOT EXISTS Cabinet")

        # --- Create Indexes for Performance ---
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cabinet_name ON Cabinet (Name);")
        print("-> Executed: CREATE INDEX IF NOT EXISTS idx_cabinet_name")

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_cabinet_creator ON Cabinet (Creator);")
        print("-> Executed: CREATE INDEX IF NOT EXISTS idx_cabinet_creator")

        # --- Commit the changes ---
        conn.commit()
        print("Database structural changes committed.")
        print("Database structure initialized/verified successfully.")
        initialized_successfully = True

    except sqlite3.Error as e:
        print(f"An SQLite error occurred during initialization: {e}", file=sys.stderr)
        if conn:
             conn.rollback() # Rollback DDL changes if possible/needed
    except Exception as e:
        print(f"An unexpected error occurred during initialization: {e}", file=sys.stderr)
    finally:
        if conn:
            conn.close()
            print("Database connection closed after initialization.")
    return initialized_successfully

# --- Main execution block ---
if __name__ == "__main__":
    # --- Set up Argument Parser ---
    parser = argparse.ArgumentParser(
        description="Initialize the marketplace SQLite database schema (creates tables and indexes if they don't exist)."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path where the SQLite database file should be created or verified (e.g., /path/to/data/marketplace.db)."
    )

    # --- Parse Arguments ---
    args = parser.parse_args()

    # --- Use Parsed Arguments ---
    database_file_path = args.db_path

    print(f"Target Database Path for Schema Initialization: {database_file_path}")

    # Call the function to initialize the database structure
    if initialize_database(database_file_path):
         print("\nDatabase creation script finished successfully.")
    else:
        print("\nDatabase creation script finished with errors.", file=sys.stderr)
        sys.exit(1) # Exit with error code if initialization failed