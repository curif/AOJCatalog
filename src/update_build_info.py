# update_build_info.py
import sqlite3
import sys
import argparse
import datetime

def update_build_info(db_path, tag, commit_sha, timestamp):
    """
    Inserts or replaces the build information into the BuildInfo table.

    Args:
        db_path (str): Path to the SQLite database file.
        tag (str): The Git tag for the build.
        commit_sha (str): The Git commit SHA for the build.
        timestamp (str): The UTC timestamp string for the build.

    Returns:
        bool: True on success, False on failure.
    """
    print(f"Updating build info in database: {db_path}")
    print(f"  Tag: {tag}")
    print(f"  Commit SHA: {commit_sha}")
    print(f"  Timestamp: {timestamp}")

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Use INSERT OR REPLACE to ensure only one row exists, updated each time.
        sql = """
            INSERT OR REPLACE INTO BuildInfo (BuildTag, BuildCommitSHA, BuildTimestampUTC)
            VALUES (?, ?, ?);
        """
        cursor.execute(sql, (tag, commit_sha, timestamp))
        conn.commit()
        print("Successfully updated BuildInfo table.")
        return True

    except sqlite3.Error as e:
        print(f"SQLite error updating BuildInfo table: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        print(f"Unexpected error updating BuildInfo table: {e}", file=sys.stderr)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()
            print("Database connection closed after updating build info.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update the BuildInfo table in the marketplace SQLite database."
    )
    parser.add_argument("--db-path", required=True, help="Path to the SQLite database file.")
    parser.add_argument("--tag", required=True, help="Git tag for the build.")
    parser.add_argument("--commit-sha", required=True, help="Git commit SHA for the build.")
    parser.add_argument("--timestamp", required=True, help="UTC timestamp for the build (ISO 8601 format).")

    args = parser.parse_args()

    if update_build_info(args.db_path, args.tag, args.commit_sha, args.timestamp):
        print("\nBuild info update script finished successfully.")
    else:
        print("\nBuild info update script finished with errors.", file=sys.stderr)
        sys.exit(1)