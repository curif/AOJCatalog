# update_build_info.py
import sqlite3
import sys
import argparse
import datetime
import re # Import regular expression module for more robust parsing

def extract_major_version(tag_string):
    """
    Extracts the major version component from a Git tag string.
    Handles formats like 'v1.2.3', '1.2.3', 'v1.2', '1.2', 'v1', '1'.

    Args:
        tag_string (str): The full Git tag string.

    Returns:
        str: The extracted major version string, or the original tag
             if parsing fails or doesn't match expected patterns.
    """
    if not tag_string:
        return "" # Return empty if input is empty

    # Remove potential leading 'v'
    cleaned_tag = tag_string.lstrip('v')

    # Split by '.' and take the first part
    parts = cleaned_tag.split('.')
    if parts:
        major_version = parts[0]
        # Basic check: ensure the first part isn't empty
        if major_version:
            print(f"    Extracted major version: '{major_version}' from tag '{tag_string}'")
            return major_version
        else:
             # Handle cases like tag being just '.' or 'v.' resulting in empty first part
             print(f"    Warning: Could not extract major version from tag '{tag_string}' after splitting. Using original tag.", file=sys.stderr)
             return tag_string # Fallback to original tag
    else:
        # Should not happen if split is called on non-empty string, but defensive check
        print(f"    Warning: Could not extract major version from tag '{tag_string}'. Using original tag.", file=sys.stderr)
        return tag_string # Fallback to original tag

def update_build_info(db_path, full_tag, commit_sha, timestamp):
    """
    Extracts major version from the tag and inserts or replaces the build
    information into the BuildInfo table.

    Args:
        db_path (str): Path to the SQLite database file.
        full_tag (str): The full Git tag for the build (e.g., v1.2.3).
        commit_sha (str): The Git commit SHA for the build.
        timestamp (str): The UTC timestamp string for the build.

    Returns:
        bool: True on success, False on failure.
    """
    # --- Extract Major Version ---
    major_version_tag = extract_major_version(full_tag)
    # --- End Extraction ---

    print(f"Updating build info in database: {db_path}")
    print(f"  Full Tag: {full_tag}")
    print(f"  Storing as BuildTag: {major_version_tag}") # Log what's being stored
    print(f"  Commit SHA: {commit_sha}")
    print(f"  Timestamp: {timestamp}")

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Use INSERT OR REPLACE. Note we now use major_version_tag for the first value.
        sql = """
            INSERT OR REPLACE INTO BuildInfo (BuildTag, BuildCommitSHA, BuildTimestampUTC)
            VALUES (?, ?, ?);
        """
        # --- Use the extracted major version here ---
        cursor.execute(sql, (major_version_tag, commit_sha, timestamp))
        # --- End Change ---

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
        description="Update the BuildInfo table in the marketplace SQLite database with the MAJOR version extracted from the tag."
    )
    parser.add_argument("--db-path", required=True, help="Path to the SQLite database file.")
    # Keep argument name --tag, but clarify help text
    parser.add_argument("--tag", required=True, help="Full Git tag for the build (e.g., v1.2.3). Major version will be extracted.")
    parser.add_argument("--commit-sha", required=True, help="Git commit SHA for the build.")
    parser.add_argument("--timestamp", required=True, help="UTC timestamp for the build (ISO 8601 format).")

    args = parser.parse_args()

    # Pass the full tag from args.tag to the function
    if update_build_info(args.db_path, args.tag, args.commit_sha, args.timestamp):
        print("\nBuild info update script finished successfully.")
    else:
        print("\nBuild info update script finished with errors.", file=sys.stderr)
        sys.exit(1)