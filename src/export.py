import openpyxl
import csv
import sys
import re
import requests
import os # Needed for path operations and directory creation
import shutil # Needed for efficient file saving from requests stream
import unicodedata # Needed for filename sanitization
from urllib.parse import urlparse # Needed to help extract filename from URL

# --- Helper Function for Sanitizing Filenames ---
def sanitize_filename(filename):
    """Removes potentially invalid characters from a filename."""
    # Normalize unicode characters
    filename = unicodedata.normalize('NFKD', filename).encode('ascii', 'ignore').decode('ascii')
    # Keep alphanumeric, spaces, dots, underscores, hyphens. Replace others.
    filename = re.sub(r'[^\w\s.-]', '_', filename).strip()
    # Replace multiple spaces/underscores with a single underscore
    filename = re.sub(r'[\s_]+', '_', filename)
    # Ensure it's not empty
    if not filename:
        filename = "downloaded_file"
    return filename

# --- Function to Download File ---
def download_file(url: str, download_folder: str, preferred_filename: str, row_number: int) -> bool:
    """
    Downloads a file from a URL to the specified folder.
    Tries to use preferred_filename, falls back to URL parsing or generic name.
    Returns True on success, False on failure.
    """
    sanitized_preferred_name = sanitize_filename(preferred_filename) if preferred_filename else "downloaded_file"
    local_filename = None

    try:
        print(f"Row {row_number}: Attempting to download URL: {url}")
        # Use stream=True to avoid loading large files into memory
        with requests.get(url, stream=True, timeout=30, allow_redirects=True) as r: # Increased timeout
            r.raise_for_status() # Check for HTTP errors (4xx or 5xx)

            # 1. Try Content-Disposition header
            content_disposition = r.headers.get('content-disposition')
            if content_disposition:
                filename_match = re.search(r'filename="?([^"]+)"?', content_disposition)
                if filename_match:
                    local_filename = sanitize_filename(filename_match.group(1))
                    print(f"Row {row_number}:   -> Filename from header: {local_filename}")

            # 2. If no header, try parsing from URL path
            if not local_filename:
                parsed_path = urlparse(url).path
                if parsed_path and parsed_path != '/':
                    potential_name = os.path.basename(parsed_path)
                    if potential_name: # Check if basename extraction yielded something
                         local_filename = sanitize_filename(potential_name)
                         print(f"Row {row_number}:   -> Filename from URL path: {local_filename}")


            # 3. If still no name, use the sanitized preferred_filename (from col A)
            if not local_filename:
                # Add extension if possible (simple check, might need improvement)
                # Often discord/drive links don't have extension in path
                # We might need a more robust way if col A doesn't include it
                if '.' not in sanitized_preferred_name:
                     # Basic check for common archive types often in .zip
                     # This is speculative - might need adjustment based on typical file types
                     if "zip" in r.headers.get("content-type", "").lower():
                         sanitized_preferred_name += ".zip"
                     elif "octet-stream" in r.headers.get("content-type", "").lower():
                          # Could be anything, maybe add .bin? Or leave extensionless?
                          # Let's leave it extensionless for now if unknown.
                          pass


                local_filename = sanitized_preferred_name
                print(f"Row {row_number}:   -> Using preferred filename (from Col A): {local_filename}")


            # Construct full path and ensure directory exists
            full_path = os.path.join(download_folder, local_filename)
            os.makedirs(os.path.dirname(full_path), exist_ok=True) # Ensure directory exists

            # Check for existing file? Overwrite or skip? Let's overwrite for simplicity.
            # If you need to skip:
            # if os.path.exists(full_path):
            #     print(f"Row {row_number}:   -> File already exists: {full_path}. Skipping download.")
            #     return True # Treat existing file as success for TSV row inclusion


            # Save the file using shutil.copyfileobj for efficiency
            print(f"Row {row_number}:   -> Saving to: {full_path}")
            with open(full_path, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            print(f"Row {row_number}:   -> Download successful.")
            return True # Download succeeded

    except requests.exceptions.Timeout:
        print(f"Error downloading file for row {row_number}: Timeout accessing {url}", file=sys.stderr)
        return False
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file for row {row_number} from {url}: {e}", file=sys.stderr)
        return False
    except OSError as e:
        print(f"Error saving file for row {row_number} (Path: {local_filename}): {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"An unexpected error occurred during download for row {row_number} ({url}): {e}", file=sys.stderr)
        return False


# --- Function from discord_url_extractor.py ---
# (get_zip_file_url function remains unchanged - keep it here)
def get_zip_file_url(token: str, discord_url: str) -> str | None:
    # ... (previous implementation) ...
    """
    Fetches the direct URL of an attachment (preferring .zip) from a Discord message URL.
    """
    try:
        # Updated regex to be slightly more robust for potential variations
        match = re.search(r'channels/(\d+|@me)/(\d+)/(\d+)', discord_url)
        if not match:
            print(f"Error: Invalid Discord URL format for '{discord_url}'. Expected 'channels/GUILD_ID/CHANNEL_ID/MESSAGE_ID'.", file=sys.stderr)
            return None

        # group(1) might be guild ID or @me, group(2) is channel ID, group(3) is message ID
        channel_id, message_id = match.group(2), match.group(3)

        headers = {
            # Using 'Bot' prefix assumes this is a bot token.
            # If it's a user token (strongly discouraged for automation),
            # remove 'Bot '. User tokens might violate Discord TOS for bots.
            'Authorization': f'Bot {token}'
        }
        message_api_url = f'https://discord.com/api/v9/channels/{channel_id}/messages/{message_id}'

        response = requests.get(message_api_url, headers=headers, timeout=10) # Added timeout
        response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
        message_data = response.json()

        if message_data.get('attachments'):
            zip_url = None
            first_url = None
            for attachment in message_data['attachments']:
                attachment_url = attachment.get('url')
                if not attachment_url:
                    continue # Skip if URL is missing for some reason

                if first_url is None:
                    first_url = attachment_url # Store the first URL found

                if attachment.get('filename', '').lower().endswith('.zip'):
                    zip_url = attachment_url
                    break # Found a zip file, prioritize it

            if zip_url:
                return zip_url
            elif first_url:
                 # If no zip, return the first attachment URL found.
                print(f"Warning: No .zip attachment found for '{discord_url}'. Returning first attachment found.", file=sys.stderr)
                return first_url
            else:
                 # This case should be rare if 'attachments' list wasn't empty
                 print(f"Error: Attachments list exists but no valid URLs found for '{discord_url}'.", file=sys.stderr)
                 return None
        else:
            print(f"Error: No attachments found in message at '{discord_url}'.", file=sys.stderr)
            return None

    except requests.exceptions.Timeout:
        print(f"Error: Timeout while fetching Discord message for '{discord_url}'.", file=sys.stderr)
        return None
    except requests.exceptions.HTTPError as e:
        print(f"Error: HTTP Error {e.response.status_code} fetching Discord message for '{discord_url}'. Response: {e.response.text}", file=sys.stderr)
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error: Network error fetching message from Discord API for '{discord_url}': {e}", file=sys.stderr)
        return None
    except KeyError as e:
        print(f"Error: Invalid Discord message data format (missing key: {e}) for '{discord_url}'.", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred while processing Discord URL '{discord_url}': {e}", file=sys.stderr)
        return None


# --- Function from original export.py ---
# (create_direct_download_link function remains unchanged - keep it here)
def create_direct_download_link(google_drive_url):
    # ... (previous implementation) ...
    """Creates a direct download link for Google Drive files."""
    # Regex updated to handle different Google Drive URL formats
    file_id_match = re.search(r'(?:/d/|id=)([a-zA-Z0-9_-]{25,})', google_drive_url)
    if file_id_match:
        file_id = file_id_match.group(1)
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    else:
        print(f"Warning: Could not extract file ID from Google Drive URL: {google_drive_url}", file=sys.stderr)
        return None

# --- Modified process_excel_row ---
def process_excel_row(row_number, row, download_folder): # Added download_folder parameter
    """
    Processes a single Excel row, handling URL conversion AND file download.
    Returns the data row list if URL processing AND download are successful, otherwise returns None.
    """
    try:
        col_a_cell = row[0]
        col_b_cell = row[1]
        col_c_cell = row[2]
        col_d_cell = row[3]
        col_e_cell = row[4]
        col_f_cell = row[5]
        col_g_cell = row[6]
        col_i_cell = row[8]
        col_m_cell = row[12]

        # --- Check for hyperlink existence FIRST ---
        if not (col_a_cell and col_a_cell.hyperlink and col_a_cell.hyperlink.target):
            print(f"Skipping row {row_number}: No hyperlink found in column A.")
            return None # Skip row if no hyperlink exists

        url = col_a_cell.hyperlink.target
        original_url = url # Keep for reference

        # Extract display text using .value (assuming data_only=True during load)
        # This will be used as the preferred filename
        col_a_display_text = col_a_cell.value if col_a_cell else None

        # --- Process URL ---
        url_successfully_processed = True # Flag to track success

        if "discord.com/channels" in url:
            print(f"Row {row_number}: Processing Discord URL: {url}")
            url_converted = get_zip_file_url(discord_token, url)
            if url_converted:
                url = url_converted
                print(f"Row {row_number}:   -> Discord URL processed successfully: {url}")
            else:
                print(f"Skipping row {row_number}: Failed to get valid Discord attachment URL from {original_url}.")
                url_successfully_processed = False

        elif "drive.google.com" in url:
            print(f"Row {row_number}: Processing Google Drive URL: {url}")
            url_converted = create_direct_download_link(url)
            if url_converted:
                url = url_converted
                print(f"Row {row_number}:   -> Google Drive URL converted: {url}")
            else:
                print(f"Skipping row {row_number}: Failed to convert Google Drive URL {original_url} to direct link.")
                url_successfully_processed = False

        # --- Check if URL processing failed ---
        if not url_successfully_processed:
             return None # Skip row if Discord/Drive processing failed

        # --- Attempt to Download the File ---
        # We use the display text (col_a) as the preferred filename base
        download_successful = download_file(url, download_folder, col_a_display_text, row_number)

        if not download_successful:
            print(f"Skipping row {row_number}: Download failed for URL {url}.")
            return None # Skip row if download failed

        # --- If we reach here, URL is processed AND download succeeded. Extract other columns. ---
        col_b = col_b_cell.value if col_b_cell else None
        col_c = col_c_cell.value if col_c_cell else None
        col_d = col_d_cell.value if col_d_cell else None
        col_e = col_e_cell.value if col_e_cell else None
        col_f = col_f_cell.value if col_f_cell else None
        col_g = col_g_cell.value if col_g_cell else None
        col_i = col_i_cell.value if col_i_cell else None
        col_m = col_m_cell.value if col_m_cell else None

        # Handle potential None values for string concatenation/formatting
        version = "1.0" if col_d is None else str(col_d)
        creator = str(col_i) if col_i else ""
        if col_m:
            creator += (" - modeler: " + str(col_m)) if creator else ("modeler: " + str(col_m))

        # Return the row data for the TSV
        return [
            col_a_display_text if col_a_display_text is not None else "",
            url if url is not None else "", # Final URL (potentially converted)
            col_b if col_b is not None else "",
            col_c if col_c is not None else "",
            version,
            col_e if col_e is not None else "",
            col_f if col_f is not None else "",
            col_g if col_g is not None else "",
            creator
        ]
    except Exception as e:
        print(f"Error processing Excel row {row_number}: {e}. Skipping row.", file=sys.stderr)
        # import traceback # Optional: Uncomment for detailed traceback
        # traceback.print_exc() # Optional: Uncomment for detailed traceback
        return None # Skip row on unexpected error during processing

# --- Modified xlsx_to_tsv Function ---
def xlsx_to_tsv(input_file, output_file, download_folder): # Added download_folder parameter
    """
    Converts XLSX to TSV, attempts downloads, skipping rows on URL/download failure.
    """
    rows_processed = 0
    rows_written = 0
    rows_skipped_no_link = 0
    rows_skipped_url_fail = 0
    rows_skipped_download_fail = 0
    rows_skipped_other_error = 0
    rows_skipped_empty = 0

    try:
        print(f"Loading workbook '{input_file}'...")
        # --- IMPORTANT: Keep data_only=True for HYPERLINK display text ---
        workbook = openpyxl.load_workbook(input_file, data_only=True)
        sheet = workbook.active
        print(f"Workbook loaded. Processing sheet '{sheet.title}'.")
        print(f"Files will be downloaded to: '{os.path.abspath(download_folder)}'") # Show absolute path

        # --- Create download directory if it doesn't exist ---
        try:
            os.makedirs(download_folder, exist_ok=True)
            print(f"Ensured download directory exists.")
        except OSError as e:
            print(f"Error: Could not create download directory '{download_folder}': {e}", file=sys.stderr)
            sys.exit(1)


        with open(output_file, 'w', newline='', encoding='utf-8') as tsvfile:
            tsv_writer = csv.writer(tsvfile, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
            print(f"Writing TSV index to '{output_file}'...")

            for row_idx, row in enumerate(sheet.iter_rows(min_row=2), start=2):
                rows_processed += 1
                # Store original reason for skipping (though process_excel_row handles details now)
                skip_reason = None

                # Check if the row seems empty before processing
                if all(cell.value is None for cell in row):
                    print(f"Skipping empty Excel row {row_idx}.")
                    rows_skipped_empty += 1
                    continue

                # --- Call process_excel_row (which now includes download) ---
                # Pass the download_folder to the processing function
                data_row = process_excel_row(row_idx, row, download_folder)

                if data_row is not None:
                    # Only write the row if process_excel_row returned data (meaning URL & download OK)
                    tsv_writer.writerow(data_row)
                    rows_written += 1
                else:
                    # process_excel_row already printed the specific reason
                    # For summary stats, we could try and refine this, but it's complex
                    # Let's just increment a general "skipped" counter based on previous logic
                     if not (row[0] and row[0].hyperlink and row[0].hyperlink.target):
                         rows_skipped_no_link +=1
                     # We can't easily differentiate between URL proc fail and download fail here
                     # without more complex return values from process_excel_row.
                     # Let's just lump them for the summary for now.
                     else:
                          rows_skipped_download_fail += 1 # Assuming most failures after link check are download/URL related


        total_skipped = rows_skipped_empty + rows_skipped_no_link + rows_skipped_url_fail + rows_skipped_download_fail + rows_skipped_other_error

        print("-" * 20)
        print(f"Processing Summary:")
        print(f"  Excel Rows Processed: {rows_processed} (excluding header)")
        print(f"  TSV Rows Written:     {rows_written} (to '{output_file}')")
        print(f"  Rows Skipped:         {total_skipped}")
        print(f"    - Empty Excel Row:    {rows_skipped_empty}")
        # Note: These might not be perfectly accurate if an error happens before the specific check
        print(f"    - No Link in Col A:   {rows_skipped_no_link}")
        print(f"    - URL/Download Fail:  {rows_skipped_download_fail}") # Combined count
        # print(f"    - URL Process Fail: {rows_skipped_url_fail}")
        # print(f"    - Download Fail:    {rows_skipped_download_fail}")
        # print(f"    - Other Error:      {rows_skipped_other_error}")
        print("-" * 20)
        print(f"Successfully finished processing '{input_file}'.")
        print(f"Index saved to '{output_file}'. Downloads attempted in '{download_folder}'.")


    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"An critical error occurred during conversion: {e}")
        # import traceback
        # traceback.print_exc()
        sys.exit(1)

# --- Modified Main execution block ---
def main():
    # Expecting: script.py <input.xlsx> <output.tsv> <download_folder>
    if len(sys.argv) != 4:
        script_name = os.path.basename(__file__) if __file__ else "export_and_download.py"
        print(f"Usage: python {script_name} <input.xlsx> <output.tsv> <download_folder>")
        sys.exit(1)
    else:
        input_file = sys.argv[1]
        output_file = sys.argv[2]
        download_folder = sys.argv[3] # Get download folder from command line

        # Optional: Basic check if input file exists here
        if not os.path.isfile(input_file):
             print(f"Error: Input file '{input_file}' does not exist or is not a file.", file=sys.stderr)
             sys.exit(1)

        # Pass the download folder to the main processing function
        xlsx_to_tsv(input_file, output_file, download_folder)

if __name__ == "__main__":
    # Ensure all necessary imports are present at the top
    main()