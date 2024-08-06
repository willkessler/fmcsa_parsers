import csv
import os
import time
import json
import chardet
from datetime import datetime
from collections import defaultdict
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = './client_secret.json'
TOKEN_FILE = 'token.json'
SPREADSHEET_ID = '1qxUu126efpWKG1ilyatStEkoxt27CB5laGWj7uJ1w-M'

# File setup
INSPECTIONS_FILE = 'raw_data/2024Jun_Inspection.txt'
README_FILE = 'raw_data/Inspection_Readme.txt'
CENSUS_FILE = '../census_and_safety/raw_data/FMCSA_CENSUS1_2024Jun.txt'
ROWS_PER_SHEET = 10000
MAX_COLUMN_WIDTH = 250
BATCH_SIZE = 1000
MAX_RETRIES = 15
REPORTING_STATE = 'TX'
TAB_PREFIX='Enriched_Inspections_Data'
MAX_CELL_CHARS = 49000  # Setting a bit below 50000 to be safe

COLUMNS_TO_COMBINE = [
    "TIME_WEIGHT", "DRIVER_OOS_TOTAL", "VEHICLE_OOS_TOTAL", "TOTAL_HAZMAT_SENT",
    "OOS_TOTAL", "HAZMAT_OOS_TOTAL", "HAZMAT_PLACARD_REQ", "UNIT_TYPE_DESC",
    "UNIT_MAKE", "UNIT_LICENSE", "UNIT_LICENSE_STATE", "VIN", "UNIT_DECAL_NUMBER",
    "UNIT_TYPE_DESC2", "UNIT_MAKE2", "UNIT_LICENSE2", "UNIT_LICENSE_STATE2", "VIN2",
    "UNIT_DECAL_NUMBER2", "UNSAFE_INSP", "FATIGUED_INSP", "DR_FITNESS_INSP",
    "SUBT_ALCOHOL_INSP", "VH_MAINT_INSP", "HM_INSP", "BASIC_VIOL", "UNSAFE_VIOL",
    "FATIGUED_VIOL", "DR_FITNESS_VIOL", "SUBT_ALCOHOL_VIOL", "VH_MAINT_VIOL", "HM_VIOL"
]

PROGRESS_FILE = 'inspections_progress.json'

def split_string(s, max_length):
    if len(s) <= max_length:
        return s, ""
    return s[:max_length], s[max_length:]

def safe_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0

def parse_date(date_string):
    try:
        return datetime.strptime(date_string, '%d-%b-%y')
    except ValueError:
        try:
            return datetime.strptime(date_string, '%m/%d/%Y')
        except ValueError:
            print(f"Unable to parse date: {date_string}")
            return None

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read(10000)  # Read first 10000 bytes
    return chardet.detect(raw_data)['encoding']

def save_progress(processed_count, sheet_counter, row_counter):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump({
            'processed_count': processed_count,
            'sheet_counter': sheet_counter,
            'row_counter': row_counter
        }, f)
    print(f"Progress saved: processed_count={processed_count}, sheet_counter={sheet_counter}, row_counter={row_counter}")

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                data = f.read().strip()
                if data:
                    progress = json.loads(data)
                    print(f"Loaded progress: {progress}")
                    return progress
                else:
                    print("Progress file is empty. Starting from the beginning.")
        except json.JSONDecodeError:
            print("Error reading progress file. Starting from the beginning.")
        except Exception as e:
            print(f"Unexpected error reading progress file: {str(e)}. Starting from the beginning.")
    else:
        print("No progress file found. Starting from the beginning.")
    return None

def get_google_sheets_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'w') as token:
            token.write(creds.to_json())
    return build('sheets', 'v4', credentials=creds)

def create_new_sheet(service, spreadsheet_id, sheet_name, num_rows, num_columns):
    body = {
        'requests': [{
            'addSheet': {
                'properties': {
                    'title': sheet_name,
                    'gridProperties': {
                        'rowCount': num_rows,
                        'columnCount': num_columns
                    }
                }
            }
        }]
    }
    try:
        response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
        return response['replies'][0]['addSheet']['properties']['sheetId']
    except HttpError as error:
        if 'already exists' in str(error):
            sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            for sheet in sheet_metadata.get('sheets', ''):
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']
        else:
            raise

def write_to_sheet_batch(service, spreadsheet_id, sheet_name, values, 
                         sheet_id, num_columns, column_descriptions, new_headers):

    print("write_to_sheet_batch running on sheet_id {sheet_id}")
    formatting_applied = False
    for i in range(0, len(values), BATCH_SIZE):
        batch = values[i:i+BATCH_SIZE]
        range_name = f"{sheet_name}!A{i+1}"
        body = {
            'values': batch
        }

        # Check each cell for character limit
        for row_index, row in enumerate(batch):
            for col_index, cell in enumerate(row):
                if isinstance(cell, str) and len(cell) > MAX_CELL_CHARS:
                    print(f"WARNING: Cell content exceeds {MAX_CELL_CHARS} characters at row {i + row_index + 1}, column {col_index + 1}")
                    print(f"Cell content (truncated): {cell[:100]}...")
                    print(f"Cell length: {len(cell)}")
                    # Truncate the cell content
                    batch[row_index][col_index] = cell[:MAX_CELL_CHARS]

        for attempt in range(MAX_RETRIES):
            try:
                response = service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id, range=range_name,
                    valueInputOption='RAW', body=body).execute()
                print(f"Successfully wrote batch of {len(batch)} rows")
                if not formatting_applied:
                    print(f"Formatting sheet {sheet_id}")
                    format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, new_headers)
                    print(f"Done formatting sheet {sheet_id}")
                    formatting_applied = True
                break
            except HttpError as error:
                print(f"HTTP Error during batch write (attempt {attempt + 1}): {error}")
                error_details = json.loads(error.content.decode('utf-8'))
                print(f"Error details: {error_details}")
                if 'Your input contains more than the maximum of 50000 characters in a single cell' in str(error):
                    for row_index, row in enumerate(batch):
                        for col_index, cell in enumerate(row):
                            if isinstance(cell, str) and len(cell) > MAX_CELL_CHARS:
                                print(f"Problematic cell at row {i + row_index + 1}, column {col_index + 1}")
                                print(f"Cell content (truncated): {cell[:100]}...")
                                print(f"Cell length: {len(cell)}")
                if attempt == MAX_RETRIES - 1:
                    print("Max retries reached. Exiting.")
                    sys.exit(1)
                time.sleep(2 ** attempt)  # Exponential backoff
            except Exception as e:
                print(f"Unexpected error during batch write: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    print("Max retries reached. Exiting.")
                    sys.exit(1)
                time.sleep(5)
        time.sleep(1)  # Short delay between batches

def read_column_descriptions(filename, encoding):
    descriptions = {}
    with open(filename, 'r', encoding=encoding) as file:
        for line in file:
            line = line.strip()
            if line and '-' in line:
                parts = line.split('-', 1)
                if len(parts) == 2:
                    column_name = parts[0].strip().lower().replace(' ', '_')
                    description = parts[1].strip()
                    descriptions[column_name] = description
    return descriptions

def read_census_data(census_file):
    encoding = detect_encoding(census_file)
    print(f"Detected encoding for census file: {encoding}")
    
    census_data = {}
    with open(census_file, 'r', newline='', encoding=encoding, errors='replace') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dot_number = row['DOT_NUMBER']
            census_data[dot_number] = {
                'LEGAL_NAME': row['LEGAL_NAME'],
                'TELEPHONE': row['TELEPHONE'],
                'EMAIL_ADDRESS': row['EMAIL_ADDRESS']
            }
    return census_data


def format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, headers):
    requests = [
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": ROWS_PER_SHEET + 1,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_columns
                    }
                }
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "frozenRowCount": 1
                    }
                },
                "fields": "gridProperties.frozenRowCount"
            }
        },

        # For some reason, auto-resize dimensions takes a very long time, maybe because the additional info
        # column has so much data in it. So we wont' autosize those columns
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": num_columns - 2
                }
            }
        },

        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 1,  # Start from the second row (index 1)
                    "endIndex": ROWS_PER_SHEET + 1  # +1 to include the header row
                },
                "properties": {
                    "pixelSize": 36  # Adjust this value to set the desired row height
                },
                "fields": "pixelSize"
            }
        }

    ]

    for i, header in enumerate(headers):
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": i,
                    "endIndex": i + 1
                },
                "properties": {
                    "pixelSize": MAX_COLUMN_WIDTH
                },
                "fields": "pixelSize"
            }
        })

        # Put comments on the header cells explaining the meaning of each column.
        normalized_header = header.lower().replace(' ', '_')
        if normalized_header in column_descriptions:
            requests.append({
                "updateCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1,
                        "startColumnIndex": i,
                        "endColumnIndex": i + 1
                    },
                    "rows": [{
                        "values": [{
                            "note": column_descriptions[normalized_header]
                        }]
                    }],
                    "fields": "note"
                }
            })

    body = {
        'requests': requests
    }
    for attempt in range(MAX_RETRIES):
        try:
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            break
        except HttpError as error:
            if attempt == MAX_RETRIES - 1:
                raise
            time.sleep(2 ** attempt)  # Exponential backoff
        except TimeoutError:
            if attempt == MAX_RETRIES - 1:
                raise
            print(f"Timed out during formatting. Retrying attempt {attempt}")
            time.sleep(5)  # Wait 5 seconds before retrying on timeout

def process_csv(inspections_file, census_file, service, spreadsheet_id):
    progress = load_progress()
    sheet_counter = progress['sheet_counter'] if progress else 1
    row_counter = progress['row_counter'] if progress else 0
    start_from = progress['processed_count'] if progress else 0
    error_count = 0
    processed_count = start_from

    print(f"Starting process: sheet_counter={sheet_counter}, row_counter={row_counter}, start_from={start_from}")

    encoding = detect_encoding(inspections_file)
    print(f"Detected encoding for inspections file: {encoding}")

    column_descriptions = read_column_descriptions(README_FILE, encoding)
    census_data = read_census_data(census_file)
    sheet_ids = []

    with open(inspections_file, 'r', newline='', encoding=encoding, errors='replace') as csvfile:
        reader = csv.DictReader(csvfile)
        headers = reader.fieldnames
        
        # Prepare new headers
        new_headers = [
            'DOT_NUMBER', 'LEGAL_NAME', 'TELEPHONE', 'EMAIL_ADDRESS',
            'REPORT_STATE', 'TOTAL_VIOLATIONS', 'ADDITIONAL_INFO', 'ADDITIONAL_INFO_CONTINUED'
        ]
        
        num_columns = len(new_headers)

        print("Reading and processing data...")
        company_inspections = defaultdict(lambda: defaultdict(list))

        for row in reader:
            processed_count += 1
            
            if processed_count <= start_from:
                continue

            insp_date = parse_date(row['INSP_DATE'])

            # Limiting by date and reporting state:
#            if not insp_date or insp_date.year < 2023 or row['REPORT_STATE'] != REPORTING_STATE:
#                continue

            # Limiting only to ones with inspections on at least one date and in the reporting state:
            if not insp_date or row['REPORT_STATE'] != REPORTING_STATE:
                continue

            # Limiting only to those with inspection dates:
            # if not insp_date:
            #    continue


            dot_number = row['DOT_NUMBER']
            company_info = census_data.get(dot_number, {'LEGAL_NAME': '', 'TELEPHONE': '', 'EMAIL_ADDRESS': ''})
            if not company_info['EMAIL_ADDRESS']:
                # print(f"Skipping company {company_info['LEGAL_NAME']}, tel: {company_info['TELEPHONE']}, since no email address -->{company_info['EMAIL_ADDRESS']}<--")
                continue

            company_inspections[dot_number]['dates'].append(insp_date.strftime('%d-%b-%y'))
            company_inspections[dot_number]['REPORT_STATE'].append(row['REPORT_STATE'])
            
            for field in COLUMNS_TO_COMBINE + ['BASIC_VIOL', 'UNSAFE_VIOL', 'FATIGUED_VIOL', 
                                               'DR_FITNESS_VIOL', 'SUBT_ALCOHOL_VIOL', 'VH_MAINT_VIOL', 'HM_VIOL']:
                if field in row:
                    company_inspections[dot_number][field].append(row[field])

            if processed_count % 10000 == 0:
                print(f"Processed {processed_count} rows")
                save_progress(processed_count, sheet_counter, row_counter)

        print(f"Finished reading data. Total companies: {len(company_inspections)}")
        print("Consolidating company data...")
        current_sheet_data = [new_headers]

        for dot_number, inspections in company_inspections.items():
            company_info = census_data.get(dot_number, {'LEGAL_NAME': '', 'TELEPHONE': '', 'EMAIL_ADDRESS': ''})
            
            violations_total = sum(safe_int(v) for v in (
                inspections.get('BASIC_VIOL', []) +
                inspections.get('UNSAFE_VIOL', []) +
                inspections.get('FATIGUED_VIOL', []) +
                inspections.get('DR_FITNESS_VIOL', []) +
                inspections.get('SUBT_ALCOHOL_VIOL', []) +
                inspections.get('VH_MAINT_VIOL', []) +
                inspections.get('HM_VIOL', [])
            ))

            additional_info = [f"INSPECTION DATES: {','.join(sorted(set(inspections['dates'])))}"]
            for field in COLUMNS_TO_COMBINE:
                if field in inspections:
                    values = ','.join(set(filter(None, inspections[field])))  # Filter out empty strings
                    if values:  # Only add non-empty fields
                        additional_info.append(f"{field}: {values}")

            additional_info_str = '\n'.join(additional_info)
            additional_info_main, additional_info_continued = split_string(additional_info_str, MAX_CELL_CHARS)

            new_row = [
                dot_number,
                company_info['LEGAL_NAME'],
                company_info['TELEPHONE'],
                company_info['EMAIL_ADDRESS'],
                inspections['REPORT_STATE'][0] if inspections.get('REPORT_STATE') else '',
                str(violations_total),
                additional_info_main,
                additional_info_continued
            ]

            # Final check for cell character limit
            for i, cell in enumerate(new_row):
                if isinstance(cell, str) and len(cell) > MAX_CELL_CHARS:
                    print(f"WARNING: Cell content exceeds {MAX_CELL_CHARS} characters for DOT_NUMBER {dot_number}, column {i}")
                    print(f"Cell content (truncated): {cell[:100]}...")
                    print(f"Cell length: {len(cell)}")
                    new_row[i] = cell[:MAX_CELL_CHARS]

            current_sheet_data.append(new_row)
            row_counter += 1

            if row_counter >= ROWS_PER_SHEET:
                print(f"Preparing to write sheet {sheet_counter} with {row_counter} rows")
                try:
                    sheet_name = f'{TAB_PREFIX}_{sheet_counter}'
                    sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, ROWS_PER_SHEET + 1, num_columns)
                    print("Writing batch of data to google sheet.")
                    write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data, 
                                         sheet_id, num_columns,column_descriptions, new_headers)
                    print(f"Created and populated sheet: {sheet_name}")
                    sheet_ids.append(sheet_id)
                    sheet_counter += 1
                    row_counter = 0
                    current_sheet_data = [new_headers]
                    save_progress(processed_count, sheet_counter, row_counter)
                    time.sleep(2)  # Add a delay between sheets
                except Exception as e:
                    error_count += 1
                    print(f"Error creating/writing sheet: {str(e)}")
                    save_progress(processed_count, sheet_counter, row_counter)
                    time.sleep(60)  # Wait for 1 minute before retrying
                    continue

    # Write any remaining data
    if len(current_sheet_data) > 1:
        print(f"Writing final sheet with {len(current_sheet_data)} rows")
        try:
            sheet_name = f'{TAB_PREFIX}_{sheet_counter}'
            sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, len(current_sheet_data), num_columns)
            write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data, 
                                 sheet_id, num_columns,column_descriptions, new_headers)
            print(f"Created and populated final sheet: {sheet_name}")
        except Exception as e:
            error_count += 1
            print(f"Error creating/writing final sheet: {str(e)}")

#    for sheet_id in sheet_ids:
#        print(f"Formatting columns for sheet id: {sheet_id}")
#        format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, new_headers)

    print(f"Processing complete. {sheet_counter} sheet(s) created in the Google Spreadsheet.")
    print(f"Total rows processed: {processed_count}")
    print(f"Total rows written: {row_counter + (sheet_counter - 1) * ROWS_PER_SHEET}")
    print(f"Total errors encountered: {error_count}")
    
    # Clear progress file after successful completion
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        print("Progress file removed after successful completion.")

if __name__ == "__main__":
    service = get_google_sheets_service()
    process_csv(INSPECTIONS_FILE, CENSUS_FILE, service, SPREADSHEET_ID)
