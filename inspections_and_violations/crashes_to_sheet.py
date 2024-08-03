import csv
import os
import time
import chardet
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = './client_secret.json'
TOKEN_FILE = 'token.json'
SPREADSHEET_ID = '1SkNMQ0czHEAkf7qNXtlo1xcjSUiX3nmeDEGkDQb9shU'

# File setup
CRASHES_FILE = 'raw_data/2024Jun_Crash.txt'
README_FILE = 'raw_data/Crash_Readme.txt'
CENSUS_FILE = '../census_and_safety/raw_data/FMCSA_CENSUS1_2024Jun.txt'
ROWS_PER_SHEET = 25000
MAX_COLUMN_WIDTH = 250
BATCH_SIZE = 1000
MAX_RETRIES = 5

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        raw_data = file.read(10000)  # Read first 10000 bytes
    return chardet.detect(raw_data)['encoding']

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

def write_to_sheet_batch(service, spreadsheet_id, sheet_name, values):
    for i in range(0, len(values), BATCH_SIZE):
        batch = values[i:i+BATCH_SIZE]
        range_name = f"{sheet_name}!A{i+1}"
        body = {
            'values': batch
        }
        for attempt in range(MAX_RETRIES):
            try:
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id, range=range_name,
                    valueInputOption='RAW', body=body).execute()
                break
            except HttpError as error:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
            except TimeoutError:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(5)  # Wait 5 seconds before retrying on timeout
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
        {
            "autoResizeDimensions": {
                "dimensions": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 0,
                    "endIndex": num_columns
                }
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
            time.sleep(5)  # Wait 5 seconds before retrying on timeout


def process_csv(crashes_file, census_file, service, spreadsheet_id):
    sheet_counter = 1
    row_counter = 0
    error_count = 0
    processed_count = 0

    encoding = detect_encoding(crashes_file)
    print(f"Detected encoding for crashes file: {encoding}")

    column_descriptions = read_column_descriptions(README_FILE, encoding)
    census_data = read_census_data(census_file)

    with open(crashes_file, 'r', newline='', encoding=encoding, errors='replace') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
        dot_number_index = headers.index('DOT_NUMBER')
        
        # Insert new columns after DOT_NUMBER
        headers.insert(dot_number_index + 1, 'LEGAL_NAME')
        headers.insert(dot_number_index + 2, 'TELEPHONE')
        headers.insert(dot_number_index + 3, 'EMAIL_ADDRESS')
        
        num_columns = len(headers)

        print("Reading and sorting data...")
        all_data = []
        for row in reader:
            if row[dot_number_index].strip():
                dot_number = row[dot_number_index]
                company_info = census_data.get(dot_number, {'LEGAL_NAME': '', 'TELEPHONE': '', 'EMAIL_ADDRESS': ''})
                row.insert(dot_number_index + 1, company_info['LEGAL_NAME'])
                row.insert(dot_number_index + 2, company_info['TELEPHONE'])
                row.insert(dot_number_index + 3, company_info['EMAIL_ADDRESS'])
                all_data.append(row)
        
        all_data.sort(key=lambda x: x[dot_number_index])
        total_rows = len(all_data)
        print(f"Total rows after filtering and enrichment: {total_rows}")

        current_sheet_data = [headers]

        for line_num, row in enumerate(all_data, start=1):
            try:
                if line_num % 1000 == 0:
                    print(f"Processed {line_num} out of {total_rows} rows ({(line_num/total_rows)*100:.2f}%)")

                current_sheet_data.append(row)
                row_counter += 1
                processed_count += 1

                if row_counter == ROWS_PER_SHEET:
                    sheet_name = f'Enriched_Crashes_Data_{sheet_counter}'
                    sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, ROWS_PER_SHEET + 1, num_columns)
                    write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data)
                    format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, headers)
                    print(f"Created and populated sheet: {sheet_name}")
                    sheet_counter += 1
                    row_counter = 0
                    current_sheet_data = [headers]
                    time.sleep(2)  # Add a delay between sheets

            except Exception as e:
                error_count += 1
                print(f"Error processing line {line_num}: {str(e)}")
                if error_count % 100 == 0:
                    print(f"Encountered {error_count} errors. Last error: {str(e)}")
                continue

    # Write any remaining data
    if len(current_sheet_data) > 1:
        sheet_name = f'Enriched_Crashes_Data_{sheet_counter}'
        sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, len(current_sheet_data), num_columns)
        write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data)
        format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, headers)
        print(f"Created and populated sheet: {sheet_name}")

    print(f"Processing complete. {sheet_counter} sheet(s) created in the Google Spreadsheet.")
    print(f"Total rows processed: {processed_count}")
    print(f"Total errors encountered: {error_count}")

if __name__ == "__main__":
    service = get_google_sheets_service()
    process_csv(CRASHES_FILE, CENSUS_FILE, service, SPREADSHEET_ID)
