import csv
import os
import time
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = './client_secret.json'
TOKEN_FILE = 'token.json'
SPREADSHEET_ID = '1sew8Kc7ecmiVlPtn44XTqFjlxJQaoDtbfqTZakt-hnQ'

# File setup
BOC3_FILE = 'boc3_allwithhistory_and_header.txt'
ROWS_PER_SHEET = 50000
MAX_COLUMN_WIDTH = 250
BATCH_SIZE = 1000
MAX_RETRIES = 5

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

def format_sheet(service, spreadsheet_id, sheet_id, num_columns):
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

    for i in range(num_columns):
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

def process_boc3_csv(boc3_file, service, spreadsheet_id):
    sheet_counter = 1
    row_counter = 0
    error_count = 0
    processed_count = 0
    ignored_count = 0

    with open(boc3_file, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
        company_name_index = headers.index('COMPANY_NAME')
        num_columns = len(headers)

        print("Reading and sorting data...")
        all_data = sorted(
            (row for row in reader if row[company_name_index].strip()),
            key=lambda x: x[company_name_index]
        )
        total_rows = len(all_data)
        print(f"Total rows after filtering: {total_rows}")

        current_sheet_data = [headers]

        for line_num, row in enumerate(all_data, start=1):
            try:
                if line_num % 1000 == 0:
                    print(f"Processed {line_num} out of {total_rows} rows ({(line_num/total_rows)*100:.2f}%)")

                current_sheet_data.append(row)
                row_counter += 1
                processed_count += 1

                if row_counter == ROWS_PER_SHEET:
                    sheet_name = f'BOC3_Data_{sheet_counter}'
                    sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, ROWS_PER_SHEET + 1, num_columns)
                    write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data)
                    format_sheet(service, spreadsheet_id, sheet_id, num_columns)
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
        sheet_name = f'BOC3_Data_{sheet_counter}'
        sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, len(current_sheet_data), num_columns)
        write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data)
        format_sheet(service, spreadsheet_id, sheet_id, num_columns)
        print(f"Created and populated sheet: {sheet_name}")

    ignored_count = sum(1 for row in csv.reader(open(boc3_file, 'r', newline='', encoding='utf-8')) if not row[company_name_index].strip()) - 1  # Subtract 1 to account for header

    print(f"Processing complete. {sheet_counter} sheet(s) created in the Google Spreadsheet.")
    print(f"Total rows in input file: {total_rows + ignored_count}")
    print(f"Rows ignored (empty company name): {ignored_count}")
    print(f"Total rows processed: {processed_count}")
    print(f"Total errors encountered: {error_count}")

if __name__ == "__main__":
    service = get_google_sheets_service()
    process_boc3_csv(BOC3_FILE, service, SPREADSHEET_ID)
