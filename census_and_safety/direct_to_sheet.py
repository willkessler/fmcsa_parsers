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
SPREADSHEET_ID = '1BhkkTNlR_eZwZ_uypc-Exd-E0lP0luCjhCIpShdm4yI'

# File setup
CENSUS_FILE = 'FMCSA_CENSUS1_2024Jun.txt'
SAFETY_FILE_AB = 'SMS_AB_PassProperty_2024Jun.txt'
SAFETY_FILE_C = 'SMS_C_PassProperty_2024Jun.txt'
README_FILE = 'CENSUS_README.txt'
SAFETY_README_FILE = 'SAFETY_README.txt'
ROWS_PER_SHEET = 10000
MAX_COLUMN_WIDTH = 250
EXCLUDE_FILE = 'exclude_columns.txt'
CITIES_FILE = 'cities.txt'
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
            # If sheet already exists, get its ID
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

def read_exclude_columns(filename):
    with open(filename, 'r') as file:
        return [line.strip() for line in file if line.strip()]

def read_cities(filename):
    cities = {}
    with open(filename, 'r') as file:
        for line in file:
            city, state = line.strip().rsplit(',', 1)
            cities[city.strip().lower()] = state.strip().lower()
    return cities

def read_column_descriptions(filename):
    descriptions = {}
    with open(filename, 'r') as file:
        for line in file:
            line = line.strip()
            if line and '-' in line:
                parts = line.split('-', 1)
                if len(parts) == 2:
                    column_name = parts[0].strip()
                    description = parts[1].strip()
                    descriptions[column_name] = description
    return descriptions

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

        if header in column_descriptions:
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
                            "note": column_descriptions[header]
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

def read_safety_data(filename):
    safety_data = {}
    encoding = detect_encoding(filename)
    with open(filename, 'r', encoding=encoding, errors='replace') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            dot_number = row['DOT_NUMBER']
            del row['DOT_NUMBER']  # Remove DOT_NUMBER from the data we're storing
            safety_data[dot_number] = row
    return safety_data

def merge_safety_data(safety_data_ab, safety_data_c):
    merged_data = {}
    for dot_number in set(safety_data_ab.keys()) | set(safety_data_c.keys()):
        merged_data[dot_number] = {**safety_data_ab.get(dot_number, {}), **safety_data_c.get(dot_number, {})}
    return merged_data

def process_csv(census_file, safety_file_ab, safety_file_c, service, spreadsheet_id):
    exclude_columns = read_exclude_columns(EXCLUDE_FILE)
    cities = read_cities(CITIES_FILE)
    column_descriptions_census = read_column_descriptions(README_FILE)
    column_descriptions_safety = read_column_descriptions(SAFETY_README_FILE)
    print("Reading safety data...")
    # Check for conflicts
    conflicts = set(column_descriptions_census.keys()) & set(column_descriptions_safety.keys())
    if conflicts:
        print(f"Warning: The following columns appear in both README files: {conflicts}")
        print("The descriptions from the safety README will be used for these columns.")

    # Merge the dictionaries
    column_descriptions = {**column_descriptions_census, **column_descriptions_safety}

    safety_data_ab = read_safety_data(safety_file_ab)
    safety_data_c = read_safety_data(safety_file_c)
    safety_data = merge_safety_data(safety_data_ab, safety_data_c)
    print("Safety data loaded.")

    sheet_counter = 1
    row_counter = 0
    current_sheet_data = []
    error_count = 0
    skipped_count = 0
    processed_count = 0
    included_count = 0

    encoding = detect_encoding(census_file)
    print(f"Detected encoding for census file: {encoding}")

    with open(census_file, 'r', encoding=encoding, errors='replace') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)

        include_indices = [i for i, header in enumerate(headers) if header not in exclude_columns]
        filtered_headers = [headers[i] for i in include_indices]

        # Add safety data headers
        if safety_data:
            safety_headers = list(next(iter(safety_data.values())).keys())
            filtered_headers.extend(safety_headers)

        current_sheet_data.append(filtered_headers)
        num_columns = len(filtered_headers)

        dot_number_index = headers.index('DOT_NUMBER')
        phy_city_index = headers.index('PHY_CITY')
        phy_state_index = headers.index('PHY_STATE')
        email_index = headers.index('EMAIL_ADDRESS')

        total_rows = sum(1 for _ in reader)
        csvfile.seek(0)
        next(reader)  # Skip header again

        for line_num, row in enumerate(reader, start=2):
            try:
                if line_num % 1000 == 0:
                    print(f"Processed {line_num} out of {total_rows} rows ({(line_num/total_rows)*100:.2f}%)")

                city = row[phy_city_index].strip().lower()
                state = row[phy_state_index].strip().lower()

                if city not in cities or cities[city] != state:
                    skipped_count += 1
                    continue

                if not row[email_index].strip():
                    skipped_count += 1
                    continue

                filtered_row = [row[i] for i in include_indices]

                # Add safety data
                dot_number = row[dot_number_index]
                if dot_number in safety_data:
                    filtered_row.extend(safety_data[dot_number].values())
                else:
                    filtered_row.extend([''] * len(safety_headers))

                current_sheet_data.append(filtered_row)
                row_counter += 1
                processed_count += 1
                included_count += 1

                if row_counter == ROWS_PER_SHEET:
                    sheet_name = f'Merged_Data_{sheet_counter}'
                    sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, ROWS_PER_SHEET + 1, num_columns)
                    write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data)
                    format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, filtered_headers)
                    print(f"Created and populated sheet: {sheet_name}")
                    sheet_counter += 1
                    row_counter = 0
                    current_sheet_data = [filtered_headers]
                    time.sleep(2)  # Add a delay between sheets

            except Exception as e:
                error_count += 1
                print(f"Error processing line {line_num}: {str(e)}")
                if error_count % 100 == 0:
                    print(f"Encountered {error_count} errors. Last error: {str(e)}")
                continue

    # Write any remaining data
    if row_counter > 0:
        sheet_name = f'Merged_Data_{sheet_counter}'
        sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, row_counter + 1, num_columns)
        write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data)
        format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, filtered_headers)
        print(f"Created and populated sheet: {sheet_name}")

    print(f"Processing complete. {sheet_counter} sheet(s) created in the Google Spreadsheet.")
    print(f"Total rows in input file: {total_rows}")
    print(f"Total rows processed: {processed_count}")
    print(f"Total rows included: {included_count}")
    print(f"Total rows skipped: {skipped_count}")
    print(f"Total errors encountered: {error_count}")

if __name__ == "__main__":
    service = get_google_sheets_service()
    process_csv(CENSUS_FILE, SAFETY_FILE_AB, SAFETY_FILE_C, service, SPREADSHEET_ID)
