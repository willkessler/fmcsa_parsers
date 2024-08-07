import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from urllib.parse import urlparse, unquote, quote_plus, urlencode
from tqdm import tqdm

import requests
import random
import html2text
import re
from bs4 import BeautifulSoup
import sys
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
SPREADSHEET_ID = '1yLk7AjKdy_b2uOZZiDY2T2Ke6567HdNhbwecKCKAEtI'

# File setup
REVOCATIONS_FILE = 'raw_data/revocation_2024_08_06.txt'
README_FILE = 'raw_data/Revocations_Readme.txt'
CENSUS_FILE = '../census_and_safety/raw_data/FMCSA_CENSUS1_2024Jun.txt'
ROWS_PER_SHEET = 500
MAX_COLUMN_WIDTH = 250
BATCH_SIZE = 1000
MAX_RETRIES = 15
TAB_PREFIX='Enriched_Revocations_Data'
MAX_CELL_CHARS = 49000  # Setting a bit below 50000 to be safe

CITIES_FILE = 'cities.txt'
PROGRESS_FILE = 'revocations_progress.json'

def read_cities(filename):
    cities = {}
    with open(filename, 'r') as file:
        for line in file:
            city, state = line.strip().rsplit(',', 1)
            cities[normalize_city_name(city)] = state.strip().lower()
    return cities

def normalize_city_name(city):
    return city.lower().strip()

def extract_city_state(address):
    # Split the address into lines
    lines = address.strip().split('\n')
    
    # If we have at least two lines, process the second line
    if len(lines) >= 2:
        city_state_zip = lines[1].strip().split(',')
        if len(city_state_zip) == 2:
            city = city_state_zip[0].strip()
            state_zip = city_state_zip[1].strip().split()
            if len(state_zip) >= 1:
                state = state_zip[0]
            else:
                state = ''
        else:
            city, state = '', ''
    else:
        city, state = '', ''
    
    return city.lower(), state.lower()

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

def process_text(text, is_address=False):
    # Convert HTML to plain text
    soup = BeautifulSoup(text, 'html.parser')
    if is_address:
        # Replace <br> with ', ' for addresses
        for br in soup.find_all("br"):
            br.replace_with(", ")
    plain_text = soup.get_text()
    # Remove extra whitespace
    plain_text = re.sub(r'\s+', ' ', plain_text).strip()
    print(f"plain_text={plain_text}")
    return plain_text

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

    print(f"write_to_sheet_batch running on sheet_id {sheet_id}.")
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
                    valueInputOption='USER_ENTERED', body=body).execute()
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
    print(f"Reading column descriptions for file {filename}.")
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

import requests
from bs4 import BeautifulSoup
import time

def extract_company_data(usdot, max_retries=3):
    url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={usdot}"
    
    extracted_data = {}
    fields_to_extract = [
        ('Legal Name', 'Legal Name:'),
        ('DBA Name', 'DBA Name:'),
        ('Phone', 'Phone:'),
        ('Physical Address', 'Physical Address:')
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception for bad status codes
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            for field_name, field_label in fields_to_extract:
                try:
                    value = soup.find('th', string=field_label).find_next_sibling('td').text.strip()
                    extracted_data[field_name] = value
                    if field_name == 'Legal Name':
                        print(f"        Legal Name: {value}   ( {usdot} )")
                except AttributeError:
                    # print(f"  Error extracting {field_name}: Field not found")
                    extracted_data[field_name] = "N/A"
            
            # If we've successfully extracted all data, break the retry loop
            break  # Remove the condition and always break after a successful extraction
        except requests.RequestException as e:
            print(f"  Error during extraction attempt {retry + 1}: {e}")
            if retry == max_retries - 1:
                print(f"  Failed to extract data for USDOT {usdot} after {max_retries} attempts")
                for field_name, _ in fields_to_extract:
                    if field_name not in extracted_data:
                        extracted_data[field_name] = "N/A"
            else:
                print(f"  Retrying in 5 seconds...")
                time.sleep(5)

    return extracted_data

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

def process_csv(revocations_file, service, spreadsheet_id):
    progress = load_progress()
    sheet_counter = progress['sheet_counter'] if progress else 1
    row_counter = progress['row_counter'] if progress else 0
    start_from = progress['processed_count'] if progress else 0
    error_count = 0
    processed_count = start_from

    print(f"Starting process: sheet_counter={sheet_counter}, row_counter={row_counter}, start_from={start_from}")

    encoding = detect_encoding(revocations_file)
    print(f"Detected encoding for revocations file: {encoding}")

    column_descriptions = read_column_descriptions(README_FILE, encoding)
    cities = read_cities(CITIES_FILE)

    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    # Set up WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

    print("Reading and processing data...")
    company_revocations = defaultdict(list)

    with open(revocations_file, 'r', newline='', encoding=encoding, errors='replace') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            processed_count += 1
            
            if processed_count <= start_from:
                continue

            dot_number = row['DOT_NUMBER']
            company_revocations[dot_number].append(row)
            
            if processed_count % 10000 == 0:
                print(f"Processed {processed_count} rows")
                save_progress(processed_count, sheet_counter, row_counter)

    print(f"Finished reading data. Total companies: {len(company_revocations)}")
    print("Consolidating company data and scraping additional info...")

    # Prepare new headers
    new_headers = [
        'DOT_NUMBER', 'LEGAL_NAME', 'DBA_NAME', 'PHONE', 'PHYSICAL_ADDRESS',
        'CITY', 'STATE',
        'OPERATING_AUTHORITY_REGISTRATION_TYPES', 'SERVE_DATES', 'REVOCATION_TYPES', 'EFFECTIVE_DATES', 'DOCKET_NUMBERS'
    ]
    num_columns = len(new_headers)

    extraction_counter = {}  # New counter
    total_companies = len(company_revocations)
    processed_companies = 0

    filtered_companies = {}

    # Create a progress bar
    with tqdm(total=total_companies, desc="Filtering companies", unit="company") as pbar:
        for dot_number, revocations in company_revocations.items():
            # print(f"\nFetching data for USDOT {dot_number}")
            
            # Increment the counter for this DOT number
            extraction_counter[dot_number] = extraction_counter.get(dot_number, 0) + 1
            
            company_data = extract_company_data(dot_number)
            if company_data.get('Legal Name') == 'N/A':
                # print(f"      *** Skipping not-located usdot : {dot_number}")
                continue

            # Extract city and state from physical address
            address = company_data.get('Physical Address', '')
            city, state = extract_city_state(address)
            normalized_city = normalize_city_name(city)

            # print(f"Extracted city: {city}, state: {state}")
            # print(f"Normalized city: {normalized_city}")

            # Check if the company is in one of the specified cities
            # if normalized_city not in cities or cities[normalized_city] != state:
            #     print(f"Skipping company not in specified cities: {city}, {state}")

            filtered_companies[dot_number] = (company_data, revocations)
            pbar.update(1)

    print(f"Filtered companies: {len(filtered_companies)}")
    print("Processing filtered companies and writing to sheets...")

    current_sheet_data = [new_headers]

    with tqdm(total=len(filtered_companies), desc="Processing filtered companies", unit="company") as pbar:
        for dot_number, (company_data, revocations) in filtered_companies.items():
            # Print the current count for this DOT number
            print(f"Extraction count for USDOT {dot_number}: {extraction_counter[dot_number]}")

            # Consolidate revocation data
            operating_authority_types = set()
            serve_dates = set()
            revocation_types = set()
            effective_dates = set()
            docket_numbers = set()

            for revocation in revocations:
                if revocation['OPERATING_AUTHORITY_REGISTRATION_TYPE']:
                    operating_authority_types.add(revocation['OPERATING_AUTHORITY_REGISTRATION_TYPE'])
                if revocation['SERVE_DATE']:
                    serve_dates.add(revocation['SERVE_DATE'])
                if revocation['REVOCATION_TYPE']:
                    revocation_types.add(revocation['REVOCATION_TYPE'])
                if revocation['EFFECTIVE_DATE']:
                    effective_dates.add(revocation['EFFECTIVE_DATE'])
                if revocation['DOCKET_NUMBER']:
                    docket_numbers.add(revocation['DOCKET_NUMBER'])

            dot_url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=USDOT&query_string={dot_number}"

            new_row = [
                f'=HYPERLINK("{dot_url}", "{dot_number}")',
                company_data.get('Legal Name', ''),
                company_data.get('DBA Name', ''),
                company_data.get('Phone', ''),
                company_data.get('Physical Address', ''),
                city.title(),
                state.upper(),
               ', '.join(sorted(operating_authority_types)) if operating_authority_types else '',
                ', '.join(sorted(serve_dates)) if serve_dates else '',
                ', '.join(sorted(revocation_types)) if revocation_types else '',
                ', '.join(sorted(effective_dates)) if effective_dates else '',
                ', '.join(sorted(docket_numbers)) if docket_numbers else ''
            ]

            current_sheet_data.append(new_row)
            row_counter += 1

            if len(current_sheet_data) - 1 >= ROWS_PER_SHEET:
                print(f"Preparing to write sheet {sheet_counter} with {row_counter} rows")
                try:
                    sheet_name = f'{TAB_PREFIX}_{sheet_counter}'
                    sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, ROWS_PER_SHEET + 1, num_columns)
                    print("Writing batch of data to google sheet.")
                    write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data, 
                                         sheet_id, num_columns, column_descriptions, new_headers)
                    print(f"Created and populated sheet: {sheet_name}")
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

            # Update progress
            processed_companies += 1
            pbar.update(1)
            pbar.set_postfix({"Processed": processed_companies, "Remaining": total_companies - processed_companies})

            # Add a delay between requests to avoid overwhelming the server
            time.sleep(1)

        # At the end of the function, print any DOT numbers that were extracted more than once
        multiple_extractions = {dot: count for dot, count in extraction_counter.items() if count > 1}
        if multiple_extractions:
            print("DOT numbers extracted multiple times:")
            for dot, count in multiple_extractions.items():
                print(f"USDOT {dot}: {count} times")
            else:
                print("All DOT numbers were extracted exactly once.")

    # Write any remaining data
    if len(current_sheet_data) > 1:
        print(f"Writing final sheet with {len(current_sheet_data)} rows")
        try:
            sheet_name = f'{TAB_PREFIX}_{sheet_counter}'
            sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, len(current_sheet_data), num_columns)
            write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data, 
                                 sheet_id, num_columns, column_descriptions, new_headers)
            print(f"Created and populated final sheet: {sheet_name}")
        except Exception as e:
            error_count += 1
            print(f"Error creating/writing final sheet: {str(e)}")

    print(f"Processing complete. {sheet_counter} sheet(s) created in the Google Spreadsheet.")
    print(f"Total rows processed: {processed_count}")
    print(f"Total rows written: {row_counter + (sheet_counter - 1) * ROWS_PER_SHEET}")
    print(f"Total errors encountered: {error_count}")
    
    # Clear progress file after successful completion
    if os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        print("Progress file removed after successful completion.")

    driver.quit()


if __name__ == "__main__":
    service = get_google_sheets_service()
    process_csv(REVOCATIONS_FILE, service, SPREADSHEET_ID)
