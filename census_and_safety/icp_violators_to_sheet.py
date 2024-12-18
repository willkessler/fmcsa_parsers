import csv
import os
import time
import chardet
import hashlib
import requests
from pprint import pformat

# Google sheet libs
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Scraping libs
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from urllib.parse import urlparse, unquote, quote_plus, urlencode

# This script will try to filter down to companies with 10-50 power units, not government entities, with > 5 OOS or violations. It will only include
# companies with truck tractors or trailers.

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")  # Run in headless mode

# Set up WebDriver using webdriver_manager. We use scraping the FMCSA to get vehicle counts
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Google Sheets API setup
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CLIENT_SECRET_FILE = './client_secret.json'
TOKEN_FILE = 'token.json'
SPREADSHEET_ID = '1hdza5Q5G8xfiTtqGXjEMHgh-mcg_yjlt8-45XT6V89E';
SCRAPING_CACHE_DIR='scraping_cache/'

# File setup
CENSUS_FILE = 'raw_data/FMCSA_CENSUS1_2024Nov.txt'
SAFETY_FILE_AB = 'raw_data/SMS_AB_PassProperty_2024Nov.txt'
SAFETY_FILE_C = 'raw_data/SMS_C_PassProperty_2024Nov.txt'
README_FILE = 'raw_data/READMEs/CENSUS_README.txt'
EXCLUDED_DOT_NUMBERS_FILE = 'raw_data/excluded_dotnumbers.txt'
SAFETY_README_FILE = 'raw_data/READMEs/SAFETY_README.txt'
ROWS_PER_SHEET = 50000
MAX_COLUMN_WIDTH = 250
EXCLUDE_FILE = 'exclude_columns.txt'
CITIES_FILE = 'cities.txt'
BATCH_SIZE = 1000
MAX_RETRIES = 5

# Ensure the scraping cache directory exists
os.makedirs(SCRAPING_CACHE_DIR, exist_ok=True)

def read_excluded_dot_numbers(filename):
    excluded_dot_numbers = set()
    with open(filename, 'r') as file:
        for line in file:
            excluded_dot_numbers.add(line.strip())
    return excluded_dot_numbers

def pretty_print_dict(d):
    return pformat(d, indent=4)

def get_cached_page(url):
    """Retrieve a cached page if it exists, otherwise return None."""
    filename = hashlib.md5(url.encode()).hexdigest() + '.html'
    filepath = os.path.join(SCRAPING_CACHE_DIR, filename)
    if os.path.exists(filepath):
        with open(filepath, 'r', encoding='utf-8') as file:
            return file.read()
    return None

def cache_page(url, content):
    """Cache a page's content."""
    filename = hashlib.md5(url.encode()).hexdigest() + '.html'
    filepath = os.path.join(SCRAPING_CACHE_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(content)

# Get the FMCSA data for truck tractor and trailer counts (not straight trucks e.g. box trucks).
# This data is unfortunately not available in the QC Api, but can be scraped from SAFER pages.
def collect_vehicle_counts(usdot_number: str, driver):
    url = f"https://ai.fmcsa.dot.gov/SMS/Carrier/{usdot_number}/CarrierRegistration.aspx"

    # Check if the page is cached
    cached_content = get_cached_page(url)
    if cached_content:
        # print("Fetching from scraping cache.")
        soup = BeautifulSoup(cached_content, 'html.parser')
    else:
        driver.get(url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
        time.sleep(random.uniform(0.5, 1))
        
        # Cache the page content
        page_content = driver.page_source
        cache_page(url, page_content)
        
        soup = BeautifulSoup(page_content, 'html.parser')
    
    vehicle_types = ['Straight Trucks', 'Truck Tractors', 'Trailers','Hazmat Cargo Tank Trailers', 'Hazmat Cargo Tank Trucks']
    vehicle_counts = {vtype: 0 for vtype in vehicle_types}

    for vehicle_type in vehicle_types:
        elements = soup.find_all('th', class_='vehType', string=lambda text: vehicle_type in text if text else False)
        for element in elements:
            sibling = element.find_next_sibling('td')
            if sibling:
                try:
                    count = int(sibling.text.strip())
                    vehicle_counts[vehicle_type] += count
                except ValueError:
                    print(f"Error parsing count for {vehicle_type}")

    return vehicle_counts

def should_include_company(vehicle_counts, min_threshold=5):
    """
    Determine whether to include the company based on the number of truck tractors or trailers.
    
    Args:
    vehicle_counts (dict): Total number of straight trucks, truck tractors, and trailers
    min_threshold (int): Minimum number of truck tractors or trailers required (default is 5)
    
    Returns:
    bool: True if the company should be included, False otherwise
    """
    return vehicle_counts['Truck Tractors'] > min_threshold or vehicle_counts['Trailers'] > min_threshold


def check_veh_maint(row,headers):
    authorized_for_hire_index = headers.index('AUTHORIZED_FOR_HIRE')
    nbr_power_unit_index = headers.index('NBR_POWER_UNIT')
    veh_maint_insp_w_viol_index = headers.index('VEH_MAINT_INSP_W_VIOL')
    veh_oos_insp_total_index = headers.index('VEHICLE_OOS_INSP_TOTAL')
    federal_government_index = headers.index('FEDERAL_GOVERNMENT')
    state_government_index = headers.index('STATE_GOVERNMENT')
    local_government_index = headers.index('LOCAL_GOVERNMENT')

    try:
        nbr_power_unit = int(row[nbr_power_unit_index])
        veh_maint_insp_w_viol = int(row[veh_maint_insp_w_viol_index])
        veh_oos_insp_total = int(row[veh_oos_insp_total_index])
    except ValueError:
        return False  # Invalid numeric data

    return all([
        row[authorized_for_hire_index] != 'N',
        row[federal_government_index] != 'Y',
        row[state_government_index] != 'Y',
        row[local_government_index] != 'Y',
        10 <= nbr_power_unit <= 50,
        (veh_oos_insp_total >= 5 or veh_maint_insp_w_viol >= 5) # Focus on oos truck counts, AND/OR maintenance violations
    ])

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

        # Hyperlink the USDot number to a useful page
        for row in batch:
            us_dot_number = row[0]
            row[0] = f'=HYPERLINK("https://ai.fmcsa.dot.gov/SMS/Carrier/{us_dot_number}/Overview.aspx", "{us_dot_number}")'

        body = {
            'values': batch
        }
        for attempt in range(MAX_RETRIES):
            try:
                service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id, range=range_name,
                    valueInputOption='USER_ENTERED', body=body).execute()
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
        }
    ]

    body = {
        'requests': requests
    }
    for attempt in range(MAX_RETRIES):
        try:
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
            # Rerunning the resize doesn't seem to help it resize properly. It resizes columns to a fixed width.
            auto_resize_requests = [{
                'autoResizeDimensions': {
                    'dimensions': {
                        'sheetId': sheet_id,
                        'dimension': 'COLUMNS',
                        'startIndex': 0,
                        'endIndex': num_columns
                    }
                }
            }]
            auto_resize_body = {
                'requests': auto_resize_requests
            }
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=auto_resize_body).execute()
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
    # Check for conflicts
    conflicts = set(column_descriptions_census.keys()) & set(column_descriptions_safety.keys())
    if conflicts:
        print(f"Warning: The following columns appear in both README files: {conflicts}")
        print("The descriptions from the safety README will be used for these columns.")

    # Merge the dictionaries
    column_descriptions = {**column_descriptions_census, **column_descriptions_safety}

    print("Reading safety data...")
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

    # Read excluded DOT numbers from previous reach-out efforts
    excluded_dot_numbers = read_excluded_dot_numbers(EXCLUDED_DOT_NUMBERS_FILE)
    print(f"Loaded {len(excluded_dot_numbers)} excluded DOT numbers.")

    with open(census_file, 'r', encoding=encoding, errors='replace') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)

        include_indices = [i for i, header in enumerate(headers) if header not in exclude_columns]
        filtered_headers = [headers[i] for i in include_indices]

        # Add safety data headers
        if safety_data:
            safety_headers = list(next(iter(safety_data.values())).keys())
            filtered_headers.extend(safety_headers)

        # Add vehicle count headers
        filtered_headers.extend(['Straight Trucks', 'Truck Tractors', 'Trailers','Hazmat Cargo Tank Trailers', 'Hazmat Cargo Tank Trucks','In 8/5/2024 campaign'])
        
        num_columns = len(filtered_headers)
        current_sheet_data.append(filtered_headers)
        num_columns = len(filtered_headers)

        dot_number_index = headers.index('DOT_NUMBER')
        phy_city_index = headers.index('PHY_CITY')
        phy_state_index = headers.index('PHY_STATE')
        email_index = headers.index('EMAIL_ADDRESS')
        nbr_power_unit_index = filtered_headers.index('NBR_POWER_UNIT')
        veh_maint_insp_w_viol_index = filtered_headers.index('VEH_MAINT_INSP_W_VIOL')
        veh_oos_insp_total_index = filtered_headers.index('VEHICLE_OOS_INSP_TOTAL')

        total_rows = sum(1 for _ in reader)
        csvfile.seek(0)
        next(reader)  # Skip header again

        for line_num, row in enumerate(reader, start=2):
            try:
                if line_num % 1000 == 0:
                    print(f"Processed {line_num} out of {total_rows} rows ({(line_num/total_rows)*100:.2f}%)")

                dot_number = row[dot_number_index]

                # Mark if the DOT number is in the excluded list
                in_previous_campaign = 'N'
                if dot_number in excluded_dot_numbers:
                    in_previous_campaign = 'Y'

                city = row[phy_city_index].strip().lower()
                state = row[phy_state_index].strip().lower()

                # Must be in chosen cities
                if city not in cities or cities[city] != state:
                    skipped_count += 1
                    continue

                # Must have an email
                if not row[email_index].strip():
                    skipped_count += 1
                    continue
                
                filtered_row = [row[i] for i in include_indices]

                # Add safety data
                if dot_number in safety_data:
                    filtered_row.extend(safety_data[dot_number].values())
                else:
                    filtered_row.extend([''] * len(safety_headers))

                filtered_row[nbr_power_unit_index] = str(filtered_row[nbr_power_unit_index]).strip()
                filtered_row[veh_maint_insp_w_viol_index] = str(filtered_row[veh_maint_insp_w_viol_index]).strip()
                filtered_row[veh_oos_insp_total_index] = str(filtered_row[veh_oos_insp_total_index]).strip()

                if not check_veh_maint(filtered_row, filtered_headers):
                    skipped_count += 1
                    continue

                vehicle_counts = collect_vehicle_counts(dot_number, driver)
                if not should_include_company(vehicle_counts):
                    # print(f"Company {dot_number} does not have the right fleet composition:\n{pretty_print_dict(vehicle_counts)}")
                    skipped_count += 1
                    continue
                # else:
                    # print(f"Company {dot_number} has the right fleet composition:\n{pretty_print_dict(vehicle_counts)}")

                # Add vehicle counts to the filtered row
                filtered_row.extend([
                    str(vehicle_counts['Straight Trucks']),
                    str(vehicle_counts['Truck Tractors']),
                    str(vehicle_counts['Trailers']),
                    str(vehicle_counts['Hazmat Cargo Tank Trailers']),
                    str(vehicle_counts['Hazmat Cargo Tank Trucks']),
                    in_previous_campaign
                ])

                current_sheet_data.append(filtered_row)
                row_counter += 1
                processed_count += 1
                included_count += 1

                if row_counter == ROWS_PER_SHEET:
                    sheet_name = f'Merged_Data_{sheet_counter}'
                    print(f"Creating non-final sheet: {sheet_name}")
                    sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, ROWS_PER_SHEET + 1, num_columns)
                    write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data)
                    format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, filtered_headers)
                    print(f"Created and populated non-final sheet: {sheet_name}")
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
        print(f"Creating final sheet: {sheet_name}")
        sheet_id = create_new_sheet(service, spreadsheet_id, sheet_name, row_counter + 1, num_columns)
        write_to_sheet_batch(service, spreadsheet_id, sheet_name, current_sheet_data)
        format_sheet(service, spreadsheet_id, sheet_id, num_columns, column_descriptions, filtered_headers)
        print(f"Created and populated final sheet: {sheet_name}")

    print(f"Processing complete. {sheet_counter} sheet(s) created in the Google Spreadsheet.")
    print(f"Total rows in input file: {total_rows}")
    print(f"Total rows processed: {processed_count}")
    print(f"Total rows included: {included_count}")
    print(f"Total rows skipped: {skipped_count}")
    print(f"Total errors encountered: {error_count}")

if __name__ == "__main__":
    service = get_google_sheets_service()
    try:
        process_csv(CENSUS_FILE, SAFETY_FILE_AB, SAFETY_FILE_C, service, SPREADSHEET_ID)
    except Exception as e:
        print(f"Error running process_csv: {e}")
    finally:
        driver.quit()
