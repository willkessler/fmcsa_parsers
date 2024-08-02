# A parser for the FMCSA census files

The census files can be downloaded from here:

https://ai.fmcsa.dot.gov/SMS/Tools/Downloads.aspx

### Setting up

1. Create a GCP project.
1. Enable Sheets API in the new GCP project.
1. Set up the OAUTH consent screen.
1. Create a *desktop* Oauth 2.0 credential.
1. Download its client_secret file into `client_secret.json` in this directory
1. Copy cities/state you want to extract data for into `cities.txt`.
1. Create a google sheet and get its id. Paste its id into
   `SPREADSHEET_ID` in `direct_to_sheet.py`. This sheet will receive
   the data on each run. Make sure you do not have existing tabs in
   the sheet except `Sheet 1`.

### Running the script

``` python
clear && python3 direct_to_sheet.py
```