import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging

def get_channels_from_google_doc():
    try:
        # Replace 'YOUR_GOOGLE_DOC_CREDENTIALS.json' with the path to your Google Doc credentials file
        CREDENTIALS = ServiceAccountCredentials.from_json_keyfile_name('config/grand-sphere-343001-881f612add4a.json')

        # Replace 'YOUR_GOOGLE_DOC_ID' with the ID of your Google Doc
        GOOGLE_DOC_ID = '1PF7EZJs4G8X46zI5W5NKzozdPEbvNmrKyydb2VuUmgU'

        # Authenticate with Google Sheets API
        client = gspread.authorize(CREDENTIALS)

        # Open the Google Doc
        google_doc = client.open_by_key(GOOGLE_DOC_ID)

        # Get the first sheet of the Google Doc
        sheet = google_doc.get_worksheet(0)

        # Get all values from the B column second row till the end of the sheet
        list_of_strings = sheet.col_values(2)[1:]

        return list_of_strings
    except Exception as e:
        logging.exception("An exception with Google Dock occurred")


result = get_channels_from_google_doc()


