from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import MemorySession
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetFullChannelRequest

from get_channels import get_channels_from_google_doc
from config import auth

from datetime import datetime, timedelta, timezone
import pytz
import csv
import os
import logging
import mysql.connector





"""
1. Authenticate with the Telegram API. 
2. Get channel list from Google Doc.
3. Get messages from channels.
4. Write messages to database. -> sql_import.py


"""

class TelegramNewsLogger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler for critical logs
        critical_handler = logging.FileHandler('logs.log')
        critical_handler.setLevel(logging.CRITICAL)

        # Create a logging format for critical logs
        critical_formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')

        # Set the formatter for the critical handler
        critical_handler.setFormatter(critical_formatter)

        # Add the critical handler to the logger
        self.logger.addHandler(critical_handler)

        # Create a file handler for debug logs
        debug_handler = logging.FileHandler('logs.log')
        debug_handler.setLevel(logging.DEBUG)

        # Create a logging format for debug logs
        debug_formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')

        # Set the formatter for the debug handler
        debug_handler.setFormatter(debug_formatter)

        # Add the debug handler to the logger
        self.logger.addHandler(debug_handler)

logger = TelegramNewsLogger().logger


def authenticate():


    """
    Authenticates the user with the Telegram API.

    This method connects to the Telegram API using the provided API ID and API hash.
    If the user is not already authorized, it sends a code request to the user's phone number
    and prompts the user to enter the received code for authentication.

    If the provided session ID is invalid, it falls back to using a memory session for authentication.

    Raises:
        SessionPasswordNeededError: If the user account has a password set and requires password authentication.
    """
    client = None

    try:
        logger.debug("Initializing Telegram client with MemorySession")
        client = TelegramClient(MemorySession(), API_ID, API_HASH)
        client.connect()
        if not client.is_user_authorized():
            try:
                logger.debug("Trying String Session Authentication")
                client = TelegramClient(StringSession(SESSION_ID), API_ID, API_HASH)
                client.connect()
                if not client.is_user_authorized():
                    logger.debug("Sending code request")
                    client.send_code_request(PHONE)
                    try:
                        logger.debug("Signing in with code")
                        client.sign_in(PHONE, input('Enter the code: '))
                    except SessionPasswordNeededError:
                        logger.debug("Signing in with password")
                        client.sign_in(password=PASSWORD)

            except ValueError:
                logger.debug("Falling back to Memory Session Authentication")
                client = TelegramClient(MemorySession(), API_ID, API_HASH)
                client.connect()
                if not client.is_user_authorized():
                    logger.debug("Sending code request")
                    client.send_code_request(PHONE)
                    
                    try:
                        logger.debug("Signing in with code")
                        client.sign_in(PHONE, input('Enter the code: '))
                    except SessionPasswordNeededError:
                        logger.debug("Signing in with password")
                        client.sign_in(password=PASSWORD)
    
    except Exception as e:
        logger.critical(f"Error occurred during authentication: {e}")
    return client



def get_messages(client):
    """
    Retrieves messages from specified channels within a given date range and writes them to a CSV file and database.
    
    Returns:
        None
    """

    # Opening connection to MySQL database
    
    def open_database_connection():
        try: 
            connection = mysql.connector.connect(
            host=DB_ENDPOINT,
            user=DB_USERNAME,
            password=DB_PASSWORD,
            database=DB_DATABASE,
            
            )
            logger.debug("Successfully connected to the database")
            return connection
        
        except mysql.connector.Error as error:
            logger.critical(f"Error connecting to the database: {error}")
            return None
    



    with open('messages.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)

      # Opening connection to DB. 
        db_connection = open_database_connection()
        

        # Check if the file is empty
        if os.stat('messages.csv').st_size == 0:
            writer.writerow(['Date', 'ID', 'Creation Date', 'Number of Subscribers', 'Channel Name', 'Views', 'Message'])
        

        for channel in CHANNELS:
            channel_entity = client.get_entity(channel)
            full_channel = client(GetFullChannelRequest(channel=channel_entity))
            posts = client(GetHistoryRequest(
                peer=PeerChannel(channel_entity.id),
                limit=3,
                offset_date=None,
                offset_id=0,
                max_id=0,
                min_id=0,
                add_offset=0,
                hash=0))
            
            
            channel_name = channel_entity.title
            channel_creation_date = channel_entity.date
            channel_subscribers_count = full_channel.full_chat.participants_count
        
            for message in posts.messages:
                if start_date <= message.date <= end_date: # Check if the message is within the date range
                    writer.writerow([message.date, message.id, channel_creation_date, channel_subscribers_count, channel_name, message.views, message.message])

                    # Writing to database

                    sql = "INSERT INTO messages (date, id, channel_creation_date, channel_subscribers_count, channel_name, views, message) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                    for message in posts.messages:
                        val = (message.date, message.id, channel_creation_date, channel_subscribers_count, channel_name, message.views, message.message)
                        try:
                           
                            cursor = db_connection.cursor()
                            cursor.execute(sql, val)
                            logger.debug("Successfully wrote to the database: " + str(message.id))
                            
                        except Exception as e:
                            logger.critical(f"Failed to write to the database: {e}")
                            #logger.critical(f"{sql},{val}") 
            
    db_connection.commit()        
    db_connection.close()
    logger.debug("Closed database connection")
    cursor.close()
    logger.debug("Closed cursor connection")
   

        


API_ID = auth.API_ID
API_HASH = auth.API_HASH
BOT_TOKEN = auth.BOT_TOKEN
PHONE = auth.PHONE  # in international format
PASSWORD = auth.PASSWORD  # if your account has two-step verification enabled
SESSION_ID = auth.SESSION_ID
CHANNELS = get_channels_from_google_doc()
DB_ENDPOINT = auth.DB_ENDPOINT
DB_USERNAME = auth.DB_USERNAME
DB_PASSWORD = auth.DB_PASSWORD
DB_DATABASE = auth.DB_DATABASE

start_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
end_date = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=999) - timedelta(days=1)

client = authenticate()

#telegram_news = TelegramNewsAuthenticator(API_ID, API_HASH, BOT_TOKEN, PHONE, PASSWORD, SESSION_ID, CHANNELS)

if __name__ == '__main__':
    get_messages(client)

    

