from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel
from telethon.errors import SessionPasswordNeededError
from telethon.sessions import MemorySession
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetFullChannelRequest

from get_channels import get_channels_from_google_doc
from config import auth

from datetime import datetime, timezone
import mysql.connector
import datetime
import csv
import os
import logging



"""
1. Authenticate with the Telegram API. 
2. Get channel list from Google Doc.
3. Get messages from channels.
4. Write messages to database.


"""

# Messing with logging. 

handler = logging.FileHandler('logs.log')
logging.basicConfig(level=logging.CRITICAL, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(handler)






class TelegramNews:
    def __init__(self, api_id, api_hash, bot_token, phone, password, session_id, CHANNELS):
        self.api_id = api_id
        self.api_hash = api_hash
        self.bot_token = bot_token
        self.phone = phone
        self.password = password
        self.session_id = session_id
        self.channels = CHANNELS
        self.client = None

    def authenticate(self):


        """
        Authenticates the user with the Telegram API.

        This method connects to the Telegram API using the provided API ID and API hash.
        If the user is not already authorized, it sends a code request to the user's phone number
        and prompts the user to enter the received code for authentication.

        If the provided session ID is invalid, it falls back to using a memory session for authentication.

        Raises:
            SessionPasswordNeededError: If the user account has a password set and requires password authentication.
        """

        try:
            logger.debug("Initializing Telegram client with MemorySession")
            self.client = TelegramClient(MemorySession(), self.api_id, self.api_hash)
            self.client.connect()
            if not self.client.is_user_authorized():
                try:
                    logger.debug("Trying String Session Authentication")
                    session_string = self.session_id
                    self.client = TelegramClient(StringSession(session_string), self.api_id, self.api_hash)
                    self.client.connect()
                    if not self.client.is_user_authorized():
                        logger.debug("Sending code request")
                        self.client.send_code_request(self.phone)
                        try:
                            logger.debug("Signing in with code")
                            self.client.sign_in(self.phone, input('Enter the code: '))
                        except SessionPasswordNeededError:
                            logger.debug("Signing in with password")
                            self.client.sign_in(password=self.password)
                except ValueError:
                    logger.debug("Falling back to Memory Session Authentication")
                    self.client = TelegramClient(MemorySession(), self.api_id, self.api_hash)
                    self.client.connect()
                    if not self.client.is_user_authorized():
                        logger.debug("Sending code request")
                        self.client.send_code_request(self.phone)
                        try:
                            logger.debug("Signing in with code")
                            self.client.sign_in(self.phone, input('Enter the code: '))
                        except SessionPasswordNeededError:
                            logger.debug("Signing in with password")
                            self.client.sign_in(password=self.password)
        except Exception as e:
            logger.critical(f"Error occurred during authentication: {e}")



    def get_messages(self):
        """
        Retrieves messages from specified channels within a given date range and writes them to a CSV file.
        
        Returns:
            None
        """
        start_date = datetime(2023, 12, 24, tzinfo=timezone.utc)
        end_date = datetime(2023, 12, 31, tzinfo=timezone.utc)

        

        with open('messages.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Check if the file is empty
            if os.stat('messages.csv').st_size == 0:
                writer.writerow(['Date', 'ID', 'Creation Date', 'Number of Subscribers', 'Channel Name', 'Views', 'Message'])

            for channel in CHANNELS:
                channel_entity = self.client.get_entity(channel)
                full_channel = self.client(GetFullChannelRequest(channel=channel_entity))
                posts = self.client(GetHistoryRequest(
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
                    if start_date <= message.date <= end_date:
                        writer.writerow([message.date, message.id, channel_creation_date, channel_subscribers_count, channel_name, message.views, message.message])

    def run(self):
        self.authenticate()
        self.get_messages()
        self.client.disconnect()


API_ID = auth.API_ID
API_HASH = auth.API_HASH
BOT_TOKEN = auth.BOT_TOKEN
PHONE = auth.PHONE  # in international format
PASSWORD = auth.PASSWORD  # if your account has two-step verification enabled
SESSION_ID = auth.SESSION_ID
CHANNELS = get_channels_from_google_doc()

telegram_news = TelegramNews(API_ID, API_HASH, BOT_TOKEN, PHONE, PASSWORD, SESSION_ID, CHANNELS)

if __name__ == '__main__':

    telegram_news.run()

