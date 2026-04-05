import os
import json
import csv
import datetime
import re
import emoji
from dotenv import load_dotenv
from telethon import TelegramClient

# Load credentials from .env
load_dotenv()

API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')

if not API_ID or not API_HASH:
    raise ValueError("Please set TELEGRAM_API_ID and TELEGRAM_API_HASH in your .env file.")

STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config', 'fetch_msg_state.json')
IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# Setup directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
CSV_FILENAME = os.path.join(OUTPUT_DIR, 'messages.csv')

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def clean_message(text):
    if not text:
        return ""
    # Remove pictorial emojis
    text = emoji.replace_emoji(text, replace='')
    # Replace one or more newlines/carriage returns with ' | ' to flatten the message safely
    text = re.sub(r'[\r\n]+', ' | ', text)
    # Clean up multiple consecutive spaces
    text = re.sub(r'\s{2,}', ' ', text)
    # Remove leading or trailing spaces and pipes
    text = text.strip(' |')
    # Convert to sentence case
    if text:
        text = text.capitalize()
    return text

async def main(max_latest_msg=None):
    # Target channel
    CHANNEL_NAME = 'https://t.me/Stock_aaj_or_kal'
    # STATE_FILE defined globally
    
    # Create the client session. It creates a 'telegram_session.session' file locally.
    client = TelegramClient('telegram_session', int(API_ID), API_HASH)
    
    print("Connecting to Telegram...")
    # This starts the client. On first run, it will ask for your phone number and login code in the console.
    await client.start()
    print("Connected successfully!")
    
    state = load_state()
    last_id = state.get(CHANNEL_NAME, 0)
    
    print(f"Fetching recent messages from: {CHANNEL_NAME}")
    
    all_messages = []
    
    if max_latest_msg is not None:
        print(f"Fetching exactly the latest {max_latest_msg} messages (Append Mode)...")
        async for message in client.iter_messages(CHANNEL_NAME, limit=max_latest_msg):
            all_messages.append(message)
        all_messages.reverse()
    elif last_id > 0:
        # We have processed this channel before, fetch messages newer than last_id
        # We process in reverse order (oldest to newest) to maintain chronological order
        print(f"Resuming from message ID: {last_id}")
        async for message in client.iter_messages(CHANNEL_NAME, min_id=last_id, reverse=True):
            all_messages.append(message)
    else:
        # First run: fetch messages from the beginning of today in IST
        now_ist = datetime.datetime.now(IST)
        start_of_today_ist = now_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # Telethon expects UTC timezone for offset_date
        start_of_today_utc = start_of_today_ist.astimezone(datetime.timezone.utc)
        
        print(f"First run. Fetching messages from: {start_of_today_ist.strftime('%Y-%m-%d %H:%M:%S')} (IST)")
        
        # Telethon's offset_date fetches messages *older* than the date by default unless reverse=True
        # With reverse=True, it fetches messages *newer* than offset_date
        async for message in client.iter_messages(CHANNEL_NAME, offset_date=start_of_today_utc, reverse=True):
            all_messages.append(message)
            
    if not all_messages:
        print("No new messages found.")
        await client.disconnect() # Disconnect if no messages
        return

    print(f"\n--- Found {len(all_messages)} new messages ---")
    
    # Save messages to CSV
    csv_file = CSV_FILENAME
    file_exists = os.path.isfile(csv_file)
    
    max_csv_id = 0
    analyze_state_path = os.path.join(BASE_DIR, 'config', 'analyze_stocks_state.json')
    if os.path.exists(analyze_state_path):
        try:
            with open(analyze_state_path, 'r') as f:
                max_csv_id = json.load(f).get('last_processed_id', 0)
        except Exception:
            pass
                        
    # We will collect them for writing
    new_rows = []
    
    for message in all_messages:
        # We only care about messages that have text content
        if message.text and message.id > max_csv_id:
            # Convert date from UTC to IST
            message_date_ist = message.date.astimezone(IST)
            date = message_date_ist.strftime('%Y-%m-%d %H:%M:%S')
            
            # Clean the text using our new function
            cleaned_text = clean_message(message.text)
            
            # Print a snippet for the console
            # print(f"[{date}] [ID: {message.id}] {cleaned_text[:80]}...")
            
            # Add the flattened, clean text to our list for CSV
            new_rows.append([message.id, date, cleaned_text])
            
    if new_rows:
        # Always write to CSV in append mode
        with open(csv_file, mode='a', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            # Write a header if the file is new
            if not file_exists:
                writer.writerow(['Message_ID', 'Timestamp', 'Text'])
            writer.writerows(new_rows)
        print(f"\nSaved {len(new_rows)} text messages to {csv_file}")
            
    # Save the ID of the last processed message to state.json
    latest_message_id = all_messages[-1].id
    state[CHANNEL_NAME] = latest_message_id
    save_state(state)
    print(f"\nUpdated last processed message ID to: {latest_message_id}")

    # Wait until disconnected if needed, or disconnect
    await client.disconnect()

if __name__ == '__main__':
    import asyncio
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch Telegram Messages")
    parser.add_argument('--max_latest_msg', type=int, default=None, 
                        help="Optional: Fetch exactly this many latest messages and APPEND to messages.csv")
    args = parser.parse_args()
    
    asyncio.run(main(max_latest_msg=args.max_latest_msg))
