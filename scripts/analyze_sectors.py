import os
import csv
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq

# Load environment variables (Make sure GROQ_API_KEY is in .env)
load_dotenv()

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

MESSAGES_CSV = os.path.join(OUTPUT_DIR, 'messages.csv')
MARKET_SECTORS_CSV = os.path.join(INPUT_DIR, 'market_sectors.csv')
OUTPUT_CSV = os.path.join(OUTPUT_DIR, 'sector_updates.csv')
SECTOR_STATE_FILE = os.path.join(BASE_DIR, 'config', 'analyze_sectors_state.json')

def load_sector_state():
    if os.path.exists(SECTOR_STATE_FILE):
        with open(SECTOR_STATE_FILE, 'r') as f:
            return json.load(f)
    return {"last_processed_id": 0}

def save_sector_state(state):
    with open(SECTOR_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def read_data():
    state = load_sector_state()
    last_id = state.get("last_processed_id", 0)

    sectors = []
    if os.path.exists(MARKET_SECTORS_CSV):
        with open(MARKET_SECTORS_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sectors.append(row)
                
    raw_messages = []
    max_msg_id = last_id
    if os.path.exists(MESSAGES_CSV):
        with open(MESSAGES_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                m_id = int(row.get('Message_ID', 0))
                if m_id > last_id:
                    raw_messages.append(f"[{row['Timestamp']}] {row['Text']}")
                    if m_id > max_msg_id:
                        max_msg_id = m_id
                        
    return raw_messages, sectors, max_msg_id

def analyze_chunk(client, chunk_msgs, sectors_text):
    messages_text = "\n".join(chunk_msgs)
    
    prompt = f"""
You are an expert financial market analyst. Read the following Telegram messages and the Market Sectors list.

MARKET SECTORS (ID, Name, Description):
{sectors_text}

TELEGRAM MESSAGES (Format: [Timestamp] Message Text):
{messages_text}

YOUR TASK:
1. Identify if any of the messages indicate news or trends that affect any of the listed MARKET SECTORS.
2. CRITICAL ID MAPPING: Use ONLY the exact "Sector ID" from the list.
   - Bank of India/PNB -> Sector ID 27 (PSU Bank).
   - HDFC/ICICI/Axis/CSB -> Sector ID 25 (Private Bank).
   - IndiGo/Aviation -> Sector ID 2 (Aviation).
   - Steel/JSW/Aluminum -> Sector ID 21 (Metal)
   etc.
3. CONSOLIDATED SUMMARY: For each sector in the list below, if the provided messages contain related news, provide exactly ONE brief, consolidated summary for that sector (even if multiple messages or companies are mentioned).
4. CATCH-ALL SECTOR 0: Use ID 0 ONLY for broad macro news (GDP, Inflation) that doesn't fit a specific sector.
5. IMPACT CLASSIFICATION: Choose "Positive" or "Negative" (binary).
   - POSITIVE: Growth, increase in market share, order wins, capacity expansion, positive policy changes.
   - NEGATIVE: Decline, decrease in market share (even if absolute share is large), earnings miss, loss of contracts, restrictive regulations.
   - Example: A drop from 2% to 1% profit is NEGATIVE.
6. OMIT: Results for messages that do not contain relevant sectoral news.
7. If there are any company name present, include all of them in the message summary.

You MUST output valid JSON:
{{
  "results": [
    {{ "sector_id": "...", "is_stock": 1, "timestamp": "...", "msg_summary": "...", "impact": "Positive/Negative" }}
  ]
}}
"""

    print(f"Sending {len(chunk_msgs)} messages to Groq (70B)...")
    chat_completion = client.chat.completions.create(
        messages=[
            {"role": "system", "content": "You are a financial analyst outputting clean JSON classification results."},
            {"role": "user", "content": prompt}
        ],
        model="llama-3.3-70b-versatile",  
        response_format={"type": "json_object"},
        temperature=0.0,
        timeout=90.0
    )
    print("Received response from Groq.")
    
    response_content = chat_completion.choices[0].message.content
    try:
        data = json.loads(response_content)
        return data.get("results", [])
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON response: {e}")
        return []

def run_analysis(messages, sectors):
    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("Error: GROQ_API_KEY not found in .env file.")
        return []
        
    client = Groq(api_key=api_key, timeout=90.0)
    
    sectors_text = ""
    for s in sectors:
        sectors_text += f"Sector ID: {s.get('id', '')}, Name: {s['sector_name']}, Description: {s.get('description', '')}\n"
        
    all_results = []
    chunk_size = 50
    chunks = [messages[i:i + chunk_size] for i in range(0, len(messages), chunk_size)]
    
    print(f"Total messages: {len(messages)}. Split into {len(chunks)} chunks.")
    
    for idx, chunk in enumerate(chunks):
        print(f"Processing batch {idx + 1} of {len(chunks)}...")
        
        success = False
        retries = 5
        while not success and retries > 0:
            try:
                results = analyze_chunk(client, chunk, sectors_text)
                all_results.extend(results)
                success = True
            except Exception as e:
                err_msg = str(e).lower()
                if "rate_limit" in err_msg or "429" in err_msg:
                    print("Rate limit reached. Waiting 60 seconds before retrying...")
                    time.sleep(60)
                    retries -= 1
                else:
                    wait_time = (6 - retries) * 10
                    print(f"Error in Groq analysis: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    retries -= 1
        
        if not success:
            print(f"Failed to process batch {idx + 1} after all retries.")
            
    return all_results

def write_results(results):
    file_exists = os.path.exists(OUTPUT_CSV)
    
    # Determine next starting ID
    start_id = 1
    if file_exists:
        with open(OUTPUT_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    row_id = int(row['id'])
                    if row_id >= start_id:
                        start_id = row_id + 1
                except: continue
                
    # Python-level Safety Aggregation (In case a sector appears in different 100-msg chunks)
    grouped = {}
    run_exec_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for r in results:
        sec_id = str(r.get('sector_id', '')).strip()
        impact = str(r.get('impact', '')).strip().capitalize()
        summary = str(r.get('msg_summary', '')).strip()
        is_stock = r.get('is_stock', 0)
        
        if impact not in ['Positive', 'Negative'] or not sec_id or not summary or 'no relevant news' in summary.lower():
            continue
            
        if sec_id not in grouped:
            grouped[sec_id] = {
                'summaries': [summary],
                'impact': impact,
                'is_stock': is_stock,
                'msg_timestamp': r.get('timestamp', 'None')
            }
        else:
            if summary.lower() not in [s.lower() for s in grouped[sec_id]['summaries']]:
                grouped[sec_id]['summaries'].append(summary)
            
    final_rows = []
    for sec_id, data in grouped.items():
        # Join any cross-chunk summaries with a pipe
        consolidated_summary = " | ".join(data['summaries'])
        
        final_rows.append({
            'id': str(start_id),
            'sector_id': sec_id,
            'is_stock': data['is_stock'],
            'msg_timestamp': data['msg_timestamp'],
            'exec_timestamp': run_exec_ts,
            'msg_summary': consolidated_summary,
            'impact': data['impact']
        })
        start_id += 1
            
    if not final_rows:
        return

    with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8-sig') as f:
        fieldnames = ['id', 'sector_id', 'is_stock', 'msg_timestamp', 'exec_timestamp', 'msg_summary', 'impact']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(final_rows)
        
    print(f"\nAnalysis complete! {len(final_rows)} consolidated sector updates saved to {OUTPUT_CSV}")

if __name__ == "__main__":
    msgs, sects, max_msg_id = read_data()
    
    if not sects:
        print("Missing market_sectors.csv. Exiting.")
    elif not msgs:
        print("No new messages found to process.")
    else:
        results = run_analysis(msgs, sects)
        if results is not None:
            write_results(results)
            save_sector_state({"last_processed_id": max_msg_id})
