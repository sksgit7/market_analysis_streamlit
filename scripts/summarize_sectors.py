import os
import csv
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq

load_dotenv()

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

SECTOR_UPDATES_CSV = os.path.join(OUTPUT_DIR, 'sector_updates.csv')
MARKET_SECTORS_CSV = os.path.join(INPUT_DIR, 'market_sectors.csv')
SECTOR_SUMMARY_CSV = os.path.join(OUTPUT_DIR, 'sector_summary.csv')

MODEL_NAME = "llama-3.3-70b-versatile"

def read_data():
    from datetime import timedelta
    cutoff_date = datetime.now() - timedelta(days=15)
    
    sectors_map = {}
    if os.path.exists(MARKET_SECTORS_CSV):
        with open(MARKET_SECTORS_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                sectors_map[row['id']] = row['sector_name']
                
    new_news = {}
    if os.path.exists(SECTOR_UPDATES_CSV):
        with open(SECTOR_UPDATES_CSV, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    # Must be within 15 days window
                    msg_ts_str = row.get('msg_timestamp', '')
                    try:
                        msg_ts = datetime.strptime(msg_ts_str, '%Y-%m-%d %H:%M:%S')
                        if msg_ts < cutoff_date:
                            continue # Skip news older than 15 days
                    except: pass
                    
                    s_id = row['sector_id']
                    if s_id not in new_news:
                        new_news[s_id] = []
                    new_news[s_id].append(row['msg_summary'])
                except: continue
                
    return new_news, sectors_map

def update_sector_summary(client, sector_name, new_news_list):
    new_news_text = "\n".join([f"- {n}" for n in new_news_list])
    
    prompt = f"""
As a senior financial analyst, create a brief executive summary for the '{sector_name}' sector based on the recent news from the last 15 days.

RECENT NEWS ITEMS:
{new_news_text}

YOUR TASK:
1. Synthesize the news into a SINGLE, brief 2-3 line executive summary.
2. Provide an overall 'impact' (exactly "Positive" or "Negative") based on the net sentiment of these news items.
3. If there is conflicting news, weigh the severity (e.g., government policy usually outweighs minor earning misses).
4. Rename "Sector 0" as "Global Market".

Output ONLY a JSON object:
{{
  "summary": "...",
  "impact": "Positive/Negative"
}}
"""
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.0
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        raise e

def run_aggregation():
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("Missing GROQ_API_KEY. Exiting.")
        return
        
    client = Groq(api_key=api_key, timeout=90.0)
    
    new_news, sectors_map = read_data()
    
    if not new_news:
        print("No sector updates found in the last 15 days.")
        return
        
    run_exec_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    current_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    snapshot_map = {}
    
    for s_id, news_list in new_news.items():
        s_name = sectors_map.get(s_id, f"Sector {s_id}")
        
        print(f"Generating summary for {s_name} ({len(news_list)} recent items)...")
        
        updated_result = None
        retries = 5
        while retries > 0:
            try:
                updated_result = update_sector_summary(client, s_name, news_list)
                if updated_result: break
            except Exception as e:
                err_msg = str(e).lower()
                if "rate_limit" in err_msg or "429" in err_msg:
                    print(f"Rate limit reached on {s_name}. Waiting 60s before retrying...")
                    time.sleep(60)
                    retries -= 1
                else:
                    wait_time = (6 - retries) * 10
                    print(f"Error summarizing {s_name}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    retries -= 1
        
        if updated_result:
            print(f"  > Summary: {updated_result.get('summary', '')[:10]}...")
            snapshot_map[s_id] = {
                'summary': updated_result.get('summary', ''),
                'impact': updated_result.get('impact', 'Positive'),
                'msg_timestamp': current_ts
            }
        
        # Respectful pause to stay under RPM limits
        time.sleep(1)
            
    # Final Write: Overwrite sector_summary.csv with the full snapshot map
    fieldnames = ['id', 'sector_id', 'sector_name', 'msg_timestamp', 'exec_timestamp', 'summary', 'impact']
    rows_to_write = []
    
    # Sort by sector_id for consistency
    sorted_ids = sorted(snapshot_map.keys(), key=lambda x: int(x) if x.isdigit() else 999)
    
    for idx, s_id in enumerate(sorted_ids):
        data = snapshot_map[s_id]
        rows_to_write.append({
            'id': idx + 1,
            'sector_id': s_id,
            'sector_name': sectors_map.get(s_id, 'Unknown'),
            'msg_timestamp': data.get('msg_timestamp', ''),
            'exec_timestamp': run_exec_ts,
            'summary': data.get('summary', ''),
            'impact': data.get('impact', 'Positive')
        })
        
    with open(SECTOR_SUMMARY_CSV, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_to_write)
        
    print(f"Sector Summaries Updated! {len(new_news)} sectors processed.")
    print(f"Summary saved to {SECTOR_SUMMARY_CSV}")

if __name__ == "__main__":
    run_aggregation()
