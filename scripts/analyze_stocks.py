import os
import csv
import json
import time
from datetime import datetime
from dotenv import load_dotenv
from groq import Groq

# Load environment variables
load_dotenv()

# Setup directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
CONFIG_DIR = os.path.join(BASE_DIR, 'config')

MESSAGES_CSV = os.path.join(OUTPUT_DIR, 'messages.csv')
WATCHLIST_CSV = os.path.join(INPUT_DIR, 'watchlist.csv')
OUTPUT_CSV = os.path.join(OUTPUT_DIR, 'watchlist_stock_updates.csv')
TRACKED_METRICS_CSV = os.path.join(INPUT_DIR, 'tracked_metrics.csv')
OUTPUT_METRICS_CSV = os.path.join(OUTPUT_DIR, 'stock_tracked_metrics.csv')
SENTIMENT_STATE_FILE = os.path.join(CONFIG_DIR, 'analyze_stocks_state.json')

BATCH_SIZE = 30  # Number of new messages to process in one LLM call
MODEL_NAME = "llama-3.3-70b-versatile"

def load_sentiment_state():
    if os.path.exists(SENTIMENT_STATE_FILE):
        try:
            with open(SENTIMENT_STATE_FILE, 'r') as f:
                return json.load(f)
        except: pass
    return {"last_processed_id": 0}

def save_sentiment_state(state):
    with open(SENTIMENT_STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def call_groq_api(client, prompt):
    retries = 3
    while retries > 0:
        try:
            chat_completion = client.chat.completions.create(
                messages=[
                    {"role": "system", "content": "You are a financial news analyzer that outputs ONLY JSON."},
                    {"role": "user", "content": prompt}
                ],
                model=MODEL_NAME,
                response_format={"type": "json_object"},
                temperature=0.0
            )
            return json.loads(chat_completion.choices[0].message.content)
        except Exception as e:
            err_msg = str(e).lower()
            if "rate_limit" in err_msg or "429" in err_msg:
                print("Rate limit reached. Waiting 60s...")
                time.sleep(60)
                retries -= 1
            else:
                time.sleep(5)
                retries -= 1
    return {}

def run_watchlist_analysis(client, messages_text, watchlist_text):
    prompt = f"""
You are an expert financial analyst. Identify news related to watchlist companies.
WATCHLIST:
{watchlist_text}
MESSAGES:
{messages_text}

TASK:
1. Identify news for companies in the watchlist (symbols/aliases/names).
2. ENTITY IDENTITY RULE: Be strict. "Bank of Baroda" is ID 10. "Bank of India" is ID 6.
3. Impact: "Positive" or "Negative".
4. OMIT if no news found.

Return JSON: {{ "general_results": [ {{ "company_id": "...", "timestamp": "...", "msg_summary": "...", "impact": "Positive/Negative" }} ] }}
"""
    data = call_groq_api(client, prompt)
    return data.get("general_results", [])

def run_metrics_analysis(client, messages_text, metrics_text):
    prompt = f"""
You are an expert financial analyst. Match the news to the "Tracked Metrics" for each company.
METRICS TO TRACK:
{metrics_text}
MESSAGES:
{messages_text}

TASK:
1. ONLY record an update for companies and metrics EXPLICITLY listed in "METRICS TO TRACK".
2. STRICT ENTITY MATCHING: Completely ignore news for companies NOT in the "METRICS TO TRACK" list (e.g. if RBL Bank or HDFC is in the news but not in the track list, ignore them). Do not hallucinate or guess company IDs.
3. ONLY record an update if a message contains EXPLICIT data for a specific metric being tracked.
4. EVIDENCE-ONLY: No hallucinations. If news doesn't mention the metric, do NOT record it.
5. NEVER return "No Update" or "N/A". Omit from list instead.
6. STATUS: "Positive", "Negative", or "Achieved".
7. Only record part of message related to the company to track, not the whole message.

Return JSON: {{ "metric_updates": [ {{ "company_id": "...", "metric_id": "...", "timestamp": "...", "msg_summary": "...", "metric_status": "Positive/Negative/Achieved" }} ] }}
"""
    data = call_groq_api(client, prompt)
    return data.get("metric_updates", [])

def write_results(general_results, metric_updates, valid_watchlist=None, valid_metrics=None):
    valid_c_ids = {str(w['id']) for w in valid_watchlist} if valid_watchlist else set()
    valid_m_ids = {str(m['id']) for m in valid_metrics} if valid_metrics else set()
    valid_c_m_map = {str(m['id']): str(m['company_id']) for m in valid_metrics} if valid_metrics else {}

    run_exec_ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # --- Write General Sentiment ---
    if general_results:
        grouped_gen = {}
        for r in general_results:
            c_id = str(r.get('company_id', '')).strip()
            impact = str(r.get('impact', '')).strip().capitalize()
            summary = str(r.get('msg_summary', '')).strip()
            timestamp = r.get('timestamp', 'None')
            
            if c_id not in valid_c_ids or impact not in ['Positive', 'Negative']: continue
            
            if c_id not in grouped_gen:
                grouped_gen[c_id] = {'c_id': c_id, 'summaries': [summary], 'impact': impact, 'timestamp': timestamp}
            else:
                if summary.lower() not in [s.lower() for s in grouped_gen[c_id]['summaries']]:
                    grouped_gen[c_id]['summaries'].append(summary)

        file_exists = os.path.exists(OUTPUT_CSV)
        current_id = 1
        if file_exists:
            with open(OUTPUT_CSV, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try: current_id = max(current_id, int(row['id']) + 1)
                    except: pass
        
        with open(OUTPUT_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['id', 'company_id', 'msg_timestamp', 'exec_timestamp', 'msg_summary', 'impact']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists: writer.writeheader()
            for c_id, data in grouped_gen.items():
                row = {
                    'id': str(current_id),
                    'company_id': c_id,
                    'msg_timestamp': data['timestamp'],
                    'exec_timestamp': run_exec_ts,
                    'msg_summary': " | ".join(data['summaries']),
                    'impact': data['impact']
                }
                writer.writerow(row)
                current_id += 1

    # --- Write Metric Updates ---
    if metric_updates:
        grouped_met = {}
        for r in metric_updates:
            c_id = str(r.get('company_id', '')).strip()
            m_id = str(r.get('metric_id', '')).strip()
            status = str(r.get('metric_status', '')).strip().capitalize()
            summary = str(r.get('msg_summary', '')).strip()
            timestamp = r.get('timestamp', 'None')
            
            if m_id not in valid_m_ids or c_id not in valid_c_ids: continue
            if valid_c_m_map.get(m_id) != c_id: continue
            
            key = f"{c_id}_{m_id}"
            if key not in grouped_met:
                grouped_met[key] = {'c_id': c_id, 'm_id': m_id, 'summaries': [summary], 'status': status, 'timestamp': timestamp}
            else:
                if summary.lower() not in [s.lower() for s in grouped_met[key]['summaries']]:
                    grouped_met[key]['summaries'].append(summary)

        metrics_file_exists = os.path.exists(OUTPUT_METRICS_CSV)
        metric_start_id = 1
        if metrics_file_exists:
            with open(OUTPUT_METRICS_CSV, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try: metric_start_id = max(metric_start_id, int(row['id']) + 1)
                    except: pass

        with open(OUTPUT_METRICS_CSV, 'a', newline='', encoding='utf-8-sig') as f:
            fieldnames = ['id', 'company_id', 'metric_id', 'msg_timestamp', 'exec_timestamp', 'msg_summary', 'metric_status']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not metrics_file_exists: writer.writeheader()
            for key, data in grouped_met.items():
                row = {
                    'id': str(metric_start_id),
                    'company_id': data['c_id'],
                    'metric_id': data['m_id'],
                    'msg_timestamp': data['timestamp'],
                    'exec_timestamp': run_exec_ts,
                    'msg_summary': " | ".join(data['summaries']),
                    'metric_status': data['status']
                }
                writer.writerow(row)
                metric_start_id += 1


if __name__ == "__main__":
    state = load_sentiment_state()
    last_processed_id = int(state.get("last_processed_id", 0))

    # 1. Load context
    watchlist = []
    if os.path.exists(WATCHLIST_CSV):
        with open(WATCHLIST_CSV, 'r', encoding='utf-8-sig') as f:
            watchlist = list(csv.DictReader(f))
    
    tracked_metrics = []
    if os.path.exists(TRACKED_METRICS_CSV):
        with open(TRACKED_METRICS_CSV, 'r', encoding='utf-8-sig') as f:
            tracked_metrics = list(csv.DictReader(f))

    if not watchlist:
        print("Missing watchlist.csv. Exiting.")
        os._exit(0)

    watchlist_info = ""
    metrics_info = ""
    for w in watchlist:
        watchlist_info += f"ID: {w['id']}, Symbol: {w['symbol']}, Name: {w['company_name']}, Aliases: {w.get('aliases','')}\n"
        c_metrics = [m for m in tracked_metrics if str(m.get('company_id','')) == str(w['id'])]
        for m in c_metrics:
            metrics_info += f"Company: {w['company_name']} (ID: {w['id']}) - Metric ID {m['id']}: {m['metric_name']} (Tgt: {m.get('target_date','')})\n"

    # 2. Get Messages
    all_rows = []
    if os.path.exists(MESSAGES_CSV):
        with open(MESSAGES_CSV, 'r', encoding='utf-8-sig') as f:
            all_rows = list(csv.DictReader(f))

    new_msg_indices = [i for i, r in enumerate(all_rows) if int(r.get('Message_ID', 0)) > last_processed_id]
    
    if not new_msg_indices:
        print("No new messages to process.")
        os._exit(0)

    # 3. Prepare Batches
    print(f"Preparing {len(new_msg_indices)} new messages in batches of {BATCH_SIZE}...")
    batch_data = [] # List of tuples: (batch_text, last_msg_id_in_batch)
    
    for start_idx in range(0, len(new_msg_indices), BATCH_SIZE):
        batch_indices = new_msg_indices[start_idx : start_idx + BATCH_SIZE]
        if not batch_indices: continue
        
        min_i = max(0, batch_indices[0] - 2)
        max_i = min(len(all_rows) - 1, batch_indices[-1] + 2)
        
        batch_msgs = []
        for i in range(min_i, max_i + 1):
            row = all_rows[i]
            prefix = "[CONTEXT] " if i not in batch_indices else ""
            batch_msgs.append(f"{prefix}[{row['Timestamp']}] {row['Text']}")
        
        batch_text = "\n".join(batch_msgs)
        last_id = int(all_rows[batch_indices[-1]].get('Message_ID', 0))
        batch_data.append((batch_text, last_id))

    api_key = os.environ.get("GROQ_API_KEY")
    if not api_key:
        print("Error: GROQ_API_KEY not found.")
        os._exit(0)
    client = Groq(api_key=api_key)

    # 4. PASS 1: Watchlist Analysis
    print(f"\n--- PASS 1: Watchlist Sentiment Analysis ({len(batch_data)} batches) ---")
    all_gen_results = []
    for i, (text, _) in enumerate(batch_data):
        print(f"  > Watchlist Batch {i+1}/{len(batch_data)}...")
        gen_results = run_watchlist_analysis(client, text, watchlist_info)
        all_gen_results.extend(gen_results)

    # 5. GAP
    print("\n[INFO] Watchlist pass complete. Waiting 60 seconds before Metrics pass...")
    time.sleep(60)

    # 6. PASS 2: Metrics Analysis
    print(f"--- PASS 2: Tracked Metrics Analysis ({len(batch_data)} batches) ---")
    all_met_updates = []
    for i, (text, _) in enumerate(batch_data):
        print(f"  > Metrics Batch {i+1}/{len(batch_data)}...")
        met_updates = run_metrics_analysis(client, text, metrics_info)
        all_met_updates.extend(met_updates)

    # Write aggregated results
    write_results(all_gen_results, all_met_updates, valid_watchlist=watchlist, valid_metrics=tracked_metrics)

    # 7. Update State (using the last batch's ID)
    final_last_id = batch_data[-1][1]
    save_sentiment_state({"last_processed_id": final_last_id})

    print("\nAll batches complete! Reports updated in 'output/' folder.")
