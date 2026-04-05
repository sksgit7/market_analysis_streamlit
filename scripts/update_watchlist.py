import os
import csv
import requests
import re
from bs4 import BeautifulSoup

# Configuration
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_DIR = os.path.join(BASE_DIR, 'input')
WATCHLIST_CSV = os.path.join(INPUT_DIR, 'watchlist.csv')
URL_TEMPLATE = "https://www.screener.in/company/{symbol}/consolidated/"

def make_session():
    s = requests.Session()
    s.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    })
    return s

def clean_text(text: str) -> str:
    # Remove citation marks like [1], [2]
    text = re.sub(r'\[\d+\]', '', text)
    # Convert multiple spaces to a single space
    text = re.sub(r'\s{2,}', ' ', text)
    return text.strip()

def scrape_stock_info(session, symbol: str):
    url = URL_TEMPLATE.format(symbol=symbol)
    print(f"Fetching {symbol} from {url} ...")
    
    rsp = session.get(url, timeout=15)
    if rsp.status_code != 200:
        print(f"Failed to fetch {symbol} (HTTP {rsp.status_code})")
        return None, None, None
        
    soup = BeautifulSoup(rsp.text, 'html.parser')
    
    # --- 1. Extract CMP ---
    cmp_val = ""
    for item in soup.find_all('li', class_='flex flex-space-between'):
        name_span = item.find('span', class_='name')
        if name_span and 'Current Price' in name_span.get_text():
            num_span = item.find('span', class_='number')
            if num_span:
                val_str = num_span.get_text(strip=True).replace(',', '')
                try:
                    cmp_val = int(float(val_str))
                except ValueError:
                    pass
            break
            
    # --- 1.5 Extract Full Legal Name ---
    full_name_text = ""
    h1_tag = soup.find('h1', class_='show-from-tablet-landscape')
    if not h1_tag:
        h1_tag = soup.find('h1')
    if h1_tag:
        full_name_text = h1_tag.get_text(strip=True)

    # --- 2. Extract Overview ---
    about_text = ""
    about_div = soup.find('div', class_=lambda c: c and 'about' in c.split())
    if about_div:
        about_text = about_div.get_text(separator=' ', strip=True)
        
    keypoints_text = ""
    commentary_div = soup.find('div', class_=lambda c: c and 'commentary' in c.split())
    if commentary_div:
        # Avoid including hidden "show more" buttons text by selecting inner p tags or just cleaning
        keypoints_text = commentary_div.get_text(separator=' ', strip=True)

    overview_parts = []
    if about_text:
        overview_parts.append(f"ABOUT: {about_text}")
    if keypoints_text:
        overview_parts.append(f"KEY POINTS: {keypoints_text}")
        
    combined_overview = " | ".join(overview_parts)
    cleaned_overview = clean_text(combined_overview)
    
    return cmp_val, full_name_text, cleaned_overview

def update_watchlist():
    if not os.path.exists(WATCHLIST_CSV):
        print(f"{WATCHLIST_CSV} not found!")
        return
        
    rows = []
    with open(WATCHLIST_CSV, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
            
    session = make_session()
    
    for row in rows:
        symbol = row.get('symbol', '').strip()
        if not symbol:
            continue
            
        # Get existing values
        company_name = row.get('company_name', '').strip()
        overview_curr = row.get('overview', '').strip()
        
        # Scrape fresh data (required for latest CMP)
        cmp_val, full_name, overview = scrape_stock_info(session, symbol)
        
        if cmp_val is not None:
            # Update CMP every time
            row['cmp'] = str(cmp_val)
            print(f"Updated {symbol} CMP to {cmp_val}")
            
        if overview and not overview_curr:
            row['overview'] = overview
        if full_name and not company_name:
            row['company_name'] = full_name
            
    # Write back to CSV
    fieldnames = ['id', 'sector_id', 'symbol', 'company_name', 'aliases', 'cmp', 'overview']
    with open(WATCHLIST_CSV, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
        
    print(f"\nSuccessfully updated {WATCHLIST_CSV}!")

if __name__ == "__main__":
    update_watchlist()
