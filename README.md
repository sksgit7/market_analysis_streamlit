# Telegram Market Sentiment Pipeline

This project automates the extraction, analysis, and summarization of financial market sentiment from Telegram channels. It uses a multi-tier approach to process raw data into executive-level sectoral summaries.

## Directory Structure
- **/scripts**: Core Python logic.
- **/input**: Static and raw data sources.
- **/output**: Generated analytical reports and state tracking.

## Execution Workflow (Order of Operations)

### 1. Update Watchlist
**Script:** `scripts/update_watchlist.py`
- **Input:** `input/watchlist.csv` (Reads existing symbols).
- **Process:** Scrapes Screener.in to update current market price (CMP) and company metadata.
- **Output:** `input/watchlist.csv` (Updated file).

### 2. Fetch Telegram Messages
**Script:** `scripts/telegram_fetch_msg.py`
- **Input:** `.env` (Telegram API credentials).
- **Process:** Connects to Telegram and fetches messages newer than the `last_id` stored in `state.json`.
- **Output:** `output/messages.csv` (Appends new raw messages).

### 3. Analyze Company Sentiment
**Script:** `scripts/analyze_stocks.py`
- **Inputs:** `output/messages.csv`, `input/watchlist.csv`.
- **Process:** Uses LLM to identify news related to watchlist companies and determine impact.
- **Output:** `output/watchlist_stock_updates.csv` (Appends results).
- **State:** `config/analyze_stocks_state.json` (Tracks last processed message ID).

### 4. Analyze Sector Trends
**Script:** `scripts/analyze_sectors.py`
- **Inputs:** `output/messages.csv`, `input/market_sectors.csv`.
- **Process:** Classifies sector-wide trends and commodities news.
- **Output:** `output/sector_updates.csv` (Appends granular news).
- **State:** `config/analyze_sectors_state.json` (Tracks last processed message ID).

### 5. Generate Executive Summary
**Script:** `scripts/summarize_sectors.py`
- **Input:** `output/sector_updates.csv`.
- **Process:** Uses high-reasoning LLM (70B) to aggregate all recent news for each sector into a 2-line summary and net direction.
- **Output:** `output/sector_summary.csv` (Appends historical aggregation records).

---

## Technical Notes
- **Stateful Processing**: Scripts 3 and 4 only process messages newer than their last run to save API costs. To force a full re-analysis, delete the `.json` files in the `output/` folder.
- **LLM Models**: We use `llama-3.1-8b` for high-volume classification and `llama-3.3-70b` for deductive executive summarization.
