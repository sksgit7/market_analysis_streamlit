import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import os
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()

def get_secret(key_name):
    """
    Safely retrieves secrets bridging local and Streamlit Cloud environments.
    1. Checks standard OS environment (handles local .env)
    2. Falls back to direct st.secrets dictionary (handles Streamlit Cloud)
    """
    val = os.getenv(key_name)
    if val:
        return val
    try:
        return st.secrets[key_name]
    except (KeyError, FileNotFoundError):
        print(f"Secret {key_name} not found")
        with open(APP_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(f"Secret {key_name} not found")
        return None

st.set_page_config(page_title="Stock Market Updates", layout="wide")

# Paths corresponding to project structure
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, 'input')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')

WATCHLIST_CSV = os.path.join(INPUT_DIR, 'watchlist.csv')
MARKET_SECTORS_CSV = os.path.join(INPUT_DIR, 'market_sectors.csv')
TRACKED_METRICS_CSV = os.path.join(INPUT_DIR, 'tracked_metrics.csv')
MESSAGES_CSV = os.path.join(OUTPUT_DIR, 'messages.csv')
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
APP_LOG_FILE = os.path.join(LOG_DIR, 'app_activity.log')

def log_button_click(button_name):
    from datetime import datetime, timedelta, timezone
    ist_time = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
    msg = f"[BUTTON CLICK] '{button_name}' triggered at {ist_time.strftime('%Y-%m-%d %H:%M:%S')} IST\n"
    print(msg.strip())
    with open(APP_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(msg)

def run_script_and_log(args_list):
    # Prepare environment for subprocess, explicitly injecting st.secrets
    sub_env = os.environ.copy()
    try:
        for k, v in st.secrets.items():
            # In case st.secrets contains dicts, only copy top-level strings/primitives
            if isinstance(v, (str, int, float, bool)):
                sub_env[k] = str(v)
    except Exception:
        pass

    result = subprocess.run(args_list, cwd=BASE_DIR, capture_output=True, text=True, env=sub_env)
    script_name = os.path.basename(args_list[1]) if len(args_list) > 1 else "Unknown"
    
    output = f"\n--- EXECUTING: {script_name} ---\n"
    if result.stdout:
        output += result.stdout
    if result.stderr:
        output += f"\nERRORS:\n{result.stderr}"
    output += f"--- FINISHED: {script_name} ---\n\n"
    
    print(output.strip())
    with open(APP_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(output)

def load_sectors():
    if os.path.exists(MARKET_SECTORS_CSV):
        return pd.read_csv(MARKET_SECTORS_CSV, dtype=str)
    return pd.DataFrame(columns=['id', 'sector_name', 'description'])

def save_sectors(df):
    df = df.dropna(subset=['sector_name'])
    df = df[df['sector_name'].str.strip() != '']
    df['id'] = range(1, len(df) + 1)
    df = df.fillna('')
    df = df[['id', 'sector_name', 'description']]
    df.to_csv(MARKET_SECTORS_CSV, index=False, encoding='utf-8-sig')

def load_watchlist():
    if os.path.exists(WATCHLIST_CSV):
        return pd.read_csv(WATCHLIST_CSV, dtype=str)
    return pd.DataFrame(columns=['id', 'sector_id', 'symbol', 'company_name', 'aliases', 'cmp', 'overview'])

def save_watchlist(disp_df, sectors_df):
    name_to_id = dict(zip(sectors_df['sector_name'], sectors_df['id']))
    df = disp_df.copy()
    df['sector_id'] = df['Sector'].map(name_to_id)
    
    df = df.dropna(subset=['symbol'])
    df = df[df['symbol'].str.strip() != '']
    
    df['id'] = range(1, len(df) + 1)
    df = df.fillna('')
    
    df = df[['id', 'sector_id', 'symbol', 'company_name', 'aliases', 'cmp', 'overview']]
    df.to_csv(WATCHLIST_CSV, index=False, encoding='utf-8-sig')

def load_metrics():
    if os.path.exists(TRACKED_METRICS_CSV):
        return pd.read_csv(TRACKED_METRICS_CSV, dtype=str)
    return pd.DataFrame(columns=['id', 'company_id', 'metric_name', 'target_date'])

def save_metrics(disp_df, watchlist_df):
    symbol_to_id = dict(zip(watchlist_df['symbol'], watchlist_df['id']))
    df = disp_df.copy()
    df['company_id'] = df['Company Symbol'].map(symbol_to_id)
    
    df = df.dropna(subset=['metric_name'])
    df = df[df['metric_name'].str.strip() != '']
    
    df['id'] = range(1, len(df) + 1)
    df = df.fillna('')
    
    df = df[['id', 'company_id', 'metric_name', 'target_date']]
    df.to_csv(TRACKED_METRICS_CSV, index=False, encoding='utf-8-sig')

def check_login():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.title("🔒 Welcome to Stock Market Updates")
        st.markdown("Please log in to access the dashboard.")
        
        with st.form("login_form"):
            email = st.text_input("Email:")
            password = st.text_input("Password:", type="password")
            submit_button = st.form_submit_button("Log In")
            
            if submit_button:
                env_email = get_secret("APP_LOGIN_EMAIL")
                env_pass = get_secret("APP_LOGIN_PASSWORD")
                
                if env_email and env_pass and email == env_email and password == env_pass:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("Invalid email or password.")
                    
        st.stop() # Halt execution completely

def main():
    check_login()
    
    # Optional Top-Right Logout Button
    col_nav, col_logout = st.columns([0.9, 0.1])
    with col_logout:
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.authenticated = False
            st.rerun()
            
    # Streamlit horizontal navigation bar
    page = option_menu(
        menu_title=None,
        options=["Inputs", "Watchlist", "Sectors", "News"],
        icons=["gear", "graph-up-arrow", "pie-chart", "newspaper"],
        menu_icon="cast",
        default_index=0,
        orientation="horizontal",
    )

    if page == "Inputs":
        st.title("⚙️ Configuration Inputs")
        
        # Sub-page navigation wrapper for inputs
        if 'input_page' not in st.session_state:
            st.session_state.input_page = 'home'

        if st.session_state.input_page == 'home':
            st.markdown("Manage your stock watchlists, sector configurations, and metrics.")
            st.write("---")
            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                if st.button("📝 View & Edit Watchlist", width="stretch"):
                    st.session_state.input_page = 'edit_watchlist'
                    st.rerun()
                if st.button("📊 View & Edit Tracked Metrics", width="stretch"):
                    st.session_state.input_page = 'edit_metrics'
                    st.rerun()
                if st.button("🏭 View & Edit Market Sectors", width="stretch"):
                    st.session_state.input_page = 'edit_sectors'
                    st.rerun()
                    
                st.write("")
                if os.path.exists(APP_LOG_FILE):
                    with open(APP_LOG_FILE, "r", encoding='utf-8') as f:
                        log_data = f.read()
                    st.download_button(
                        label="📄 Download Activity Logs",
                        data=log_data,
                        file_name="app_activity.log",
                        mime="text/plain",
                        use_container_width=True
                    )

        elif st.session_state.input_page == 'edit_watchlist':
            if st.button("⬅️ Back to Inputs Home"):
                st.session_state.input_page = 'home'
                st.rerun()
                
            st.header("📝 Edit Watchlist")
            df = load_watchlist()
            sectors_df = load_sectors()
            
            id_to_name = dict(zip(sectors_df['id'], sectors_df['sector_name']))
            disp_df = df.copy()
            disp_df['Sector'] = disp_df['sector_id'].map(id_to_name)
            
            cols = ['Sector', 'symbol', 'company_name', 'aliases', 'cmp', 'overview']
            for c in cols:
                if c not in disp_df:
                    disp_df[c] = ""
            disp_df = disp_df[cols]

            sector_list = sorted(sectors_df['sector_name'].dropna().unique().tolist())
            st.info("Tip: Sector and symbol required. Others fetched automatically via the scraper.")

            edited_df = st.data_editor(
                disp_df, num_rows="dynamic", width="stretch", height=600,
                column_config={
                    "Sector": st.column_config.SelectboxColumn("Sector Name", options=sector_list, required=True),
                    "symbol": st.column_config.TextColumn("Symbol", required=True),
                    "company_name": "Company Name", "aliases": "Aliases", "cmp": "CMP", "overview": "Overview"
                }
            )

            st.write("---")
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("💾 Save Changes", type="primary"):
                    save_watchlist(edited_df, sectors_df)
                    st.success("Watchlist updated successfully!")
            with col2:
                if st.button("🔄 Update watchlist data"):
                    log_button_click("Update watchlist data")
                    with st.spinner("Scraping Screener.in..."):
                        script_path = os.path.join(BASE_DIR, 'scripts', 'update_watchlist.py')
                        run_script_and_log([os.sys.executable, script_path])
                    st.success("Data fetched! Reloading...")
                    st.rerun()

        elif st.session_state.input_page == 'edit_metrics':
            if st.button("⬅️ Back to Inputs Home"):
                st.session_state.input_page = 'home'
                st.rerun()
                
            st.header("📊 Edit Tracked Metrics")
            df = load_metrics()
            watchlist_df = load_watchlist()
            
            id_to_symbol = dict(zip(watchlist_df['id'], watchlist_df['symbol']))
            disp_df = df.copy()
            disp_df['Company Symbol'] = disp_df['company_id'].map(id_to_symbol)
            
            cols = ['Company Symbol', 'metric_name', 'target_date']
            for c in cols:
                if c not in disp_df:
                    disp_df[c] = ""
            disp_df = disp_df[cols]

            disp_df['target_date'] = pd.to_datetime(disp_df['target_date'], errors='coerce').dt.date
            symbol_list = sorted(watchlist_df['symbol'].dropna().unique().tolist())

            edited_df = st.data_editor(
                disp_df, num_rows="dynamic", width="stretch", height=600,
                column_config={
                    "Company Symbol": st.column_config.SelectboxColumn("Company Symbol", options=symbol_list, required=True),
                    "metric_name": st.column_config.TextColumn("Metric Name", required=True),
                    "target_date": st.column_config.DateColumn("Target Date", format="YYYY-MM-DD")
                }
            )

            st.write("---")
            if st.button("💾 Save Changes", type="primary"):
                if 'target_date' in edited_df:
                    edited_df['target_date'] = pd.to_datetime(edited_df['target_date'], errors='coerce').dt.strftime('%Y-%m-%d')
                    edited_df['target_date'] = edited_df['target_date'].fillna('')
                save_metrics(edited_df, watchlist_df)
                st.success("Tracked Metrics updated successfully!")

        elif st.session_state.input_page == 'edit_sectors':
            if st.button("⬅️ Back to Inputs Home"):
                st.session_state.input_page = 'home'
                st.rerun()
                
            st.header("🏭 Edit Market Sectors")
            df = load_sectors()
            
            disp_df = df.copy()
            cols = ['sector_name', 'description']
            for c in cols:
                if c not in disp_df:
                    disp_df[c] = ""
            disp_df = disp_df[cols]

            edited_df = st.data_editor(
                disp_df, num_rows="dynamic", width="stretch", height=600,
                column_config={
                    "sector_name": st.column_config.TextColumn("Sector Name", required=True),
                    "description": "Description"
                }
            )

            st.write("---")
            if st.button("💾 Save Changes", type="primary"):
                save_sectors(edited_df)
                st.success("Market Sectors updated successfully!")

    elif page == "Watchlist":
        st.title("📈 Watchlist Updates")
        st.markdown("Latest impact analysis of your tracked companies.")
        
        # --- Analysis Trigger ---
        # st.info("Trigger Analysis Pipeline")
        if st.button("🔍 Run Stock Analysis", type="primary"):
            log_button_click("Run Stock Analysis")
            with st.spinner("Analyzing messages with LLM..."):
                script_path = os.path.join(BASE_DIR, 'scripts', 'analyze_stocks.py')
                run_script_and_log([os.sys.executable, script_path])
            st.success("Analysis complete! Reports updated.")
            st.rerun()
        
        st.write("---")
        
        updates_file = os.path.join(OUTPUT_DIR, 'watchlist_stock_updates.csv')
        metrics_results_file = os.path.join(OUTPUT_DIR, 'stock_tracked_metrics.csv')
        metrics_config_file = os.path.join(INPUT_DIR, 'tracked_metrics.csv')
        watchlist_df = load_watchlist()
        
        if os.path.exists(updates_file) and not watchlist_df.empty:
            updates_df = pd.read_csv(updates_file, dtype=str)
            
            # Load Metrics Data
            metrics_results_df = pd.DataFrame()
            if os.path.exists(metrics_results_file):
                metrics_results_df = pd.read_csv(metrics_results_file, dtype=str)
            
            metrics_config_df = pd.DataFrame()
            if os.path.exists(metrics_config_file):
                metrics_config_df = pd.read_csv(metrics_config_file, dtype=str)
            
            # Map company_id to company_name
            id_to_name = dict(zip(watchlist_df['id'].astype(str), watchlist_df['company_name']))
            updates_df['company_name'] = updates_df['company_id'].map(id_to_name).fillna("Unknown Company")
            
            # Convert timestamp and sort
            updates_df['msg_datetime'] = pd.to_datetime(updates_df['msg_timestamp'], errors='coerce')
            # Sort: Company Alpha, then Latest Msg first
            updates_df = updates_df.sort_values(by=['company_name', 'msg_datetime'], ascending=[True, False])
            
            # Display by company
            for company, group in updates_df.groupby('company_name', sort=False):
                # Get Company details
                c_row = watchlist_df[watchlist_df['company_name'] == company]
                if not c_row.empty:
                    cmp_val = c_row['cmp'].iloc[0]
                    c_id = c_row['id'].iloc[0]
                else:
                    cmp_val = "N/A"
                    c_id = "0"

                # Subheader with CMP in side font
                st.markdown(f"### 🏢 {company} <span style='font-size: 16px; color: #888; font-weight: normal; margin-left: 10px;'>CMP: {cmp_val}</span>", unsafe_allow_html=True)
                
                # Metrics Button (Expander)
                with st.expander("📊 Metrics"):
                    c_metrics = metrics_results_df[metrics_results_df['company_id'] == str(c_id)]
                    if not c_metrics.empty:
                        # Group results by metric ID
                        for m_id, m_group in c_metrics.groupby('metric_id'):
                            # Find metric name
                            m_name_match = metrics_config_df[metrics_config_df['id'] == str(m_id)]
                            m_name = m_name_match['metric_name'].iloc[0] if not m_name_match.empty else f"Metric {m_id}"
                            
                            # Latest status for top icon
                            latest_row = m_group.sort_values('msg_timestamp', ascending=False).iloc[0]
                            status = str(latest_row['metric_status']).strip().lower()
                            
                            m_icon = "⚪"
                            if status == 'positive': m_icon = "🟢"
                            elif status == 'negative': m_icon = "🔴"
                            elif status == 'achieved': m_icon = "✅"
                            
                            st.markdown(f"**{m_name}** {m_icon}")
                            for _, m_row in m_group.sort_values('msg_timestamp', ascending=False).iterrows():
                                # Compact display for messages
                                st.markdown(f"<p style='margin-bottom: 2px; margin-top: -10px; font-size: 0.9em; padding-left: 15px;'>• {m_row['msg_summary']}</p>", unsafe_allow_html=True)
                            st.write("") # small gap between different metrics
                    else:
                        st.caption("No specific metric updates found for this company.")

                # Latest News Items
                for _, row in group.iterrows():
                    # Format timestamp
                    if pd.isna(row['msg_datetime']):
                        ts_str = "Unknown time"
                    else:
                        # Required format: dd-mm hh:mm
                        ts_str = row['msg_datetime'].strftime('%d-%m %H:%M')
                    
                    # Impact icon
                    impact = str(row.get('impact', '')).strip().lower()
                    if impact == 'positive':
                        icon = "🟢"
                    elif impact == 'negative':
                        icon = "🔴"
                    else:
                        icon = "⚪"
                        
                    msg_summary = str(row.get('msg_summary', ''))
                    
                    # UI Formatting: Timestamp on top, icons below
                    st.markdown(f"**{ts_str}**")
                    st.markdown(f"{icon} {msg_summary}")
                    st.write("") # Spacer
                
                st.write("---")
    elif page == "Sectors":
        st.title("🏭 Sector Insights")
        st.markdown("Macro and Sector-level analysis based on latest news.")
        
        # --- Analysis Triggers ---
        # st.info("Trigger Pipeline Tools")
        if st.button("🔍 Run Sector Analysis", type="primary"):
            log_button_click("Run Sector Analysis")
            with st.spinner("Step 1/2: Classifying messages to sectors..."):
                script_path = os.path.join(BASE_DIR, 'scripts', 'analyze_sectors.py')
                run_script_and_log([os.sys.executable, script_path])
            
            st.info("Step 1 complete. Pausing for 60 seconds to respect rate limits before summarizing...")
            time.sleep(60)
            
            with st.spinner("Step 2/2: Synthesizing executive summaries..."):
                script_path = os.path.join(BASE_DIR, 'scripts', 'summarize_sectors.py')
                run_script_and_log([os.sys.executable, script_path])
                
            st.success("Sector Pipeline complete!")
            st.rerun()

        st.write("---")
        
        # Data Files
        summary_file = os.path.join(OUTPUT_DIR, 'sector_summary.csv')
        updates_file = os.path.join(OUTPUT_DIR, 'sector_updates.csv')
        
        # Load Data
        df_summary = pd.read_csv(summary_file, dtype=str) if os.path.exists(summary_file) else pd.DataFrame()
        df_updates = pd.read_csv(updates_file, dtype=str) if os.path.exists(updates_file) else pd.DataFrame()
        
        if not df_summary.empty:
            # Handle Global Market (Sector ID 0) first at the top
            global_summary = df_summary[df_summary['sector_id'] == '0']
            if not global_summary.empty:
                row = global_summary.iloc[0]
                impact = str(row.get('impact', '')).strip().lower()
                icon = "🟢" if impact == 'positive' else "🔴"
                st.header("🌎 Global Market")
                st.markdown(f"{icon} *{row['summary']}*")
                
                # Global Updates in expander
                global_updates = df_updates[df_updates['sector_id'] == '0']
                if not global_updates.empty:
                    with st.expander("View Global Market Updates"):
                        for _, u_row in global_updates.sort_values('msg_timestamp', ascending=False).iterrows():
                            u_impact = str(u_row.get('impact', '')).strip().lower()
                            u_icon = "🟢" if u_impact == 'positive' else "🔴"
                            st.markdown(f"{u_icon} {u_row['msg_summary']}")
                st.write("---")

            # Other Sectors
            for _, row in df_summary.iterrows():
                s_id = str(row['sector_id'])
                if s_id == '0': continue # Already handled above
                
                s_name = str(row.get('sector_name', f"Sector {s_id}"))
                impact = str(row.get('impact', '')).strip().lower()
                icon = "🟢" if impact == 'positive' else "🔴"
                
                st.subheader(f"🏭 {s_name}")
                st.markdown(f"{icon} *{row['summary']}*")
                
                # Sector Specific Updates
                sec_updates = df_updates[df_updates['sector_id'] == s_id]
                if not sec_updates.empty:
                    with st.expander(f"Recent updates for {s_name}"):
                        for _, u_row in sec_updates.sort_values('msg_timestamp', ascending=False).iterrows():
                            u_impact = str(u_row.get('impact', '')).strip().lower()
                            u_icon = "🟢" if u_impact == 'positive' else "🔴"
                            st.markdown(f"{u_icon} {u_row['msg_summary']}")
                st.write("---")
        else:
            st.info("No sector summaries found. Run the triggers above to process sectoral news.")

    elif page == "News":
        st.title("📰 Telegram Market News")
        st.markdown("View all raw channel messages in the local pipeline.")

        # st.info("Run Pipeline Tools")
        col1, col2 = st.columns([1, 1])
        with col1:
            max_msg = st.number_input("Max Latest Messages", min_value=0, value=0, step=10, help="0 means append all unseen messages.")
        with col2:
            st.write("") # Adjust alignment for the button
            st.write("")
            if st.button("📥 Fetch New Messages", type="primary"):
                log_button_click("Fetch New Messages")
                with st.spinner("Calling Telegram API..."):
                    script_path = os.path.join(BASE_DIR, 'scripts', 'telegram_fetch_msg.py')
                    args = [os.sys.executable, script_path]
                    if max_msg > 0:
                        args.extend(['--max_latest_msg', str(int(max_msg))])
                    run_script_and_log(args)
                st.success("Fetched successfully! Refreshing...")
                st.rerun()
                
        st.write("---")
        st.info("Displaying contents of 'messages.csv'. Editing is disabled.")
        df_msgs = pd.DataFrame()
        if os.path.exists(MESSAGES_CSV):
            df_msgs = pd.read_csv(MESSAGES_CSV, dtype=str)
        
        st.dataframe(df_msgs, width="stretch", height=600)

if __name__ == "__main__":
    main()
