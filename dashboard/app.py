import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date, timedelta
import os
import io
from dotenv import load_dotenv

load_dotenv()

# Page config
st.set_page_config(
    page_title="Financial Data Pipeline Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern design
st.markdown("""
    <style>
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }
    
    .stApp {
        background: linear-gradient(135deg, #0f172a 0%, #1a2f5a 100%);
        color: #e2e8f0;
    }
    
    .main {
        padding: 2rem;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        border-left: 4px solid #3b82f6;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        transition: all 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 12px rgba(0, 0, 0, 0.5);
    }
    
    .metric-card.warning {
        border-left-color: #f59e0b;
    }
    
    .metric-card.success {
        border-left-color: #10b981;
    }
    
    .metric-card.error {
        border-left-color: #ef4444;
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #fff;
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 0.875rem;
        color: #94a3b8;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .metric-change {
        font-size: 0.875rem;
        margin-top: 0.5rem;
    }
    
    .metric-change.positive {
        color: #10b981;
    }
    
    .metric-change.negative {
        color: #ef4444;
    }
    
    .section-header {
        font-size: 1.5rem;
        font-weight: 700;
        color: #fff;
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.75rem;
        border-bottom: 2px solid #3b82f6;
    }
    
    .status-badge {
        display: inline-block;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.875rem;
        font-weight: 600;
    }
    
    .status-success {
        background-color: rgba(16, 185, 129, 0.2);
        color: #10b981;
        border: 1px solid #10b981;
    }
    
    .status-warning {
        background-color: rgba(245, 158, 11, 0.2);
        color: #f59e0b;
        border: 1px solid #f59e0b;
    }
    
    .status-error {
        background-color: rgba(239, 68, 68, 0.2);
        color: #ef4444;
        border: 1px solid #ef4444;
    }
    
    .status-idle {
        background-color: rgba(148, 163, 184, 0.2);
        color: #94a3b8;
        border: 1px solid #94a3b8;
    }
    
    .stTabs [data-baseweb="tab-list"] button {
        background-color: transparent;
        color: #94a3b8;
        border-radius: 8px;
        border: none;
        padding: 0.75rem 1.5rem;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab-list"] button:hover {
        background-color: rgba(59, 130, 246, 0.2);
        color: #3b82f6;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: rgba(59, 130, 246, 0.2);
        color: #3b82f6;
    }
    
    .header-container {
        text-align: center;
        padding: 2rem 0;
        margin-bottom: 2rem;
    }
    
    .main-title {
        font-size: 3rem;
        font-weight: 700;
        background: linear-gradient(135deg, #3b82f6 0%, #8b5cf6 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin: 0;
    }
    
    .subtitle {
        font-size: 1rem;
        color: #64748b;
        margin-top: 0.5rem;
    }
    
    .data-table {
        background-color: rgba(30, 41, 59, 0.8);
        border-radius: 8px;
    }
    </style>
    """, unsafe_allow_html=True)

# Sidebar Navigation
with st.sidebar:
    st.markdown("### 📱 Navigation")
    page = st.radio("", ["Overview", "Data Explorer", "DAG Runs", "Data Quality", "Alerts"], label_visibility="collapsed")
    
    st.markdown("---")
    st.markdown("### ⚙️ Settings")
    auto_refresh = st.checkbox("Auto-refresh (every 60s)", value=True)
    refresh_interval = st.slider("Refresh interval (seconds)", 30, 300, 60) if auto_refresh else 60
    
    st.markdown("---")
    st.markdown("### 📊 About")
    st.caption("Financial Data Pipeline Dashboard v1.0")
    st.caption("Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

@st.cache_resource
def get_db_connection():
    # Use environment variables with fallbacks
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'localhost'),
        port=os.getenv("POSTGRES_PORT", 5433), 
        database=os.getenv("POSTGRES_DB", "market_data"),
        user=os.getenv("POSTGRES_USER", 'postgres'),
        password=os.getenv("POSTGRES_PASSWORD")
    )

@st.cache_data(ttl=60)
def fetch_metrics():
    try:
        conn = get_db_connection()
        
        # Today's record count
        query_today = "SELECT COUNT(*) FROM ohclv_data WHERE date = %s"
        df_today = pd.read_sql(query_today, conn, params=(date.today(),))
        record_count = df_today.iloc[0, 0]

        # Weekly data
        week_ago = date.today() - timedelta(days=7)
        query_weekly = """
            SELECT date, COUNT(*) as count
            FROM ohclv_data
            WHERE date >= %s
            GROUP BY date
            ORDER BY date
            """
        weekly_data = pd.read_sql(query_weekly, conn, params=(week_ago,))
        
        return record_count, weekly_data
    except Exception as e:
        st.error(f"Database error: {e}")
        return 0, pd.DataFrame()

def render_metric_card(label, value, change, change_type="positive", badge_color="success"):
    """Render a styled metric card"""
    badge_class = f"status-{badge_color}"
    change_class = f"metric-change {'positive' if change_type == 'positive' else 'negative'}"
    change_arrow = "↑" if change_type == "positive" else "↓"
    
    st.markdown(f"""
    <div class="metric-card {badge_color}">
        <div class="metric-label">{label}</div>
        <div class="metric-value">{value}</div>
        <div class="{change_class}">{change_arrow} {change}</div>
    </div>
    """, unsafe_allow_html=True)

def format_status_badge(status):
    """Format status as HTML badge"""
    status_map = {
        'success': ('✅ Success', 'status-success'),
        'warning': ('⚠️ Warning', 'status-warning'),
        'error': ('❌ Failed', 'status-error'),
        'idle': ('⏸️ Idle', 'status-idle'),
    }
    
    text, css_class = status_map.get(status, (status, 'status-idle'))
    return f'<span class="status-badge {css_class}">{text}</span>'

# ============================================================================
# DATA EXPLORER FUNCTIONS
# ============================================================================

@st.cache_data(ttl=60)
def get_table_stats():
    """Get record counts for all tables"""
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        stats = {}
        tables = ['ohclv_data', 'options_data', 'dividends', 'stock_splits', 'economic_indicators']
        
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cur.fetchone()[0]
        
        cur.close()
        return stats
    except Exception as e:
        st.error(f"Error fetching table stats: {e}")
        return {}

@st.cache_data(ttl=60)
def fetch_ohclv_data(ticker=None, limit=1000):
    """Fetch OHLCV data with optional ticker filter"""
    try:
        conn = get_db_connection()
        
        if ticker:
            query = f"""
                SELECT ticker, date, open, high, low, close, volume 
                FROM ohclv_data 
                WHERE ticker = %s
                ORDER BY date DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn, params=(ticker.upper(),))
        else:
            query = f"""
                SELECT ticker, date, open, high, low, close, volume 
                FROM ohclv_data 
                ORDER BY date DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn)
        
        return df
    except Exception as e:
        st.error(f"Error fetching OHLCV data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_dividends_data(ticker=None, limit=1000):
    """Fetch dividends data"""
    try:
        conn = get_db_connection()
        
        if ticker:
            query = f"""
                SELECT ticker, ex_date, payment_date, amount, dividend_type 
                FROM dividends 
                WHERE ticker = %s
                ORDER BY ex_date DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn, params=(ticker.upper(),))
        else:
            query = f"""
                SELECT ticker, ex_date, payment_date, amount, dividend_type 
                FROM dividends 
                ORDER BY ex_date DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn)
        
        return df
    except Exception as e:
        st.error(f"Error fetching dividends data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_splits_data(ticker=None, limit=1000):
    """Fetch stock splits data"""
    try:
        conn = get_db_connection()
        
        if ticker:
            query = f"""
                SELECT ticker, ex_date, split_ratio 
                FROM stock_splits 
                WHERE ticker = %s
                ORDER BY ex_date DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn, params=(ticker.upper(),))
        else:
            query = f"""
                SELECT ticker, ex_date, split_ratio 
                FROM stock_splits 
                ORDER BY ex_date DESC 
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn)
        
        return df
    except Exception as e:
        st.error(f"Error fetching splits data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_economic_data(limit=1000):
    """Fetch economic indicators data"""
    try:
        conn = get_db_connection()
        
        query = f"""
            SELECT indicator_code, indicator_name, date, value 
            FROM economic_indicators 
            ORDER BY date DESC 
            LIMIT {limit}
        """
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error fetching economic data: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=60)
def fetch_options_data(ticker=None, limit=1000):
    """Fetch options data"""
    try:
        conn = get_db_connection()
        
        if ticker:
            query = f"""
                SELECT ticker, expiration, strike, option_type, bid, ask, implied_volatility, volume 
                FROM options_data 
                WHERE ticker = %s
                ORDER BY expiration DESC, strike ASC
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn, params=(ticker.upper(),))
        else:
            query = f"""
                SELECT ticker, expiration, strike, option_type, bid, ask, implied_volatility, volume 
                FROM options_data 
                ORDER BY expiration DESC, strike ASC
                LIMIT {limit}
            """
            df = pd.read_sql(query, conn)
        
        return df
    except Exception as e:
        st.error(f"Error fetching options data: {e}")
        return pd.DataFrame()

def show_data_explorer():
    """Render Data Explorer page"""
    st.markdown('<div class="header-container">', unsafe_allow_html=True)
    st.markdown('<h1 class="main-title">🔍 Data Explorer</h1>', unsafe_allow_html=True)
    st.markdown('<p class="subtitle">Browse and query extracted financial data</p>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Get table stats
    stats = get_table_stats()
    
    # Display table statistics
    st.markdown('<div class="section-header">Database Statistics</div>', unsafe_allow_html=True)
    
    stat_col1, stat_col2, stat_col3, stat_col4, stat_col5 = st.columns(5)
    
    with stat_col1:
        st.metric("OHLCV Records", f"{stats.get('ohclv_data', 0):,}")
    
    with stat_col2:
        st.metric("Options Records", f"{stats.get('options_data', 0):,}")
    
    with stat_col3:
        st.metric("Dividends", f"{stats.get('dividends', 0):,}")
    
    with stat_col4:
        st.metric("Stock Splits", f"{stats.get('stock_splits', 0):,}")
    
    with stat_col5:
        st.metric("Economic Indicators", f"{stats.get('economic_indicators', 0):,}")
    
    st.markdown("---")
    
    # Create tabs for different data types
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 OHLCV", "💰 Dividends", "📊 Splits", "🌍 Economics", "📉 Options"])
    
    # ========== OHLCV DATA TAB ==========
    with tab1:
        st.markdown("### OHLCV (Price) Data")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            ticker_input = st.text_input("Filter by ticker (leave empty for all)", placeholder="e.g., AAPL")
        
        with col2:
            limit = st.number_input("Rows to display", 10, 10000, 100)
        
        if st.button("🔄 Fetch OHLCV Data", key="btn_ohlcv"):
            df = fetch_ohclv_data(ticker=ticker_input if ticker_input else None, limit=limit)
            
            if not df.empty:
                st.success(f"✅ Loaded {len(df)} records")
                st.dataframe(df, use_container_width=True, hide_index=True)
                
                # Export button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"ohlcv_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No data found. Try a different ticker or check the database.")
    
    # ========== DIVIDENDS TAB ==========
    with tab2:
        st.markdown("### Dividend Payments")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            div_ticker = st.text_input("Filter by ticker (leave empty for all)", placeholder="e.g., MSFT", key="div_ticker")
        
        with col2:
            div_limit = st.number_input("Rows to display", 10, 10000, 100, key="div_limit")
        
        if st.button("🔄 Fetch Dividends", key="btn_div"):
            df = fetch_dividends_data(ticker=div_ticker if div_ticker else None, limit=div_limit)
            
            if not df.empty:
                st.success(f"✅ Loaded {len(df)} dividend records")
                st.dataframe(df, use_container_width=True, hide_index=True)
                
                # Summary stats
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Total Amount", f"${df['amount'].sum():,.2f}")
                with col_b:
                    st.metric("Avg Dividend", f"${df['amount'].mean():.2f}")
                with col_c:
                    st.metric("Unique Tickers", df['ticker'].nunique())
                
                # Export
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"dividends_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No dividend data found.")
    
    # ========== SPLITS TAB ==========
    with tab3:
        st.markdown("### Stock Splits")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            split_ticker = st.text_input("Filter by ticker (leave empty for all)", placeholder="e.g., NVDA", key="split_ticker")
        
        with col2:
            split_limit = st.number_input("Rows to display", 10, 10000, 100, key="split_limit")
        
        if st.button("🔄 Fetch Splits", key="btn_split"):
            df = fetch_splits_data(ticker=split_ticker if split_ticker else None, limit=split_limit)
            
            if not df.empty:
                st.success(f"✅ Loaded {len(df)} split records")
                st.dataframe(df, use_container_width=True, hide_index=True)
                
                # Summary
                col_a, col_b = st.columns(2)
                with col_a:
                    st.metric("Total Splits", len(df))
                with col_b:
                    st.metric("Unique Tickers", df['ticker'].nunique())
                
                # Export
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"splits_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No split data found.")
    
    # ========== ECONOMIC TAB ==========
    with tab4:
        st.markdown("### Economic Indicators")
        
        econ_limit = st.number_input("Rows to display", 10, 10000, 500, key="econ_limit")
        
        if st.button("🔄 Fetch Economic Data", key="btn_econ"):
            df = fetch_economic_data(limit=econ_limit)
            
            if not df.empty:
                st.success(f"✅ Loaded {len(df)} economic records")
                
                # Filter by indicator
                indicators = df['indicator_code'].unique()
                selected_indicator = st.selectbox("Filter by indicator", ["All"] + list(indicators))
                
                if selected_indicator != "All":
                    df = df[df['indicator_code'] == selected_indicator]
                
                st.dataframe(df, use_container_width=True, hide_index=True)
                
                # Summary
                col_a, col_b = st.columns(2)
                with col_a:
                    st.metric("Indicators", df['indicator_code'].nunique())
                with col_b:
                    st.metric("Date Range", f"{df['date'].min()} to {df['date'].max()}")
                
                # Export
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"economic_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No economic data found.")
    
    # ========== OPTIONS TAB ==========
    with tab5:
        st.markdown("### Options Data")
        
        col1, col2 = st.columns([3, 1])
        with col1:
            opt_ticker = st.text_input("Filter by ticker (leave empty for all)", placeholder="e.g., TSLA", key="opt_ticker")
        
        with col2:
            opt_limit = st.number_input("Rows to display", 10, 10000, 500, key="opt_limit")
        
        if st.button("🔄 Fetch Options", key="btn_opt"):
            df = fetch_options_data(ticker=opt_ticker if opt_ticker else None, limit=opt_limit)
            
            if not df.empty:
                st.success(f"✅ Loaded {len(df)} options records")
                st.dataframe(df, use_container_width=True, hide_index=True)
                
                # Summary
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Unique Tickers", df['ticker'].nunique())
                with col_b:
                    st.metric("Expirations", df['expiration'].nunique())
                with col_c:
                    st.metric("Avg IV", f"{df['implied_volatility'].mean():.2%}")
                
                # Export
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"options_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No options data found.")

# Header
st.markdown("""
<div class="header-container">
    <h1 class="main-title">📊 Financial Data Pipeline</h1>
    <p class="subtitle">Real-time data quality monitoring & DAG orchestration dashboard</p>
</div>
""", unsafe_allow_html=True)

# Fetch data
record_count, weekly_data = fetch_metrics()

# Navigation routing
if page == "Data Explorer":
    show_data_explorer()

elif page == "Overview":
    # Top Metrics Row
    st.markdown('<div class="section-header">Key Metrics</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        render_metric_card("Reliability", "99.6%", "0.1%", "positive", "success")
    
    with col2:
        render_metric_card("Data Freshness", "8 mins", "2 mins", "negative", "warning")
    
    with col3:
        render_metric_card("Active DAGs", "4/4", "0", "positive", "success")
    
    with col4:
        render_metric_card("Records Today", f"{record_count:,}", "23 records", "positive", "success")
    
    with col5:
        render_metric_card("Warnings", "3", "1", "negative", "warning")
    
    st.markdown("---")
    
    # Charts Section
    st.markdown('<div class="section-header">Performance Analytics</div>', unsafe_allow_html=True)
    
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.markdown("#### 📈 Weekly Collection Trend")
        if not weekly_data.empty:
            fig = px.line(
                weekly_data, 
                x='date', 
                y='count', 
                markers=True,
                title=None,
                labels={'count': 'Records Collected', 'date': 'Date'}
            )
            fig.update_trace(
                line=dict(color='#3b82f6', width=3),
                marker=dict(size=10, color='#8b5cf6')
            )
            fig.update_layout(
                plot_bgcolor='rgba(0,0,0,0.1)',
                paper_bgcolor='rgba(0,0,0,0)',
                font_color='#e2e8f0',
                font_size=12,
                xaxis=dict(showgrid=False, title_text='Date'),
                yaxis=dict(showgrid=True, gridcolor='#334155', title_text='Record Count'),
                height=400,
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("📭 No data available for the last 7 days.")
    
    with chart_col2:
        st.markdown("#### 🎯 Data Quality Score")
        labels = ['Excellent', 'Good', 'Warning', 'Failed']
        values = [92, 6, 1.5, 0.5]
        colors = ['#10b981', '#3b82f6', '#f59e0b', '#ef4444']
        
        fig_pie = go.Figure(data=[go.Pie(
            labels=labels, 
            values=values, 
            hole=0.5, 
            marker_colors=colors,
            textposition='inside',
            textinfo='label+percent'
        )])
        fig_pie.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='#e2e8f0',
            font_size=12,
            height=400,
            showlegend=True,
            legend=dict(x=-0.2, y=0.5)
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    
    st.markdown("---")
    
    # DAG Status Overview
    st.markdown('<div class="section-header">DAG Execution Status</div>', unsafe_allow_html=True)
    
    dag_runs = pd.DataFrame([
        {'DAG': 'market_data_collection', 'Status': 'success', 'Duration': '3m 24s', 'Last Run': '16:15', 'Records': '125,430'},
        {'DAG': 'options_data_collection', 'Status': 'success', 'Duration': '5m 12s', 'Last Run': '16:00', 'Records': '89,120'},
        {'DAG': 'corporate_actions_collection', 'Status': 'success', 'Duration': '1m 45s', 'Last Run': '18:00', 'Records': '1,240'},
        {'DAG': 'economic_indicators_collection', 'Status': 'idle', 'Duration': '-', 'Last Run': 'Mon 10:00', 'Records': '156'},
    ])
    
    # Format status column
    dag_runs['Status'] = dag_runs['Status'].apply(format_status_badge)
    
    # Display as HTML table
    st.markdown(dag_runs.to_html(escape=False, index=False), unsafe_allow_html=True)
    
    # Quick Actions
    st.markdown("---")
    st.markdown('<div class="section-header">Quick Actions</div>', unsafe_allow_html=True)
    
    action_col1, action_col2, action_col3, action_col4 = st.columns(4)
    
    with action_col1:
        if st.button("🔄 Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    with action_col2:
        if st.button("📥 Export Metrics", use_container_width=True):
            st.info("Export feature coming soon!")
    
    with action_col3:
        if st.button("⚙️ Configure", use_container_width=True):
            st.info("Configuration panel coming soon!")
    
    with action_col4:
        if st.button("📞 Support", use_container_width=True):
            st.info("Support documentation available in README.md")

elif page == "DAG Runs":
    st.markdown('<div class="section-header">Recent DAG Executions</div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["All", "Success", "Warnings"])
    
    dag_runs_detail = pd.DataFrame([
        {'DAG': 'market_data_collection', 'Status': '✅ Success', 'Started': '16:15:00', 'Duration': '3m 24s', 'Tasks': '4/4', 'Errors': '0'},
        {'DAG': 'options_data_collection', 'Status': '✅ Success', 'Started': '16:00:00', 'Duration': '5m 12s', 'Tasks': '3/3', 'Errors': '0'},
        {'DAG': 'corporate_actions_collection', 'Status': '⚠️ Warning', 'Started': '18:00:00', 'Duration': '1m 45s', 'Tasks': '3/3', 'Errors': '1'},
        {'DAG': 'economic_indicators_collection', 'Status': '⏸️ Idle', 'Started': 'Mon 10:00', 'Duration': '-', 'Tasks': '-', 'Errors': '-'},
    ])
    
    with tab1:
        st.dataframe(dag_runs_detail, use_container_width=True, hide_index=True)
    
    with tab2:
        success_runs = dag_runs_detail[dag_runs_detail['Status'] == '✅ Success']
        st.dataframe(success_runs, use_container_width=True, hide_index=True)
    
    with tab3:
        warning_runs = dag_runs_detail[dag_runs_detail['Status'].str.contains('⚠️|❌')]
        st.dataframe(warning_runs, use_container_width=True, hide_index=True)

elif page == "Data Quality":
    st.markdown('<div class="section-header">Data Quality Metrics</div>', unsafe_allow_html=True)
    
    quality_col1, quality_col2, quality_col3 = st.columns(3)
    
    with quality_col1:
        st.markdown("### 🎯 Completeness")
        st.metric("Missing Values", "0.2%", "-0.1%")
    
    with quality_col2:
        st.markdown("### ✔️ Validity")
        st.metric("Invalid Records", "12", "-5")
    
    with quality_col3:
        st.markdown("### ⏱️ Timeliness")
        st.metric("Avg Latency", "234ms", "+12ms")
    
    st.markdown("---")
    st.markdown("### 📋 Quality Rule Violations")
    
    violations = pd.DataFrame([
        {'Rule': 'Price Range Validation', 'Violations': '3', 'Severity': '🟡 Warning', 'Last Triggered': '2h ago'},
        {'Rule': 'Volume Threshold', 'Violations': '1', 'Severity': '🔴 Critical', 'Last Triggered': '30m ago'},
        {'Rule': 'Duplicate Detection', 'Violations': '0', 'Severity': '🟢 OK', 'Last Triggered': 'Never'},
    ])
    
    st.dataframe(violations, use_container_width=True, hide_index=True)

elif page == "Alerts":
    st.markdown('<div class="section-header">System Alerts & Notifications</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    
    with col2:
        alert_filter = st.selectbox("Filter by", ["All", "Critical", "Warning", "Info"])
    
    st.markdown("---")
    
    # Alert examples
    alerts = [
        ("🔴 CRITICAL", "High latency detected in market_data_collection", "2 mins ago", "Market Data"),
        ("🟡 WARNING", "Corporate actions DAG showed slower performance", "15 mins ago", "Corporate Data"),
        ("🔵 INFO", "Data quality score improved to 99.6%", "1 hour ago", "Quality"),
        ("🟢 SUCCESS", "All DAGs executed successfully in last cycle", "3 hours ago", "Pipeline"),
    ]
    
    for severity, message, time, source in alerts:
        with st.container():
            col1, col2, col3 = st.columns([1, 3, 1])
            with col1:
                st.markdown(f"**{severity}**")
            with col2:
                st.markdown(f"{message}")
            with col3:
                st.caption(f"{time} • {source}")
            st.markdown("---")