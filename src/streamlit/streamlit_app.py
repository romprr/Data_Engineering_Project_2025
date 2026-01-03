import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
from utils.db_clients import PGDriver

# SETUP AND STYLING

st.set_page_config(
    page_title="Geopolitics & Markets",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
    .main { 
        background-color: #f9f9f9; 
    }
    /* Dark Card Style for Metrics */
    div[data-testid="stMetric"] {
        background-color: #262730; /* Dark Charcoal */
        padding: 15px;
        border-radius: 8px;
        border: 1px solid #444;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    /* Metric Label (Top text) */
    div[data-testid="stMetric"] label {
        color: #a0a0a0 !important; /* Light Gray */
    }
    /* Metric Value (Big number) */
    div[data-testid="stMetric"] div[data-testid="stMetricValue"] {
        color: #ffffff !important; /* Pure White */
    }
    /* Metric Delta (Change text) */
    div[data-testid="stMetric"] div[data-testid="stMetricDelta"] {
        font-weight: bold;
    }
    h1, h2, h3 { 
        color: #2c3e50; 
    }
    </style>
""", unsafe_allow_html=True)

def load_query(filename):
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(current_dir, filename)
        with open(file_path, 'r') as file:
            return file.read()
    except FileNotFoundError:
        st.error(f"Error: Could not find '{filename}'.")
        return ""

QUERY_REGIONAL_SQL = load_query('Query1.sql')
QUERY_OVERVIEW_SQL = load_query('Query2.sql')
QUERY_US_ROLE_SQL = load_query('Query3.sql')

# DB CONN

def get_db_connection():
    try:
        db_user = os.environ.get('POSTGRES_USER', 'postgres')
        db_pass = os.environ.get('POSTGRES_PASSWORD', 'postgres')
        db_host = os.environ.get('POSTGRES_HOST', 'localhost')
        db_name = os.environ.get('POSTGRES_DB', 'postgres')
        
        driver = PGDriver(db_user, db_pass, db_host, db_name)
        return driver.connect()
    except Exception as e:
        return None

@st.cache_data(ttl=600)
def load_data(query_text):
    if not query_text.strip():
        return pd.DataFrame()
    
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            df = pd.read_sql(query_text, conn)
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        print(f"SQL Execution Error: {e}") 
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()

# SIDE BAR

st.sidebar.title("🔍 Analytics Hub")
st.sidebar.caption("Connected to Live Database")

st.sidebar.markdown("---")
st.sidebar.subheader("📊 Global Market Context")
df_global_context = load_data(QUERY_OVERVIEW_SQL)

if not df_global_context.empty:
    try:
        war_row = df_global_context[df_global_context['global_news_state'].str.contains("Fresh Conflict", na=False)]
        peace_row = df_global_context[df_global_context['global_news_state'].str.contains("Status Quo", na=False)]
        
        war_ret = war_row['average_return_pct'].values[0] if not war_row.empty else 0
        peace_ret = peace_row['average_return_pct'].values[0] if not peace_row.empty else 0
        
        st.sidebar.metric("Status Quo Avg", f"{peace_ret}%")
        st.sidebar.metric("Conflict Avg", f"{war_ret}%", f"{war_ret - peace_ret:.2f}% Impact", delta_color="inverse")
    except Exception:
        st.sidebar.info("Could not parse global context.")
else:
    st.sidebar.warning("Global data unavailable.")

st.sidebar.markdown("---")
page = st.sidebar.radio("Navigate to:", ["Overview: S&P 500", "Regional Analysis", "US Involvement Impact", "Raw Data Inspector"])

# MAIN LOGIC OF DASH

if page == "Overview: S&P 500":
    st.title("🌍 Global Stability & Market Returns")
    st.markdown("### How geopolitical conflict impacts the S&P 500")
    
    df_overview = df_global_context 

# PAGE 1

    if not df_overview.empty:
        war_row = df_overview[df_overview['global_news_state'].str.contains("Fresh Conflict", na=False)]
        peace_row = df_overview[df_overview['global_news_state'].str.contains("Status Quo", na=False)]

        war_val = war_row['average_return_pct'].values[0] if not war_row.empty else 0
        peace_val = peace_row['average_return_pct'].values[0] if not peace_row.empty else 0
        war_count = war_row['months_count'].values[0] if not war_row.empty else 0
        
        m1, m2, m3 = st.columns(3)
        with m1: st.metric("Baseline Return (Status Quo)", f"{peace_val}%") 
        with m2: st.metric("Conflict Return (New War)", f"{war_val}%", f"{war_val - peace_val:.2f}% Impact", delta_color="inverse")
        with m3: st.metric("Total Conflict Months", f"{war_count}", "Historical occurrences")
        
        st.markdown("---")

        c1, c2 = st.columns([1, 1.5])
        
        with c1:
            st.subheader("Frequency of Conflict")
            fig_pie = px.pie(
                df_overview, 
                values='months_count', 
                names='global_news_state',
                hole=0.6,
                color='global_news_state',
                color_discrete_map={
                    '1. Fresh Conflict Month (New War started)': '#ff4b4b',
                    '2. Status Quo Month (Ongoing Wars or Peace)': '#2ecc71'
                }
            )
            fig_pie.update_layout(legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5))
            st.plotly_chart(fig_pie, use_container_width=True)

        with c2:
            st.subheader("Market Reaction (Returns)")
            fig_bar = px.bar(
                df_overview, 
                y='global_news_state', 
                x='average_return_pct',
                color='global_news_state',
                orientation='h',
                text_auto=True,
                color_discrete_map={
                    '1. Fresh Conflict Month (New War started)': '#ff4b4b',
                    '2. Status Quo Month (Ongoing Wars or Peace)': '#2ecc71'
                }
            )
            fig_bar.update_layout(
                showlegend=False, 
                yaxis={'title': None, 'showticklabels': False}, 
                xaxis_title="Average Monthly Return (%)"
            )
            st.plotly_chart(fig_bar, use_container_width=True)

    else:
        st.error("Please run the airflow pipeline before running streamlit")

# PAGE 2

elif page == "Regional Analysis":
    st.title("📍 Regional Conflict Impact & Panic Levels")
    st.markdown("### The 'Panic' Evolution: Before, Start, and After")
    
    df_regional = load_data(QUERY_REGIONAL_SQL)
    
    if not df_regional.empty:
        if 'type_pct_change' in df_regional.columns:
            df_regional['numeric_change'] = (
                df_regional['type_pct_change']
                .astype(str)
                .str.replace('%', '', regex=False)
                .astype(float)
            )
        else:
            df_regional['numeric_change'] = 0

        if 'start_month_return' in df_regional.columns:
             df_regional['immediate_shock'] = (
                df_regional['start_month_return']
                .astype(str)
                .str.replace('%', '', regex=False)
                .astype(float)
            )
        else:
             df_regional['immediate_shock'] = 0

        df_regional['panic_score_before'] = df_regional.get('panic_score_before', 0).fillna(0)
        df_regional['panic_score_start'] = df_regional.get('panic_score_start', 0).fillna(0)
        df_regional['panic_score_after'] = df_regional.get('panic_score_after', 0).fillna(0)

        df_regional['panic_score_net_change'] = df_regional['panic_score_after'] - df_regional['panic_score_before']
        df_regional['panic_magnitude'] = df_regional['panic_score_net_change'].abs()

        best_row = df_regional.loc[df_regional['numeric_change'].idxmax()]
        crash_row = df_regional.loc[df_regional['immediate_shock'].idxmin()]
        most_volatile_row = df_regional.loc[df_regional['panic_score_net_change'].idxmax()]
        
        m1, m2, m3 = st.columns(3)
        with m1: 
            st.metric(
                "Best 3-Month Trend", 
                f"{best_row['numeric_change']:.2f}%", 
                f"{best_row['region']} - {best_row['asset_type']}"
            )
        with m2: 
            st.metric(
                "Deepest Initial Crash", 
                f"{crash_row['immediate_shock']:.2f}%", 
                f"{crash_row['region']} (Month 1 Return)",
                delta_color="inverse"
            )
        with m3:
            st.metric(
                "Highest Volatility Spike", 
                f"+{most_volatile_row['panic_score_net_change']:.2f}", 
                f"{most_volatile_row['region']} (Net Change)",
                delta_color="inverse"
            )

        st.markdown("---")
        
        st.subheader("🗺️ Global Conflict Monitor")
        st.caption("3D Globe view of market impact. Rotate to explore.")

        REGION_COORDS = {
            'North America': {'lat': 45.0, 'lon': -100.0},
            'South America': {'lat': -15.0, 'lon': -60.0},
            'Americas': {'lat': 20.0, 'lon': -90.0},
            'Europe': {'lat': 50.0, 'lon': 15.0},
            'Eastern Europe': {'lat': 50.0, 'lon': 30.0},
            'Western Europe': {'lat': 48.0, 'lon': 5.0},
            'Middle East': {'lat': 30.0, 'lon': 45.0},
            'Africa': {'lat': 5.0, 'lon': 20.0},
            'Asia': {'lat': 35.0, 'lon': 100.0},
            'East Asia': {'lat': 35.0, 'lon': 110.0},
            'South Asia': {'lat': 20.0, 'lon': 77.0},
            'Southeast Asia': {'lat': 10.0, 'lon': 105.0},
            'Oceania': {'lat': -25.0, 'lon': 135.0},
        }

        def get_lat_lon(region_name, kind):
            return REGION_COORDS.get(region_name, {'lat': 0, 'lon': 0})[kind]

        df_regional['lat'] = df_regional['region'].apply(lambda x: get_lat_lon(x, 'lat'))
        df_regional['lon'] = df_regional['region'].apply(lambda x: get_lat_lon(x, 'lon'))

        fig_map = px.scatter_geo(
            df_regional,
            lat='lat',
            lon='lon',
            color='immediate_shock',
            size='panic_magnitude',
            hover_name='region',
            hover_data={'lat': False, 'lon': False, 'numeric_change': True, 'panic_score_net_change': True},
            projection="orthographic", 
            color_continuous_scale="RdYlGn",
            size_max=25
        )
        
        fig_map.update_geos(
            bgcolor="rgba(0,0,0,0)",
            showocean=True, oceancolor="#0e1117", 
            showland=True, landcolor="#2c3e50",
            showcountries=True, countrycolor="#555555",
            showlakes=False,
            showcoastlines=False,
            projection_type="orthographic"
        )
        
        fig_map.update_layout(
            margin={"r":0,"t":10,"l":0,"b":0},
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=500
        )
        st.plotly_chart(fig_map, use_container_width=True)

        st.markdown("---")

        st.subheader("📈 Volatility Evolution per Episode")
        st.caption("Detailed breakdown: How stability changed during each specific historical conflict episode.")

        regions = df_regional['region'].unique()
        selected_region = st.selectbox("Select Region", regions)
        filtered_data = df_regional[df_regional['region'] == selected_region].copy()
        
        def get_category(asset_name):
            val_lower = str(asset_name).lower()
            if 'index' in val_lower or 'indices' in val_lower:
                return 'Index'
            return 'Futures'

        filtered_data['Category'] = filtered_data['asset_type'].apply(get_category)
        
        available_cats = sorted(filtered_data['Category'].unique())
        
        if available_cats:
            view_cat = st.radio("View Asset Class:", available_cats, horizontal=True, key="vol_cat_selector")
            
            asset_episode_data = filtered_data[filtered_data['Category'] == view_cat].copy()

            df_melted = asset_episode_data.melt(
                id_vars=['episode_start'], 
                value_vars=['panic_score_before', 'panic_score_start', 'panic_score_after'],
                var_name='Period', 
                value_name='Volatility'
            )
            
            df_melted['Period'] = df_melted['Period'].replace({
                'panic_score_before': '1. Before',
                'panic_score_start': '2. Start',
                'panic_score_after': '3. After'
            })
            
            df_melted['Episode'] = pd.to_datetime(df_melted['episode_start']).dt.strftime('%Y-%m-%d')

            if not df_melted.empty:
                fig_grouped = px.bar(
                    df_melted, 
                    x='Episode', 
                    y='Volatility', 
                    color='Period',
                    barmode='group', 
                    title=f"Volatility History: {view_cat} in {selected_region}",
                    labels={'Volatility': 'StdDev (%)', 'Episode': 'Conflict Start Date'},
                    color_discrete_map={
                        '1. Before': '#3498db', 
                        '2. Start': '#e74c3c',  
                        '3. After': '#f1c40f'   
                    }
                )
                st.plotly_chart(fig_grouped, use_container_width=True)
            else:
                st.info(f"No data for {view_cat} in {selected_region}")
        else:
            st.warning("No asset data available for this region.")

        st.markdown("---")

        st.subheader("⚠️ The Panic Map (Net Volatility Change)")
        st.markdown("""
        * **X-Axis:** Immediate Shock (Return in Month 1).
        * **Y-Axis:** Net Volatility Change (After - Before).
        * **Interpretation:** Top-Left = Crash + Long-term Instability.
        """)
        
        fig_scatter = px.scatter(
            df_regional,
            x='immediate_shock',
            y='panic_score_net_change',
            color='region',
            size='panic_magnitude',
            hover_data=['asset_type', 'assets_in_category', 'numeric_change', 'panic_score_before', 'panic_score_after'],
            title="Immediate Market Crash vs. Long-Term Volatility Shift",
            labels={
                'immediate_shock': 'Immediate Shock (Month 1 Return %)',
                'panic_score_net_change': 'Net Volatility Change (After - Before)',
                'region': 'Region'
            }
        )
        
        fig_scatter.add_hline(y=0, line_width=2, line_color="black", opacity=0.5)
        fig_scatter.add_vline(x=0, line_width=1, line_dash="dash", line_color="gray")
        
        st.plotly_chart(fig_scatter, use_container_width=True)

    else:
        st.error("Please run the airflow pipeline before running streamlit")

# PAGE 3

elif page == "US Involvement Impact":
    st.title("🇺🇸 The US Factor")
    
    df_us = load_data(QUERY_US_ROLE_SQL)

    if not df_us.empty:
        df_us['asset_type_clean'] = df_us['asset_type'].str.lower().str.strip()
        asset_class_view = st.radio("View Asset Class:", ["Index", "Forex"], horizontal=True)
        df_view = df_us[df_us['asset_type_clean'] == asset_class_view.lower()].copy()
        
        if not df_view.empty:
            df_view['returns_pct'] = df_view['monthly_returns'] * 100
            
            fig = px.bar(
                df_view, 
                x='name', 
                y='returns_pct', 
                color='max_conflict_role', 
                barmode='group',
                title=f"US {asset_class_view} Performance by Conflict Role",
                labels={'returns_pct': 'Avg Monthly Return (%)', 'name': 'Instrument', 'max_conflict_role': 'Role'},
                color_discrete_sequence=px.colors.qualitative.Bold
            )
            st.plotly_chart(fig, use_container_width=True)
            
            with st.expander("View Raw Data"):
                st.dataframe(df_view[['name', 'max_conflict_role', 'monthly_returns', 'returns_pct']])
        else:
            st.info(f"No data found for asset class: {asset_class_view}")
    else:
        st.error("Please run the airflow pipeline before running streamlit")

# PAGE 4

elif page == "Raw Data Inspector":
    st.title("🗄️ Raw Data Inspector")
    st.markdown("Explore the underlying datasets generated by the SQL queries.")

    tab1, tab2, tab3 = st.tabs(["Regional Analysis (Query 1)", "Global Overview (Query 2)", "US Involvement (Query 3)"])

    with tab1:
        st.subheader("Regional Conflict Impact Data")
        df_1 = load_data(QUERY_REGIONAL_SQL)
        if not df_1.empty:
            st.dataframe(df_1, use_container_width=True)
        else:
            st.warning("No data returned for Query 1.")

    with tab2:
        st.subheader("Global S&P 500 Overview Data")
        df_2 = load_data(QUERY_OVERVIEW_SQL)
        if not df_2.empty:
            st.dataframe(df_2, use_container_width=True)
        else:
            st.warning("No data returned for Query 2.")

    with tab3:
        st.subheader("US Involvement Data")
        df_3 = load_data(QUERY_US_ROLE_SQL)
        if not df_3.empty:
            st.dataframe(df_3, use_container_width=True)
        else:
            st.warning("No data returned for Query 3.")