import streamlit as st
import pandas as pd
import plotly.express as px
import psycopg2
from datetime import datetime, timedelta

from helper.config import load_config

# Page configuration
st.set_page_config(page_title="Screentime Analytics", layout="wide")
st.title("📱 Screentime Analytics")

# Load data from database
@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_screentime_data():
    config = load_config()
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    to_timestamp(start_time+tz) AT TIME ZONE 'UTC' as start_time,
                    to_timestamp(end_time+tz) AT TIME ZONE 'UTC' as end_time,
                    app,
                    device_id,
                    device_model,
                    record_type,
                    usage_time
                FROM apple.fact_screentime
                ORDER BY start_time DESC
            """)
            df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df

# Add refresh button in sidebar
with st.sidebar:
    if st.button("🔄 Refresh Data", help="Clear cache and reload from database"):
        st.cache_data.clear()
        st.rerun()

# Load and prepare data
df = load_screentime_data()

# Convert to datetime if not already
df['start_time'] = pd.to_datetime(df['start_time'])
df['end_time'] = pd.to_datetime(df['end_time'])

# Calculate time used (in minutes) - use provided usage_time if available
df['time_used'] = df['usage_time'] / 60 if 'usage_time' in df.columns and df['usage_time'].notna().any() else (df['end_time'] - df['start_time']).dt.total_seconds() / 60

# Extract date for filtering
df['start_date'] = df['start_time'].dt.date

# Sidebar for filtering
st.sidebar.header("Filters")

# Record type filter
record_types = sorted(df['record_type'].unique())
selected_record_type = st.sidebar.multiselect(
    "Data Type",
    record_types,
    default=record_types,
    help="Select which data types to display"
)

# Filter by record type
df_filtered_type = df[df['record_type'].isin(selected_record_type)] if selected_record_type else df

# Date selection
available_dates = sorted(df_filtered_type['start_date'].unique(), reverse=True)
selected_date = st.sidebar.selectbox(
    "Select Date",
    available_dates,
    format_func=lambda x: x.strftime("%Y-%m-%d (%A)")
)

# Filter data for selected date
filtered_df = df_filtered_type[df_filtered_type['start_date'] == selected_date].copy()
filtered_df['device_model'] = filtered_df['device_model'].fillna('Unknown Device')

# Show data status
with st.sidebar:
    st.divider()
    latest_date = df_filtered_type['start_date'].max()
    st.caption(f"📊 Latest data: {latest_date}")
    st.caption(f"📈 Total records: {len(df_filtered_type):,}")

# Display overview metrics
col1, col2, col3 = st.columns(3)
with col1:
    total_time = filtered_df['time_used'].sum()
    st.metric("Total Screen Time", f"{total_time:.0f} min", f"{total_time/60:.1f} hours")
with col2:
    unique_apps = filtered_df['app'].nunique()
    st.metric("Apps Used", unique_apps)
with col3:
    unique_devices = filtered_df['device_model'].nunique()
    st.metric("Devices", unique_devices)

# Tab layout
tab1, tab2, tab3, tab4 = st.tabs(["Timeline", "Apps", "Devices", "Intents"])

with tab1:
    st.subheader("App Usage Timeline")

    # Get top 20 apps for clarity
    app_usage = filtered_df.groupby('app')['time_used'].sum().reset_index().sort_values(by='time_used', ascending=False)
    top_20_apps = app_usage.head(20)['app'].tolist()
    timeline_df = filtered_df[filtered_df['app'].isin(top_20_apps)]

    if len(timeline_df) > 0:
        fig = px.timeline(
            timeline_df,
            x_start='start_time',
            x_end='end_time',
            y='app',
            color='device_model',
            title=f'App Usage Timeline on {selected_date}',
            hover_data={'device_model': True}
        )
        fig.update_yaxes(categoryorder='total ascending')
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data available for selected date")

with tab2:
    st.subheader("App Usage Summary")

    app_usage = filtered_df.groupby('app')['time_used'].sum().reset_index().sort_values(by='time_used', ascending=False)
    app_usage = app_usage[app_usage['time_used'] > 0]

    if len(app_usage) > 20:
        app_usage = app_usage.head(20)

    if len(app_usage) > 0:
        fig = px.bar(
            app_usage,
            x='time_used',
            y='app',
            orientation='h',
            title=f'Total Screen Time by App on {selected_date} (Top 20)',
            labels={'time_used': 'Time (minutes)', 'app': 'Application'},
            color='time_used',
            color_continuous_scale='Blues'
        )
        fig.update_layout(height=600, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        # Show data table
        st.dataframe(app_usage.reset_index(drop=True), use_container_width=True)
    else:
        st.info("No data available for selected date")

with tab3:
    st.subheader("Device Usage Summary")

    device_usage = filtered_df.groupby('device_model')['time_used'].sum().reset_index().sort_values(by='time_used', ascending=False)

    if len(device_usage) > 0:
        fig = px.bar(
            device_usage,
            x='time_used',
            y='device_model',
            orientation='h',
            title=f'Total Screen Time by Device on {selected_date}',
            labels={'time_used': 'Time (minutes)', 'device_model': 'Device'},
            color='time_used',
            color_continuous_scale='Greens'
        )
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        # Show data table
        st.dataframe(device_usage.reset_index(drop=True), use_container_width=True)
    else:
        st.info("No data available for selected date")

with tab4:
    st.subheader("App Intents & Events")

    # Filter for intents only
    intents_df = filtered_df[filtered_df['record_type'] == '/app/intents']

    if len(intents_df) > 0:
        col1, col2 = st.columns(2)

        with col1:
            # Intent count by app
            intent_count = intents_df.groupby('app').size().reset_index(name='count').sort_values(by='count', ascending=False)
            if len(intent_count) > 15:
                intent_count = intent_count.head(15)

            fig_intents = px.bar(
                intent_count,
                x='count',
                y='app',
                orientation='h',
                title=f'App Intents on {selected_date} (Top 15)',
                labels={'count': 'Number of Events', 'app': 'App/Intent'},
                color='count',
                color_continuous_scale='Purples'
            )
            fig_intents.update_layout(height=500, showlegend=False)
            st.plotly_chart(fig_intents, use_container_width=True)

        with col2:
            # Intents by device
            device_intents = intents_df.groupby('device_model').size().reset_index(name='count').sort_values(by='count', ascending=False)
            device_intents['device_model'] = device_intents['device_model'].fillna('Unknown Device')

            fig_device_intents = px.pie(
                device_intents,
                values='count',
                names='device_model',
                title=f'Intents by Device on {selected_date}'
            )
            fig_device_intents.update_layout(height=500)
            st.plotly_chart(fig_device_intents, use_container_width=True)

        # Intent timeline
        st.subheader("Intent Timeline")
        top_intents = intent_count.head(10)['app'].tolist()
        timeline_intents = intents_df[intents_df['app'].isin(top_intents)]

        if len(timeline_intents) > 0:
            fig_timeline = px.timeline(
                timeline_intents,
                x_start='start_time',
                x_end='end_time',
                y='app',
                color='device_model',
                title=f'Intent Timeline on {selected_date} (Top 10 Intents)',
                hover_data={'device_model': True}
            )
            fig_timeline.update_yaxes(categoryorder='total ascending')
            fig_timeline.update_layout(height=500)
            st.plotly_chart(fig_timeline, use_container_width=True)

        # Data table
        st.subheader("Intent Details")
        intent_details = intents_df[['app', 'device_model', 'start_time']].copy()
        intent_details['start_time'] = intent_details['start_time'].dt.strftime('%H:%M:%S')
        st.dataframe(
            intent_details.sort_values('start_time', ascending=False).reset_index(drop=True),
            use_container_width=True
        )
    else:
        st.info("No intent data available for selected date")

# Bottom section - Date range overview
st.divider()
st.subheader("📊 Overall Trends")

time_per_day = df_filtered_type.groupby('start_date')['time_used'].sum().reset_index()
time_per_day['day_of_week'] = pd.to_datetime(time_per_day['start_date']).dt.day_name()
time_per_day['is_weekend'] = time_per_day['day_of_week'].isin(['Saturday', 'Sunday'])

fig_overview = px.bar(
    time_per_day,
    x='start_date',
    y='time_used',
    color='is_weekend',
    color_discrete_map={True: '#ff6b6b', False: '#4a90e2'},
    title='Total Screen Time per Day (Weekends in Red)',
    labels={'time_used': 'Time (minutes)', 'start_date': 'Date', 'is_weekend': 'Weekend'},
    hover_data={'day_of_week': True}
)
fig_overview.update_xaxes(title='Date')
fig_overview.update_yaxes(title='Total Time (Minutes)')
st.plotly_chart(fig_overview, use_container_width=True)
