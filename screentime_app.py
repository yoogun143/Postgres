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
@st.cache_data(ttl=3600)
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
                    device_model
                FROM apple.fact_screentime
                ORDER BY start_time DESC
            """)
            df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
    return df

# Load and prepare data
df = load_screentime_data()

# Convert to datetime if not already
df['start_time'] = pd.to_datetime(df['start_time'])
df['end_time'] = pd.to_datetime(df['end_time'])

# Calculate time used (in minutes)
df['time_used'] = (df['end_time'] - df['start_time']).dt.total_seconds() / 60

# Extract date for filtering
df['start_date'] = df['start_time'].dt.date

# Sidebar for date selection
st.sidebar.header("Filters")
available_dates = sorted(df['start_date'].unique(), reverse=True)
selected_date = st.sidebar.selectbox(
    "Select Date",
    available_dates,
    format_func=lambda x: x.strftime("%Y-%m-%d (%A)")
)

# Filter data for selected date
filtered_df = df[df['start_date'] == selected_date].copy()
filtered_df['device_model'] = filtered_df['device_model'].fillna('Unknown Device')

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
tab1, tab2, tab3 = st.tabs(["Timeline", "Apps", "Devices"])

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

# Bottom section - Date range overview
st.divider()
st.subheader("📊 Overall Trends")

time_per_day = df.groupby('start_date')['time_used'].sum().reset_index()
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
