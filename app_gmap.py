import streamlit as st
import folium
from folium.plugins import MarkerCluster, AntPath
from streamlit_folium import folium_static
import pandas as pd
from datetime import datetime
import psycopg2
from helper.config import load_config

# Page config
st.set_page_config(
    page_title="Google Maps Timeline Viewer",
    page_icon="🗺️",
    layout="wide"
)

# Title
st.title("🗺️ Google Maps Timeline Viewer")

# Sidebar
st.sidebar.header("Settings")

# Date selector
selected_date = st.sidebar.date_input(
    "Select Date",
    value=datetime.now()
)

# Convert selected date to the format needed (YYYYMMDD)
selected_date_str = selected_date.strftime("%Y%m%d")

# Database connection
@st.cache_resource
def init_connection():
    config = load_config()
    return psycopg2.connect(**config)

conn = init_connection()

@st.cache_data
def get_timeline_data(date_str, _conn):
    # Convert date string to unix timestamp
    from_date_unix = int(datetime.strptime(str(date_str) + ' 00:00:00', '%Y%m%d %H:%M:%S').timestamp()) + 25200
    to_date_unix = int(datetime.strptime(str(date_str) + ' 23:59:59', '%Y%m%d %H:%M:%S').timestamp()) + 25200
    
    with _conn.cursor() as cur:
        # Get activity data
        cur.execute(f"""
                select 
                    a.start_time
                    ,a.end_time
                    ,c.lat as start_lat
                    ,c.lon as start_lon
                    ,d.lat as end_lat
                    ,d.lon as end_lon
                    ,a.vehicle_type
                    ,a.distance_meters
                    ,c.location_name as start_location_name
                    ,c.category as start_category
                    ,d.location_name as end_location_name
                    ,d.category as end_category
                    ,case 
                        when json_agg(
                            json_build_object('txtime', b.txtime, 'lat', b.lat, 'lon', b.lon)
                            order by b.txtime
                        ) filter (where b.txtime is not null or b.lat is not null or b.lon is not null) is null 
                        then null
                        else json_agg(
                            json_build_object('txtime', b.txtime, 'lat', b.lat, 'lon', b.lon)
                            order by b.txtime
                        )
                    end as path 
                from gmap.fact_activity a
                left join gmap.fact_timelinepath b
                on b.txtime between a.start_time and a.end_time 
                left join gmap.dim_location c
                on a.start_location_id = c.location_id
                left join gmap.dim_location d
                on a.end_location_id = d.location_id
                where a.start_time between {from_date_unix} AND {to_date_unix}
                group by 
                    a.start_time
                    ,a.end_time
                    ,c.lat
                    ,c.lon
                    ,d.lat
                    ,d.lon
                    ,a.vehicle_type
                    ,a.distance_meters
                    ,c.location_name
                    ,c.category
                    ,d.location_name
                    ,d.category
                """)
        activity = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

        # Get visit data
        cur.execute(f"""
                select 
                    a.start_time 
                    ,a.end_time 
                    ,c.lat 
                    ,c.lon
                    ,c.location_name
                    ,c.category
                    ,case 
                        when json_agg(
                            json_build_object('txtime', b.txtime, 'lat', b.lat, 'lon', b.lon)
                            order by b.txtime
                        ) filter (where b.txtime is not null or b.lat is not null or b.lon is not null) is null 
                        then null
                        else json_agg(
                            json_build_object('txtime', b.txtime, 'lat', b.lat, 'lon', b.lon)
                            order by b.txtime
                        )
                    end as path
                from gmap.fact_visit a
                left join gmap.fact_timelinepath b
                on b.txtime between a.start_time and a.end_time 
                left join gmap.dim_location c
                on a.location_id = c.location_id
                where a.start_time between {from_date_unix} AND {to_date_unix}
                group by 
                    a.start_time 
                    ,a.end_time 
                    ,c.lat 
                    ,c.lon
                    ,c.location_name
                    ,c.category
                """)
        visit = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

    # Process path data in activity DataFrame
    activity['path'] = activity['path'].apply(lambda x: {item['txtime']: [item['lat'], item['lon']] for item in x} if x is not None else None)
    activity['color'] = activity['vehicle_type'].map({
        'in bus': 'red',
        'in passenger vehicle': 'blue',
        'walking': 'green',
        'motorcycling': 'magenta',
        'in train': 'yellow',
        'in subway': 'cyan',
        'unknown': 'black'
    })
    activity['start_time_human'] = pd.to_datetime(activity['start_time'], unit='s')
    activity['end_time_human'] = pd.to_datetime(activity['end_time'], unit='s')

    # add info to visit
    visit['path'] = visit['path'].apply(lambda x: {item['txtime']: [item['lat'], item['lon']] for item in x} if x is not None else None)
    visit['duration'] = visit['end_time'] - visit['start_time']
    visit['duration'] = visit['duration'].apply(lambda x: '{} hours {} minutes'.format(int(divmod(x, 60*60)[0]), int(divmod(divmod(x, 60*60)[1], 60)[0])))
    visit['start_time_human'] = pd.to_datetime(visit['start_time'], unit='s')
    visit['end_time_human'] = pd.to_datetime(visit['end_time'], unit='s')

    return visit, activity

# Function to save changes to database
def save_timeline_changes(df, _conn):
    with _conn.cursor() as cur:
        for idx, row in df.iterrows():
            # Convert timestamps back to unix
            start_time = int(row['start_time'].timestamp())
            end_time = int(row['end_time'].timestamp())
            
            # Update fact_activity table
            cur.execute("""
                UPDATE gmap.fact_activity
                SET start_time = %s,
                    end_time = %s,
                    vehicle_type = %s
                WHERE start_time = %s AND end_time = %s
            """, (start_time, end_time, row['vehicle_type'], 
                  int(df.at[idx, '_original_start_time']), 
                  int(df.at[idx, '_original_end_time'])))
        
        _conn.commit()

# Get data for selected date
visit_df, activity_df = get_timeline_data(selected_date_str, conn)

# Create map
m = folium.Map(location=[21.0278, 105.8342], zoom_start=13)
marker_cluster = MarkerCluster().add_to(m)

# Add markers for visits
for idx, row in activity_df.iterrows():
    # Start marker
    folium.Marker(
        [row['start_lat'], row['start_lon']],
        popup=f"Start: {datetime.fromtimestamp(row['start_time'])}<br>Location: {row['start_location_name']}",
        icon=folium.Icon(color='green', icon='info-sign')
    ).add_to(marker_cluster)
    
    # End marker
    folium.Marker(
        [row['end_lat'], row['end_lon']],
        popup=f"End: {datetime.fromtimestamp(row['end_time'])}<br>Location: {row['end_location_name']}",
        icon=folium.Icon(color='red', icon='info-sign')
    ).add_to(marker_cluster)

# Add path lines
for idx, row in activity_df.iterrows():
    if row['path']:
        path_points = [[coords[0], coords[1]] for coords in row['path'].values()]
        AntPath(
            locations=path_points,
            popup=f"Journey: {row['vehicle_type']}",
            weight=2,
            color='blue'
        ).add_to(m)

# Display layout
st.subheader("Timeline")
# Display timeline data
timeline_df = activity_df[[
    'start_time', 
    'end_time', 
    'start_location_name', 
    'end_location_name', 
    'vehicle_type'
]].copy()

# Store original timestamps for reference
timeline_df['_original_start_time'] = timeline_df['start_time']
timeline_df['_original_end_time'] = timeline_df['end_time']

# Convert timestamps for display
timeline_df['start_time'] = timeline_df['start_time'].apply(lambda x: datetime.fromtimestamp(x))
timeline_df['end_time'] = timeline_df['end_time'].apply(lambda x: datetime.fromtimestamp(x))

# Create editable data editor
edited_df = st.data_editor(
    timeline_df.drop(['_original_start_time', '_original_end_time'], axis=1),
    hide_index=True,
    column_config={
        "start_time": st.column_config.DatetimeColumn(
            "Start Time",
            format="D MMM YYYY, HH:mm"
        ),
        "end_time": st.column_config.DatetimeColumn(
            "End Time",
            format="D MMM YYYY, HH:mm"
        ),
        "start_location_name": st.column_config.TextColumn(
            "Start Location",
            disabled=True
        ),
        "end_location_name": st.column_config.TextColumn(
            "End Location",
            disabled=True
        ),
        "vehicle_type": st.column_config.SelectboxColumn(
            "Vehicle Type",
            options=["WALKING", "IN_PASSENGER_VEHICLE", "CYCLING", "FLYING"]
        )
    },
    key="timeline_editor"
)

# Check if the dataframe was edited
if st.session_state.get("timeline_editor", {}).get("edited_rows"):
    st.button("Save Changes", on_click=lambda: save_timeline_changes(edited_df, conn))

st.subheader("Map View")
folium_static(m)