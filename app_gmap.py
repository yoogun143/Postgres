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

# Get all coordinates for map zoom display
visit_path_coordinates = sum(visit_df['path'].apply(lambda x: list(x.values() if isinstance(x, dict) else [])),[])
visit_coordinates = visit_df[['lat', 'lon']].apply(lambda x: [float(x['lat']), float(x['lon'])], axis=1).to_list()
activity_path_coordinates = sum(activity_df['path'].apply(lambda x: list(x.values() if isinstance(x, dict) else [])),[])
activity_start_coordinates = activity_df[['start_lat', 'start_lon']].apply(lambda x: [float(x['start_lat']), float(x['start_lon'])], axis=1).to_list()
activity_end_coordinates = activity_df[['end_lat', 'end_lon']].apply(lambda x: [float(x['end_lat']), float(x['end_lon'])], axis=1).to_list()
coordinates = sum([visit_path_coordinates, visit_coordinates, activity_path_coordinates, activity_start_coordinates, activity_end_coordinates], [])
min_lat = min((coord[0] for coord in coordinates if coord[0] is not None), default=0)
max_lat = max((coord[0] for coord in coordinates if coord[0] is not None), default=0)
min_lon = min((coord[1] for coord in coordinates if coord[1] is not None), default=0)
max_lon = max((coord[1] for coord in coordinates if coord[1] is not None), default=0)

f = folium.Figure(width=10000, height=10000)
m = folium.Map(
                location=[(min_lat + max_lat)/2, (min_lon + max_lon)/2],
                zoom_start=13, 
                control_scale=True,
                tiles="cartodbpositron",
               ).add_to(f)

# # if the points are too close to each other, cluster them, create a cluster overlay with MarkerCluster
marker_cluster = MarkerCluster().add_to(m)

for _, item in activity_df.iterrows():
    # print(item['path'].values())
    tooltip_txt = '<p>'\
                +'From: '\
                +pd.to_datetime(item['start_time'], unit = 's').strftime('%Y-%m-%d %H:%M:%S')\
                +'<br> To: '\
                +pd.to_datetime(item['end_time'], unit = 's').strftime('%Y-%m-%d %H:%M:%S')\
                +'<br> Transportation: '\
                +item['vehicle_type']\
                +'</p>'
    if isinstance(item['path'], dict):
        AntPath(
            locations=list(item['path'].values()), 
            color=item['color'],
            delay=800,
            weight=5,
            opacity=0.5,
            # reverse="True", 
            dash_array=[10, 20],
            tooltip=tooltip_txt
        ).add_to(m)

        for key, value in item['path'].items():
            folium.CircleMarker(
                value,
                radius=5,
                fill=True,
                color=None,
                fill_color = 'blue',
                fill_opacity=0.3,
                tooltip=pd.to_datetime(key, unit = 's').strftime('%Y-%m-%d %H:%M:%S')
            ).add_to(m)

    folium.Marker(
                    location = (item['start_lat'], item['start_lon']),
                    icon = folium.Icon(icon='play', prefix='glyphicon', color='orange'),
                    tooltip=tooltip_txt
                ).add_to(marker_cluster)    
    
    folium.Marker(
                    location = (item['end_lat'], item['end_lon']),
                    icon = folium.Icon(icon='stop', prefix='glyphicon', color='blue'),
                    tooltip=tooltip_txt
                ).add_to(marker_cluster)    
        
for _, item in visit_df.iterrows():
    if isinstance(item['path'], dict):
        folium.PolyLine(
            list(item['path'].values()), 
            color='black',
            weight=0.5,
            opacity=0.5,
            # dash_array='5, 5'
            ).add_to(m)
    
    tooltip_txt = '<p>'\
                +'From: '\
                +pd.to_datetime(item['start_time'], unit = 's').strftime('%Y-%m-%d %H:%M:%S')\
                +'<br> To: '\
                +pd.to_datetime(item['end_time'], unit = 's').strftime('%Y-%m-%d %H:%M:%S')\
                +'<br> Duration: '\
                +item['duration']\
                +'</p>'
    
    folium.Marker(
                    location = (item['lat'], item['lon']),
                    icon = folium.Icon(icon='ok', prefix='glyphicon', color='green'),
                    tooltip = tooltip_txt
                ).add_to(marker_cluster)    
    
m.fit_bounds(m.get_bounds())

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