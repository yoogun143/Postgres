import streamlit as st
import folium
from folium.plugins import MarkerCluster, AntPath
from streamlit_folium import folium_static
import pandas as pd
from datetime import datetime
import psycopg2
from helper.config import load_config
import numpy as np

# Page config
st.set_page_config(
    page_title="Google Maps Timeline Viewer",
    page_icon="🗺️",
    layout="wide"
)

# Title
st.title("🗺️ Google Maps Timeline Viewer")

# Custom CSS for date picker
st.markdown("""
    <style>
        div[data-baseweb="calendar"] {
            font-size: 12px;
        }
        div[data-baseweb="input"] {
            width: 200px;
        }
    </style>
""", unsafe_allow_html=True)

# Add refresh button in the sidebar
refresh_sidebar = st.sidebar.button("🔄 Refresh Data", key="refresh_sidebar")

# Date picker in a smaller column
col1, col2 = st.columns([1, 4])
with col1:
    # Date selection
    selected_date = st.date_input(
        "Select Date",
        datetime.now().date(),
        format="YYYY-MM-DD"
    )
    selected_date_str = selected_date.strftime('%Y%m%d')
    
    # Add a prominent refresh button
    refresh_main = st.button("🔄 Refresh Data", use_container_width=True, key="refresh_main")

# Create a cache key that changes when refresh buttons are clicked
if 'cache_key' not in st.session_state:
    st.session_state.cache_key = 0
    
# Update cache key when refresh buttons are clicked
if refresh_sidebar or refresh_main:
    st.session_state.cache_key += 1
    st.rerun()

# Database connection
@st.cache_resource
def init_connection():
    config = load_config()
    return psycopg2.connect(**config)

conn = init_connection()

@st.cache_data
def get_timeline_data(date_str, _conn, cache_key=0):
    # Convert date string to unix timestamp
    from_date_unix = int(datetime.strptime(str(date_str) + ' 00:00:00', '%Y%m%d %H:%M:%S').timestamp()) + 25200
    to_date_unix = int(datetime.strptime(str(date_str) + ' 23:59:59', '%Y%m%d %H:%M:%S').timestamp()) + 25200
    
    # Add debugging information
    st.sidebar.write("Debug Information:")
    st.sidebar.write(f"Date string: {date_str}")
    st.sidebar.write(f"From timestamp: {from_date_unix} ({datetime.fromtimestamp(from_date_unix).strftime('%Y-%m-%d %H:%M:%S')})")
    st.sidebar.write(f"To timestamp: {to_date_unix} ({datetime.fromtimestamp(to_date_unix).strftime('%Y-%m-%d %H:%M:%S')})")
    
    with _conn.cursor() as cur:
        # Get activity data
        activity_query = f"""
                select 
                    a.start_time
                    ,a.end_time
                    ,a.start_location_id
                    ,a.end_location_id
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
                    ,a.start_location_id
                    ,a.end_location_id
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
                """
                
        # Show the SQL query for debugging
        with st.sidebar.expander("Activity Query"):
            st.code(activity_query, language="sql")
            
        cur.execute(activity_query)
        activity = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        
        # Show raw results count
        st.sidebar.write(f"Activity results: {len(activity)} rows")

        # Get visit data
        visit_query = f"""
                select 
                    a.start_time 
                    ,a.end_time 
                    ,a.location_id
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
                    ,a.location_id
                    ,c.lat 
                    ,c.lon
                    ,c.location_name
                    ,c.category
                """
                
        with st.sidebar.expander("Visit Query"):
            st.code(visit_query, language="sql")
            
        cur.execute(visit_query)
        visit = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        
        # Show raw results count
        st.sidebar.write(f"Visit results: {len(visit)} rows")

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

# Function to get location name from ID
def get_location_name(location_id, _conn):
    if location_id is None:
        return None
    with _conn.cursor() as cur:
        cur.execute("""
            SELECT location_name 
            FROM gmap.dim_location 
            WHERE location_id = %s
        """, (location_id,))
        result = cur.fetchone()
        return result[0] if result else None

# Function to generate SQL statements for preview
def generate_sql_statements(df, edited_rows):
    sql_statements = []
    for idx in edited_rows:
        row = df.iloc[idx]
        if pd.isna(row['end_location_id']):  # This is a visit record
            sql = f"""UPDATE gmap.fact_visit
                     SET location_id = {row['start_location_id']}
                     WHERE start_time = {int(row['start_time'])} 
                     AND end_time = {int(row['end_time'])};"""
        else:  # This is an activity record
            sql = f"""UPDATE gmap.fact_activity
                     SET start_location_id = {row['start_location_id']},
                         end_location_id = {row['end_location_id']}
                     WHERE start_time = {int(row['start_time'])} 
                     AND end_time = {int(row['end_time'])};"""
        sql_statements.append(sql)
    return sql_statements

# Function to save changes to database
def save_timeline_changes(df, _conn):
    with _conn.cursor() as cur:
        for idx, row in df.iterrows():
            if pd.isna(row['end_location_id']):  # This is a visit record
                cur.execute("""
                    UPDATE gmap.fact_visit
                    SET location_id = %s
                    WHERE start_time = %s AND end_time = %s
                """, (row['start_location_id'], 
                      int(row['start_time']), 
                      int(row['end_time'])))
            else:  # This is an activity record
                cur.execute("""
                    UPDATE gmap.fact_activity
                    SET start_location_id = %s,
                        end_location_id = %s
                    WHERE start_time = %s AND end_time = %s
                """, (row['start_location_id'], row['end_location_id'],
                      int(row['start_time']), int(row['end_time'])))
        _conn.commit()
    st.success("Changes saved successfully!")

# Function to get all locations
def get_all_locations_data(_conn):
    with _conn.cursor() as cur:
        cur.execute("""
            SELECT location_id, location_name, address, lat, lon, category 
            FROM gmap.dim_location 
            ORDER BY location_id
        """)
        results = cur.fetchall()
        return pd.DataFrame(results, columns=['location_id', 'location_name', 'address', 'lat', 'lon', 'category'])

# Function to add a new location
def add_new_location(location_name, address, lat, lon, category, _conn):
    with _conn.cursor() as cur:
        # Get the next location_id
        cur.execute("SELECT MAX(location_id) FROM gmap.dim_location")
        max_id = cur.fetchone()[0]
        next_id = max_id + 1 if max_id else 1
        
        # Insert new location
        cur.execute("""
            INSERT INTO gmap.dim_location (location_id, location_name, address, lat, lon, category)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (next_id, location_name, address, lat, lon, category))
        _conn.commit()
    return next_id

# Function to initialize form values
def init_form_values():
    """Initialize default values for location form if not already in session state"""
    if "form_clear_state" not in st.session_state:
        st.session_state.form_clear_state = False
        
    # Initialize form values when clear state is True
    if st.session_state.form_clear_state:
        st.session_state.loc_name = ""
        st.session_state.loc_cat = ""
        st.session_state.loc_addr = ""
        st.session_state.loc_coords = ""
        st.session_state.loc_lat = 0.0
        st.session_state.loc_lon = 0.0
        # Reset the clear state
        st.session_state.form_clear_state = False

# Get data for selected date
visit_df, activity_df = get_timeline_data(selected_date_str, conn, st.session_state.cache_key)

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

# Create combined timeline dataframe
timeline_df = pd.DataFrame(
    np.concatenate([
        activity_df[['start_time_human', 'end_time_human', 'start_time', 'end_time', 
                    'start_location_id', 'start_location_name', 
                    'end_location_id', 'end_location_name', 'vehicle_type']].to_numpy(),
        visit_df[['start_time_human', 'end_time_human', 'start_time', 'end_time', 
                 'location_id', 'location_name']].assign(
                     end_location_id=None, 
                     end_location_name=None, 
                     vehicle_type=None).rename(
                         columns={'location_id': 'start_location_id', 
                                'location_name': 'start_location_name'}).to_numpy()
    ]),
    columns=['start_time_human', 'end_time_human', 'start_time', 'end_time', 
             'start_location_id', 'start_location_name', 
             'end_location_id', 'end_location_name', 'vehicle_type']
).sort_values('start_time')

# Store original timestamps for reference
timeline_df['_original_start_time'] = timeline_df['start_time']
timeline_df['_original_end_time'] = timeline_df['end_time']

# Create editable data editor
edited_df = st.data_editor(
    timeline_df,
    hide_index=True,
    column_config={
        "start_time_human": st.column_config.DatetimeColumn(
            "Start Time (Human)",
            format="D MMM YYYY, HH:mm",
            disabled=True
        ),
        "end_time_human": st.column_config.DatetimeColumn(
            "End Time (Human)",
            format="D MMM YYYY, HH:mm",
            disabled=True
        ),
        "start_time": st.column_config.NumberColumn(
            "Start Time (Unix)",
            disabled=True
        ),
        "end_time": st.column_config.NumberColumn(
            "End Time (Unix)",
            disabled=True
        ),
        "start_location_id": st.column_config.NumberColumn(
            "Start Location ID",
            disabled=False,  # Allow editing
            min_value=1
        ),
        "start_location_name": st.column_config.TextColumn(
            "Start Location",
            disabled=True
        ),
        "end_location_id": st.column_config.NumberColumn(
            "End Location ID",
            disabled=False,  # Allow editing
            min_value=1
        ),
        "end_location_name": st.column_config.TextColumn(
            "End Location",
            disabled=True
        ),
        "vehicle_type": st.column_config.TextColumn(
            "Vehicle Type",
            disabled=True,
            width="medium"
        )
    },
    column_order=[
        "start_time_human", "end_time_human",
        "start_time", "end_time",
        "start_location_id", "start_location_name",
        "end_location_id", "end_location_name",
        "vehicle_type"
    ],
    key="timeline_editor"
)

# Update location names if IDs have changed
if st.session_state.get("timeline_editor", {}).get("edited_rows"):
    edited_rows = st.session_state["timeline_editor"]["edited_rows"]
    for idx, changes in edited_rows.items():
        if 'start_location_id' in changes:
            new_name = get_location_name(changes['start_location_id'], conn)
            if new_name:
                edited_df.at[idx, 'start_location_name'] = new_name
        if 'end_location_id' in changes:
            new_name = get_location_name(changes['end_location_id'], conn)
            if new_name:
                edited_df.at[idx, 'end_location_name'] = new_name
    
    # Show SQL preview and confirmation
    sql_statements = generate_sql_statements(edited_df, edited_rows.keys())
    st.subheader("Preview SQL Updates")
    
    # Combine all SQL statements into a single string with line breaks
    combined_sql = "\n\n".join(sql_statements)
    st.code(combined_sql, language="sql")
    
    # Show save button with confirmation
    if st.button("Confirm and Save Changes"):
        save_timeline_changes(edited_df, conn)

st.subheader("Map View")
folium_static(m)

# Add a compact location management section at the bottom
with st.expander("Location Management", expanded=False):
    col1, col2 = st.columns([2, 1])
    
    with col1:
        # Display locations in a data table
        st.caption("All Locations")
        locations_df = get_all_locations_data(conn)
        st.dataframe(
            locations_df,
            column_config={
                "location_id": st.column_config.NumberColumn("ID", width="small"),
                "location_name": st.column_config.TextColumn("Name"),
                "address": st.column_config.TextColumn("Address"),
                "lat": st.column_config.NumberColumn("Latitude", format="%.6f", width="medium"),
                "lon": st.column_config.NumberColumn("Longitude", format="%.6f", width="medium"),
                "category": st.column_config.TextColumn("Category", width="small")
            },
            use_container_width=True,
            height=300,
            hide_index=True
        )
    
    with col2:
        # Add new location form
        st.caption("Add New Location")
        
        # Initialize form values
        init_form_values()
        
        # Clear form button (outside the form)
        if st.button("Clear Form", key="clear_form_button"):
            st.session_state.form_clear_state = True
            st.rerun()
        
        with st.form("add_location_form", border=False):
            new_location_name = st.text_input("Name", key="loc_name", label_visibility="collapsed", placeholder="Location Name")
            new_category = st.text_input("Category", key="loc_cat", label_visibility="collapsed", placeholder="Category")
            new_address = st.text_input("Address", key="loc_addr", label_visibility="collapsed", placeholder="Address")
            
            # Add coordinates input in format "lat, lon"
            coords_input = st.text_input("Coordinates", key="loc_coords", label_visibility="collapsed", 
                                        placeholder="Coordinates (e.g., 21.047597, 105.744979)")
            
            # Keep individual lat/lon inputs but hide them if coordinates are provided
            new_lat = st.number_input("Lat", format="%.6f", step=0.000001, key="loc_lat", 
                                     label_visibility="collapsed", placeholder="Latitude")
            new_lon = st.number_input("Lon", format="%.6f", step=0.000001, key="loc_lon", 
                                     label_visibility="collapsed", placeholder="Longitude")
            
            # Submit button
            submit_button = st.form_submit_button("Add Location")
            
            if submit_button:
                if new_location_name:
                    # Parse coordinates if provided
                    if coords_input:
                        try:
                            lat_str, lon_str = coords_input.split(',')
                            new_lat = float(lat_str.strip())
                            new_lon = float(lon_str.strip())
                        except:
                            st.error("Invalid coordinates format. Use format: latitude, longitude")
                            st.stop()
                    
                    new_id = add_new_location(new_location_name, new_address, new_lat, new_lon, new_category, conn)
                    st.success(f"Added location ID: {new_id}")
                    st.rerun()
                else:
                    st.error("Name required")