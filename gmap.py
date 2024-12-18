import pandas as pd 
import numpy as np
import json
from datetime import datetime

from helper.config import load_config
from helper.string_manipulation import haversine
import psycopg2

from scipy.spatial import cKDTree
import numpy as np
from load_warehouse import excel_to_pandas, pandas_to_warehouse
from datetime import datetime
import pandas as pd
import numpy as np
import psycopg2
from scipy.spatial import cKDTree

# Constants
fk_date = datetime.now().strftime('%Y%m%d')
schema = 'gmap'
from_date = 20241107
to_date = 20241107

# Convert dates to UNIX timestamps
from_date_unix = int(datetime.strptime(str(from_date) + ' 00:00:00', '%Y%m%d %H:%M:%S').timestamp()) + 25200 # Add 7 hours
to_date_unix = int(datetime.strptime(str(to_date) + ' 23:59:59', '%Y%m%d %H:%M:%S').timestamp()) + 25200 # Add 7 hours

# Create tree from dim_location
def build_tree():
    """
    Loads location data from Excel, stores it in dim_location in PostgreSQL, and
    builds a cKDTree from the coordinates.

    Returns
    -------
    tree : cKDTree
        A cKDTree object from the coordinates of the stored location data.
    stored_location : pd.DataFrame
        The stored location data, with columns 'lat' and 'lon' converted to numeric values.

    Notes
    -----
    This function is used to build the cKDTree from the stored location data.
    The cKDTree is used to quickly find the nearest location to a given coordinate.
    """
    print(f'Starting to build tree at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    # Load data from Excel
    df = excel_to_pandas('raw/dim_location.xlsx')

    # Store data in dim_location in PostgreSQL
    table = 'dim_location'
    pandas_to_warehouse(df, schema=schema, table=table)

    print(f'Finished loading data from Excel at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    # Fetch stored location data
    config = load_config()
    with psycopg2.connect(**config) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM gmap.dim_location")
            stored_location = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

    print(f'Finished fetching stored location data at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    # Process coordinates
    stored_location['lat'] = pd.to_numeric(stored_location['lat'], errors='coerce')
    stored_location['lon'] = pd.to_numeric(stored_location['lon'], errors='coerce')
    stored_coords = np.radians(stored_location[['lat', 'lon']].to_numpy())

    print(f'Finished processing coordinates at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    # Build cKDTree
    tree = cKDTree(stored_coords)

    print(f'Finished building cKDTree at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

    return tree, stored_location

tree, stored_location = build_tree()

def pre_process_json(df_path='raw\location-history.json'):
    """
    Pre-processes the JSON file downloaded from Google Maps.

    The function reads the JSON file, normalizes it, and processes the data. The data is filtered to only include records with a 'visit' or 'activity'.
    The 'visit' records are further filtered to only include records with a 'hierarchyLevel' of 0. The 'activity' records are also filtered to only include records with a 'hierarchyLevel' of 0.
    The 'timelinePath' records are exploded, and the 'point' column is split into 'lat' and 'lon' columns.
    The 'visit' records are cleaned to only include the 'startTime', 'endTime', 'lat', 'lon', 'probability', and 'isTimelessVisit' columns.
    The 'activity' records are cleaned to only include the 'startTime', 'endTime', 'start_lat', 'end_lat', 'start_lon', 'end_lon', 'topCandidate.type', 'probability', and 'distanceMeters' columns.

    Parameters
    ----------
    df_path : str, optional
        The path to the JSON file. Defaults to 'raw\location-history.json' if not provided.

    Returns
    -------
    visit : pd.DataFrame
        A DataFrame containing the pre-processed 'visit' records.
    activity : pd.DataFrame
        A DataFrame containing the pre-processed 'activity' records.
    timelinepath : pd.DataFrame
        A DataFrame containing the pre-processed 'timelinePath' records.
    """
    print(f"Reading JSON data from {df_path}")
    with open(df_path, 'r') as f:
        data_json = json.load(f)
    df = pd.json_normalize(data_json)

    print("Converting timestamps to datetime with GMT +7 offset")
    df['startTime'] = pd.to_datetime(df['startTime'],utc=True) + pd.Timedelta('07:00:00')
    df['endTime'] = pd.to_datetime(df['endTime'], utc=True) + pd.Timedelta('07:00:00')
    df['startTime'] = df['startTime'].map(pd.Timestamp.timestamp).astype(int)
    df['endTime'] = df['endTime'].map(pd.Timestamp.timestamp).astype(int)

    print("Identifying columns for visits, activities, and timelinePaths")
    visit_cols = list(df.columns[df.columns.str.contains('visit')])
    activity_cols = list(df.columns[df.columns.str.contains('activity')])
    timelinepath_cols = list(df.columns[df.columns.str.contains('timelinePath')])
    time_cols = ['startTime', 'endTime']

    print("Verifying column integrity")
    assert len(visit_cols) + len(activity_cols) + len(timelinepath_cols) + len(time_cols) == len(df.columns)  

    print("Filtering visit and activity records")
    visit_activity = df[df[timelinepath_cols].isna().sum(axis=1) == len(timelinepath_cols)]
    visit_activity = visit_activity[visit_activity['visit.hierarchyLevel'].fillna('0') == '0']
    visit_activity = visit_activity.sort_values(['startTime'])

    print("Calculating time differences between records")
    visit_activity['startTime_human'] = pd.to_datetime(visit_activity['startTime'], unit = 's')
    visit_activity['endTime_human'] = pd.to_datetime(visit_activity['endTime'], unit = 's')
    visit_activity['endTime_lag'] = visit_activity['endTime'].shift(1).astype('Int64')
    visit_activity['start_time_minus_prev_end_time'] = visit_activity['startTime'] - visit_activity['endTime_lag']

    assert len(visit_activity[visit_activity['start_time_minus_prev_end_time'] < 0]) == 0
    assert len(visit_activity[(visit_activity['start_time_minus_prev_end_time'] > 0) & (visit_activity['startTime'] > 1729707612)]) == 0

    print("Adjusting overlapping start times")
    visit_activity['startTime'] = visit_activity['startTime'].mask(visit_activity['start_time_minus_prev_end_time'] == 0, visit_activity['startTime'] + 1)
    visit_activity['duration'] = visit_activity['endTime'] - visit_activity['startTime']

    print("Separating visit, activity, and timelinePath data")
    activity = visit_activity[visit_activity[visit_cols].isna().sum(axis=1) == len(visit_cols)][time_cols + activity_cols]
    visit = visit_activity[visit_activity[activity_cols].isna().sum(axis=1) == len(activity_cols)][time_cols + visit_cols]
    timelinepath = df[df[timelinepath_cols].isna().sum(axis=1) != len(timelinepath_cols)][time_cols + timelinepath_cols]

    print("Renaming columns")
    visit.columns = visit.columns.str.replace('visit.', '')
    activity.columns = activity.columns.str.replace('activity.', '')

    print("Processing timelinePath data")
    timelinepath_explode = timelinepath.explode('timelinePath', ignore_index=True)
    normalized = pd.json_normalize(timelinepath_explode['timelinePath'])
    timelinepath = pd.concat([timelinepath_explode['startTime'], normalized], axis=1)
    timelinepath['point']  = timelinepath['point'].apply(lambda x: x.replace('geo:',''))
    timelinepath['point']  = timelinepath['point'].apply(lambda x: x.split(','))
    timelinepath['txtime'] = timelinepath['startTime'] + timelinepath['durationMinutesOffsetFromStartTime'].astype(int) * 60
    timelinepath['lat'] = timelinepath['point'].apply(lambda x: x[0])
    timelinepath['lon'] = timelinepath['point'].apply(lambda x: x[1])
    timelinepath = timelinepath[['txtime', 'lat', 'lon']]
    timelinepath = timelinepath.drop_duplicates(subset=['txtime'], keep='first')

    print("Processing visit data")
    visit['point'] = visit['topCandidate.placeLocation'].apply(lambda x: x.replace('geo:',''))
    visit['point']  = visit['point'].apply(lambda x: x.split(','))
    visit['lat'] = visit['point'].apply(lambda x: x[0])
    visit['lon'] = visit['point'].apply(lambda x: x[1])
    visit['isTimelessVisit'] = visit['isTimelessVisit'].map({'true': 1, 'false': 0})
    visit = visit[['startTime', 'endTime','lat', 'lon', 'probability', 'isTimelessVisit']]
    visit.columns = ['start_time', 'end_time', 'lat', 'lon', 'probability', 'is_timeless_visit']

    print("Processing activity data")
    activity['start_point'] = activity['start'].apply(lambda x: x.replace('geo:',''))
    activity['start_point']  = activity['start_point'].apply(lambda x: x.split(','))
    activity['start_lat'] = activity['start_point'].apply(lambda x: x[0])
    activity['start_lon'] = activity['start_point'].apply(lambda x: x[1])
    activity['end_point'] = activity['end'].apply(lambda x: x.replace('geo:',''))
    activity['end_point']  = activity['end_point'].apply(lambda x: x.split(','))
    activity['end_lat'] = activity['end_point'].apply(lambda x: x[0])
    activity['end_lon'] = activity['end_point'].apply(lambda x: x[1])
    activity = activity[['startTime', 'endTime', 'start_lat', 'end_lat', 'start_lon', 'end_lon', 'topCandidate.type', 'probability', 'distanceMeters']]
    activity.columns = ['start_time', 'end_time', 'start_lat', 'end_lat', 'start_lon', 'end_lon', 'vehicle_type', 'probability', 'distance_meters']

    print("Pre-processing completed")
    return visit, activity, timelinepath

visit, activity, timelinepath = pre_process_json()

def export_timelinepath(timelinepath: pd.DataFrame) -> None:
    """
    Export timelinepath data to PostgreSQL table gmap.fact_timelinepath.
    
    Filter timelinepath data by txtime between from_date_unix and to_date_unix, and
    export the result to PostgreSQL table gmap.fact_timelinepath with schema.
    If the table already exists, append the data to the existing table.
    
    Parameters
    ----------
    timelinepath : pd.DataFrame
        The timelinepath data to export.
    
    Returns
    -------
    None
    """
    # Print initial state
    print("Starting export_timelinepath")
    print(f"Initial number of records: {len(timelinepath)}")
    
    # Filter timelinepath data by txtime
    timelinepath = timelinepath[timelinepath['txtime'].between(from_date_unix, to_date_unix)]
    print(f"Number of records after filtering: {len(timelinepath)}")

    # Export timelinepath data to PostgreSQL table
    table = 'fact_timelinepath'
    print(f"Exporting data to table: {schema}.{table}")
    pandas_to_warehouse(timelinepath, schema=schema, table=table, truncate=False)
    print("Export completed")

export_timelinepath(timelinepath)  

def export_visit(visit: pd.DataFrame) -> None:
    """
    Export visit data to the PostgreSQL table gmap.fact_visit.

    Parameters
    ----------
    visit : pd.DataFrame
        The visit data to export. Must contain columns 'start_time' and 'end_time' of type np.datetime64 or pd.Timestamp,
        'lat' and 'lon' of type float64, 'probability' of type float64, and 'is_timeless_visit' of type bool.

    Returns
    -------
    None
    """
    print("Starting export_visit")
    table = 'fact_visit'
    print(f"Exporting data to table: {schema}.{table}")
    # Filter visit data by start_time
    visit = visit[visit['start_time'].between(from_date_unix, to_date_unix)]
    print(f"Number of records after filtering: {len(visit)}")
    # Ensure lat and lon columns are numeric in both DataFrames
    visit[['lat', 'lon']] = visit[['lat', 'lon']].apply(pd.to_numeric, errors='coerce')
    # Prepare data
    visit_coords = np.radians(visit[['lat', 'lon']].to_numpy())
    # Query the nearest neighbors
    distances, indices = tree.query(visit_coords, k=1)
    # Assign the nearest location_id to the visit DataFrame
    visit['location_id'] = stored_location.iloc[indices]['location_id'].values
    # Join the location_id to the stored_location DataFrame
    visit = pd.merge(visit, stored_location[['location_id', 'lat', 'lon']].rename(columns={'lat': 'lat_location', 'lon': 'lon_location'}), how='left', on='location_id')
    print(f"Number of records after joining: {len(visit)}")
    # Calculate the distance from the stored location coordinates
    visit['distance'] = visit.apply(lambda x: haversine(x['lat'], x['lon'], x['lat_location'], x['lon_location']), axis=1)
    # Drop the lat_location and lon_location columns
    visit = visit.drop(['lat_location', 'lon_location'], axis=1)
    # Select the columns to export
    visit = visit[['start_time', 'end_time', 'location_id', 'lat', 'lon', 'probability', 'is_timeless_visit']]
    # Export the visit data to the PostgreSQL table
    pandas_to_warehouse(visit, schema=schema, table=table, truncate=False)
    print("Export completed")

export_visit(visit)

def export_activity(activity: pd.DataFrame) -> None:
    """
    Export activity data to the PostgreSQL table gmap.fact_activity.

    This function filters activity data by start_time between from_date_unix and to_date_unix, 
    ensuring latitude and longitude columns are numeric. It computes the nearest location_id 
    using a spatial query and calculates the distance from the stored location coordinates. 
    The processed data is then exported to the specified PostgreSQL table, truncating 
    the table before inserting new data.

    Parameters
    ----------
    activity : pd.DataFrame
        The activity data to export, with columns start_time, end_time, start_lat, start_lon, end_lat, end_lon, vehicle_type, probability, distance_meters.

    Returns
    -------
    None
    """
    print("Starting export_activity")
    table = 'fact_activity'
    print(f"Exporting data to table: {schema}.{table}")
    
    # Filter activity data by start_time
    activity = activity[activity['start_time'].between(from_date_unix, to_date_unix)]
    print(f"Number of records after filtering: {len(activity)}")
    
    # Ensure lat and lon columns are numeric
    activity[['start_lat', 'start_lon', 'end_lat', 'end_lon']] = activity[['start_lat', 'start_lon', 'end_lat', 'end_lon']].apply(pd.to_numeric, errors='coerce')
    
    # Prepare data
    activity_start_coords = np.radians(activity[['start_lat', 'start_lon']].to_numpy())
    activity_end_coords = np.radians(activity[['end_lat', 'end_lon']].to_numpy())
    
    # Query the nearest neighbors
    start_distances, start_indices = tree.query(activity_start_coords, k=1)
    end_distances, end_indices = tree.query(activity_end_coords, k=1)
    
    # Assign the nearest location_id to the activity DataFrame
    activity['start_location_id'] = stored_location.iloc[start_indices]['location_id'].values
    activity['end_location_id'] = stored_location.iloc[end_indices]['location_id'].values
    
    # Calculate distances from start and end points to nearest locations
    activity = pd.merge(activity, stored_location[['location_id', 'lat', 'lon']].rename(columns={'lat': 'lat_location', 'lon': 'lon_location'}), how='left', left_on='start_location_id', right_on='location_id')
    activity['distance_start'] = activity.apply(lambda x: haversine(x['start_lat'], x['start_lon'], x['lat_location'], x['lon_location']), axis=1)
    activity = activity.drop(['lat_location', 'lon_location', 'location_id'], axis=1)
    
    activity = pd.merge(activity, stored_location[['location_id', 'lat', 'lon']].rename(columns={'lat': 'lat_location', 'lon': 'lon_location'}), how='left', left_on='end_location_id', right_on='location_id')
    activity['distance_end'] = activity.apply(lambda x: haversine(x['end_lat'], x['end_lon'], x['lat_location'], x['lon_location']), axis=1)
    activity = activity.drop(['lat_location', 'lon_location', 'location_id'], axis=1)
    
    # Select the columns to export
    activity = activity[['start_time', 'end_time', 'start_location_id', 'start_lat', 'start_lon', 'end_location_id', 'end_lat', 'end_lon', 'vehicle_type', 'probability', 'distance_meters']]
    
    # Export the activity data to the PostgreSQL table
    pandas_to_warehouse(activity, schema=schema, table=table, truncate=False)
    print("Export completed")

export_activity(activity)