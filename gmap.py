import pandas as pd 
import numpy as np
import json
from datetime import datetime

from helper.config import load_config
import psycopg2

pd.options.mode.chained_assignment = None  # default='warn'
pd.options.mode.copy_on_write = True

from scipy.spatial import cKDTree
import numpy as np

## Run gmap.dim_location
from load_warehouse import excel_to_pandas, pandas_to_warehouse

fk_date = datetime.now().strftime('%Y%m%d')
schema = 'gmap'
table = 'dim_location'

from_date = 20241101
to_date = 20241101

from_date_unix = int(datetime.strptime(str(from_date) + ' 00:00:00', '%Y%m%d %H:%M:%S').timestamp())
to_date_unix = int(datetime.strptime(str(to_date) + ' 23:59:59', '%Y%m%d %H:%M:%S').timestamp())

df = excel_to_pandas('raw\dim_location.xlsx')
pandas_to_warehouse(df, schema=schema, table=table)

config = load_config()
with psycopg2.connect(**config) as conn:
    with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM gmap.dim_location")
            stored_location = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description]) 

stored_location['lat'] = pd.to_numeric(stored_location['lat'], errors='coerce')
stored_location['lon'] = pd.to_numeric(stored_location['lon'], errors='coerce')
stored_coords = np.radians(stored_location[['lat', 'lon']].to_numpy())
tree = cKDTree(stored_coords)

import math

def haversine(lat1, lon1, lat2, lon2):
    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of Earth in kilometers (mean radius)
    r = 6371.0
    return c * r

df_path = 'raw\location-history.json'
with open(df_path, 'r') as f:
    data_json = json.load(f)
df = pd.json_normalize(data_json)

# Convert to datetime GMT +7
df['startTime'] = pd.to_datetime(df['startTime'],utc=True) + pd.Timedelta('07:00:00')
df['endTime'] = pd.to_datetime(df['endTime'], utc=True) + pd.Timedelta('07:00:00')
df['startTime'] = df['startTime'].map(pd.Timestamp.timestamp).astype(int)
df['endTime'] = df['endTime'].map(pd.Timestamp.timestamp).astype(int)

# Get columns with visit, activity, timelinePath
visit_cols = list(df.columns[df.columns.str.contains('visit')])
activity_cols = list(df.columns[df.columns.str.contains('activity')])
timelinepath_cols = list(df.columns[df.columns.str.contains('timelinePath')])
time_cols = ['startTime', 'endTime']

# Check columns right
assert len(visit_cols) + len(activity_cols) + len(timelinepath_cols) + len(time_cols) == len(df.columns)  

# assert duration between start_time and end_time of activity and visit are not intersect
visit_activity = df[df[timelinepath_cols].isna().sum(axis=1) == len(timelinepath_cols)]
visit_activity = visit_activity[visit_activity['visit.hierarchyLevel'].fillna('0') == '0']
visit_activity = visit_activity.sort_values(['startTime'])

visit_activity['startTime_human'] = pd.to_datetime(visit_activity['startTime'], unit = 's')
visit_activity['endTime_human'] = pd.to_datetime(visit_activity['endTime'], unit = 's')
visit_activity['endTime_lag'] = visit_activity['endTime'].shift(1).astype("Int64")
visit_activity['start_time_minus_prev_end_time'] = visit_activity['startTime'] - visit_activity['endTime_lag']

# visit_activity[visit_activity['start_time_minus_prev_end_time'] > 0]
# visit_activity[visit_activity['endTime'].between(1729707612-86400, 1729707612+86400)]

visit_activity['startTime'] = visit_activity['startTime'].mask(visit_activity['start_time_minus_prev_end_time'] == 0, visit_activity['startTime'] + 1)
visit_activity['duration'] = visit_activity['endTime'] - visit_activity['startTime']

# Get visit, activity, timelinePath
activity = visit_activity[visit_activity[visit_cols].isna().sum(axis=1) == len(visit_cols)][time_cols + activity_cols]
visit = visit_activity[visit_activity[activity_cols].isna().sum(axis=1) == len(activity_cols)][time_cols + visit_cols]
timelinepath = df[df[timelinepath_cols].isna().sum(axis=1) != len(timelinepath_cols)][time_cols + timelinepath_cols]

# Rename columns
visit.columns = visit.columns.str.replace('visit.', '')
activity.columns = activity.columns.str.replace('activity.', '')

# clean timelinePath
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

# clean visit
visit['point'] = visit['topCandidate.placeLocation'].apply(lambda x: x.replace('geo:',''))
visit['point']  = visit['point'].apply(lambda x: x.split(','))
visit['lat'] = visit['point'].apply(lambda x: x[0])
visit['lon'] = visit['point'].apply(lambda x: x[1])
visit['isTimelessVisit'] = visit['isTimelessVisit'].map({'true': 1, 'false': 0})
visit = visit[['startTime', 'endTime','lat', 'lon', 'probability', 'isTimelessVisit']]
visit.columns = ['start_time', 'end_time', 'lat', 'lon', 'probability', 'is_timeless_visit']

# clean activity
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

## Run gmap.fact_timelinepath
table = 'fact_timelinepath'

timelinepath = timelinepath[timelinepath['txtime'].between(from_date_unix, to_date_unix)]

pandas_to_warehouse(timelinepath, schema=schema, table=table, truncate=False)

## Run gmap.fact_visit
## Run gmap.fact_visit

table = 'fact_visit'
visit = visit[visit['start_time'].between(from_date_unix, to_date_unix)]
# Ensure lat and lon columns are numeric in both DataFrames
visit[['lat', 'lon']] = visit[['lat', 'lon']].apply(pd.to_numeric, errors='coerce')
# Prepare data
visit_coords = np.radians(visit[['lat', 'lon']].to_numpy())
# Query the nearest neighbors
distances, indices = tree.query(visit_coords, k=1)
# Assign the nearest location_id to the visit DataFrame
visit['location_id'] = stored_location.iloc[indices]['location_id'].values
visit = pd.merge(visit, stored_location[['location_id', 'lat', 'lon']].rename(columns={'lat': 'lat_location', 'lon': 'lon_location'}), how='left', on='location_id')
visit['distance'] = visit.apply(lambda x: haversine(x['lat'], x['lon'], x['lat_location'], x['lon_location']), axis=1)
visit = visit.drop(['lat_location', 'lon_location'], axis=1)
visit = visit[['start_time', 'end_time', 'location_id', 'lat', 'lon', 'probability', 'is_timeless_visit']]
pandas_to_warehouse(visit, schema=schema, table=table, truncate=True)

## Run gmap.fact_activity
table = 'fact_activity'
activity = activity[activity['start_time'].between(from_date_unix, to_date_unix)]
# Ensure lat and lon columns are numeric in both DataFrames
activity[['start_lat', 'start_lon', 'end_lat', 'end_lon']] = activity[['start_lat', 'start_lon', 'end_lat', 'end_lon']].apply(pd.to_numeric, errors='coerce')
# Prepare data
activity_start_coords = np.radians(activity[['start_lat', 'start_lon']].to_numpy())
activity_end_coords = np.radians(activity[['end_lat', 'end_lon']].to_numpy())
# Query the nearest neighbors
start_distances, start_indices = tree.query(activity_start_coords, k=1)
end_distances, end_indices = tree.query(activity_end_coords, k=1)
# Assign the nearest location_id to the visit DataFrame
activity['start_location_id'] = stored_location.iloc[start_indices]['location_id'].values
activity['end_location_id'] = stored_location.iloc[end_indices]['location_id'].values
activity = pd.merge(activity, stored_location[['location_id', 'lat', 'lon']].rename(columns={'lat': 'lat_location', 'lon': 'lon_location'}), how='left', left_on='start_location_id', right_on = 'location_id')
activity['distance_start'] = activity.apply(lambda x: haversine(x['start_lat'], x['start_lon'], x['lat_location'], x['lon_location']), axis=1)
activity = activity.drop(['lat_location', 'lon_location', 'location_id'], axis=1)
activity = pd.merge(activity, stored_location[['location_id', 'lat', 'lon']].rename(columns={'lat': 'lat_location', 'lon': 'lon_location'}), how='left', left_on='end_location_id', right_on = 'location_id')
activity['distance_end'] = activity.apply(lambda x: haversine(x['end_lat'], x['end_lon'], x['lat_location'], x['lon_location']), axis=1)
activity = activity.drop(['lat_location', 'lon_location', 'location_id'], axis=1)
activity = activity[['start_time', 'end_time', 'start_location_id', 'start_lat', 'start_lon', 'end_location_id', 'end_lat', 'end_lon', 'vehicle_type', 'probability', 'distance_meters']]
pandas_to_warehouse(activity, schema=schema, table=table, truncate=True)




# activity[activity['end_time'].between(1729707612-86400, 1729707612+86400)]
# visit[visit['end_time'].between(1729707612-86400, 1729707612+86400)]

# # Join activity and timelinePath
# activity['key'] = 1
# timelinepath['key'] = 1

# activity_right = pd.merge(activity, timelinepath, how='left', on = ['key'], suffixes = ('_activity', '_timelinepath'))
# activity_right = activity_right.drop(['key'], axis=1)
# activity_right = activity_right[activity_right['time'].between(activity_right['start_time'], activity_right['end_time'])]

# activity_right = activity_right.groupby(['start_time', 'end_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon', 'type', 'probability', 'distance_meters'])[['time', 'lat', 'lon']].apply(
#     lambda x: {key: [float(value['lat']), float(value['lon'])] for key, value in x.set_index('time').to_dict(orient='index').items()}
# ).reset_index().rename(columns={0: 'path'})

# activity = pd.merge(activity, activity_right, how='left', on = ['start_time', 'end_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon', 'type', 'probability', 'distance_meters'])
# activity = activity.drop(['key'], axis=1)

# activity['color'] = activity['type'].map({
#     'in bus': 'red',
#     'in passenger vehicle': 'blue',
#     'walking': 'green',
#     'motorcycling': 'yellow',
#     'unknown': 'black'
# })

# activity = activity.reset_index(drop=True)

# # Join visit and timelinePath
# visit['key'] = 1
# timelinepath['key'] = 1

# visit_right = pd.merge(visit, timelinepath, how='left', on = ['key'], suffixes = ('_visit', '_timelinepath'))
# visit_right = visit_right.drop(['key'], axis=1)
# visit_right = visit_right[visit_right['time'].between(visit_right['start_time'], visit_right['end_time'])]

# visit_right = visit_right.groupby(['start_time', 'end_time', 'hierarchy_level', 'lat_visit', 'lon_visit', 'probability', 'is_timeless_visit'])[['time', 'lat_timelinepath', 'lon_timelinepath']].apply(
#     lambda x: {key: [float(value['lat_timelinepath']), float(value['lon_timelinepath'])] for key, value in x.set_index('time').to_dict(orient='index').items()}
# ).reset_index().rename(columns={0: 'path'})

# visit_right = visit_right.rename(columns={'lat_visit': 'lat', 'lon_visit': 'lon'})

# visit = pd.merge(visit, visit_right, how='left', on = ['start_time', 'end_time', 'hierarchy_level', 'lat', 'lon', 'probability', 'is_timeless_visit'])
# visit = visit.drop(['key'], axis=1)

# visit['duration'] = visit['end_time'] - visit['start_time']
# visit['duration'] = visit['duration'].apply(lambda x: '{} hours {} minutes'.format(int(divmod(x, 60*60)[0]), int(divmod(divmod(x, 60*60)[1], 60)[0])))

# visit.columns = ['start_time', 'end_time', 'hierarchy_level', 'lat', 'lon', 'probability', 'is_timeless_visit', 'path', 'duration']

# visit = visit.reset_index(drop=True)