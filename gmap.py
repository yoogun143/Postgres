import pandas as pd 
import numpy as np
import json
from datetime import datetime

pd.options.mode.chained_assignment = None  # default='warn'
pd.options.mode.copy_on_write = True

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

visit_activity[visit_activity['start_time_minus_prev_end_time'] > 0]
visit_activity[visit_activity['endTime'].between(1729707612-86400, 1729707612+86400)]

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
timelinepath['time'] = timelinepath['startTime'] + timelinepath['durationMinutesOffsetFromStartTime'].astype(int) * 60
timelinepath['lat'] = timelinepath['point'].apply(lambda x: x[0])
timelinepath['lon'] = timelinepath['point'].apply(lambda x: x[1])
timelinepath = timelinepath[['time', 'lat', 'lon']]
timelinepath = timelinepath.drop_duplicates(subset=['time'], keep='first')

# clean visit
visit['point'] = visit['topCandidate.placeLocation'].apply(lambda x: x.replace('geo:',''))
visit['point']  = visit['point'].apply(lambda x: x.split(','))
visit['lat'] = visit['point'].apply(lambda x: x[0])
visit['lon'] = visit['point'].apply(lambda x: x[1])
visit['isTimelessVisit'] = visit['isTimelessVisit'].map({'true': 1, 'false': 0})
visit = visit[['startTime', 'endTime', 'hierarchyLevel','lat', 'lon', 'probability', 'isTimelessVisit']]
visit.columns = ['start_time', 'end_time', 'hierarchy_level', 'lat', 'lon', 'probability', 'is_timeless_visit']

# clean activity
activity['start_point'] = activity['start'].apply(lambda x: x.replace('geo:',''))
activity['start_point']  = activity['start_point'].apply(lambda x: x.split(','))
activity['start_lat'] = activity['start_point'].apply(lambda x: x[0])
activity['start_lon'] = activity['start_point'].apply(lambda x: x[1])
activity['end_point'] = activity['end'].apply(lambda x: x.replace('geo:',''))
activity['end_point']  = activity['end_point'].apply(lambda x: x.split(','))
activity['end_lat'] = activity['end_point'].apply(lambda x: x[0])
activity['end_lon'] = activity['end_point'].apply(lambda x: x[1])
activity = activity[['startTime', 'endTime', 'start_lat', 'start_lon', 'end_lat', 'end_lon', 'topCandidate.type', 'probability', 'distanceMeters']]
activity.columns = ['start_time', 'end_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon', 'type', 'probability', 'distance_meters']

activity[activity['end_time'].between(1729707612-86400, 1729707612+86400)]
visit[visit['end_time'].between(1729707612-86400, 1729707612+86400)]

# Join activity and timelinePath
activity['key'] = 1
timelinepath['key'] = 1

activity = pd.merge(activity, timelinepath, how='left', on = ['key'], suffixes = ('_activity', '_timelinepath'))
activity = activity.drop(['key'], axis=1)
activity = activity[activity['time'].between(activity['start_time'], activity['end_time'])]

activity = activity.groupby(['start_time', 'end_time', 'start_lat', 'start_lon', 'end_lat', 'end_lon', 'type', 'probability', 'distance_meters'])[['time', 'lat', 'lon']].apply(
    lambda x: {key: [float(value['lat']), float(value['lon'])] for key, value in x.set_index('time').to_dict(orient='index').items()}
).reset_index().rename(columns={0: 'path'})

activity['color'] = activity['type'].map({
    'in bus': 'red',
    'in passenger vehicle': 'blue',
    'walking': 'green',
    'motorcycling': 'yellow',
    'unknown': 'black'
})

activity = activity.reset_index(drop=True)

# Join visit and timelinePath
visit['key'] = 1
timelinepath['key'] = 1

visit = pd.merge(visit, timelinepath, how='left', on = ['key'], suffixes = ('_visit', '_timelinepath'))
visit = visit.drop(['key'], axis=1)
visit = visit[visit['time'].between(visit['start_time'], visit['end_time'])]

visit = visit.groupby(['start_time', 'end_time', 'hierarchy_level', 'lat_visit', 'lon_visit', 'probability', 'is_timeless_visit'])[['time', 'lat_timelinepath', 'lon_timelinepath']].apply(
    lambda x: {key: [float(value['lat_timelinepath']), float(value['lon_timelinepath'])] for key, value in x.set_index('time').to_dict(orient='index').items()}
).reset_index().rename(columns={0: 'path'})

visit = visit[visit['hierarchy_level'].astype(int) == 0]

visit['duration'] = visit['end_time'] - visit['start_time']
visit['duration'] = visit['duration'].apply(lambda x: '{} hours {} minutes'.format(int(divmod(x, 60*60)[0]), int(divmod(divmod(x, 60*60)[1], 60)[0])))

visit.columns = ['start_time', 'end_time', 'hierarchy_level', 'lat', 'lon', 'probability', 'is_timeless_visit', 'path', 'duration']

visit = visit.reset_index(drop=True)