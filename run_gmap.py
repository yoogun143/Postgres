from gmap import export_visit, export_activity, export_timelinepath, pre_process_json, export_dim_location
from datetime import datetime

import warnings
import pandas as pd
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action='ignore', category=(SettingWithCopyWarning))

if __name__ == '__main__':
    # Constants
    schema = 'gmap'
    df_path = 'raw/location-history-20250329.json'
    from_date = 20250305
    to_date = 20250329

    # Convert dates to UNIX timestamps
    from_date_unix = int(datetime.strptime(str(from_date) + ' 00:00:00', '%Y%m%d %H:%M:%S').timestamp()) + 25200 # Add 7 hours
    to_date_unix = int(datetime.strptime(str(to_date) + ' 23:59:59', '%Y%m%d %H:%M:%S').timestamp()) + 25200 # Add 7 hours

    # Run functions
    # export_dim_location()
    visit, activity, timelinepath = pre_process_json(df_path)
    export_timelinepath(timelinepath, schema=schema, from_date_unix=from_date_unix, to_date_unix=to_date_unix)
    export_visit(visit, schema=schema, from_date_unix=from_date_unix, to_date_unix=to_date_unix)
    export_activity(activity, schema=schema, from_date_unix=from_date_unix, to_date_unix=to_date_unix)