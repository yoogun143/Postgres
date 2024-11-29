CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
    start_time	          bigint
    ,end_time             bigint
    ,start_location_id	  int
    ,start_lat	          decimal(38,10)
    ,start_lon	          decimal(38,10)
    ,end_location_id	    int
    ,end_lat	            decimal(38,10)
    ,end_lon	            decimal(38,10)
    ,vehicle_type	        varchar(50)
    ,probability	        decimal(38,10)
    ,distance_meters	    decimal(38,10)
  ); 