CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
    start_time	          bigint PRIMARY KEY
    ,end_time             bigint
    ,start_location_id	  int
    ,start_lat	          decimal(9,6)
    ,start_lon	          decimal(9,6)
    ,end_location_id	    int
    ,end_lat	            decimal(9,6)
    ,end_lon	            decimal(9,6)
    ,vehicle_type	        varchar(50)
    ,probability	        decimal(9,6)
    ,distance_meters	    decimal(20,2)
    ,note                 text  
  ); 