CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
    start_time	        bigint PRIMARY KEY
    ,end_time           bigint
    ,location_id	      int
    ,lat	              decimal(9,6)
    ,lon	              decimal(9,6)
    ,probability	      decimal(9,6)
    ,is_timeless_visit  int
  ); 