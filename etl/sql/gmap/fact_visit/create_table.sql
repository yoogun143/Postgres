CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
    start_time	        bigint
    ,end_time           bigint
    ,location_id	      int
    ,lat	              decimal(38,10)
    ,lon	              decimal(38,10)
    ,probability	      decimal(38,10)
    ,is_timeless_visit  int
  ); 