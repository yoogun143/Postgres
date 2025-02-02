CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
    app	           varchar(500)
    ,usage_time	   int
    ,start_time	   bigint
    ,end_time	     bigint
    ,created_at	   bigint
    ,tz	           decimal(38,10)
    ,device_id	   varchar(500)
    ,device_model	 varchar(500)
    ,fk_date       varchar(8)
  ); 

  CREATE TABLE IF NOT EXISTS staging.%(table)s
  (
    app	           varchar(500)
    ,usage_time	   int
    ,start_time	   bigint
    ,end_time	     bigint
    ,created_at	   bigint
    ,tz	           decimal(38,10)
    ,device_id	   varchar(500)
    ,device_model	 varchar(500)
    ,fk_date       varchar(8)
  ); 