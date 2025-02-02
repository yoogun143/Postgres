DELETE FROM %(schema)s.%(table)s WHERE fk_date = %(fk_date)s;

WITH insert_data AS (
     SELECT
          app	         
          ,usage_time	
          ,start_time	
          ,end_time	    
          ,created_at	
          ,tz	         
          ,device_id	
          ,device_model
          ,fk_date
     FROM staging.%(table)s
)
INSERT INTO %(schema)s.%(table)s
(
     app	         
     ,usage_time	
     ,start_time	
     ,end_time	    
     ,created_at	
     ,tz	         
     ,device_id	
     ,device_model
     ,fk_date
)
SELECT * FROM insert_data;