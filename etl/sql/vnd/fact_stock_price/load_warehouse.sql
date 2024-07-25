DELETE FROM %(schema)s.%(table)s WHERE fk_date = %(fk_date)s;

WITH insert_data AS (
     SELECT DISTINCT
          code
          ,date
          ,time
          ,floor
          ,type
          ,basicprice * 1000
          ,ceilingprice * 1000
          ,floorprice * 1000
          ,open * 1000
          ,high * 1000
          ,low * 1000
          ,close * 1000
          ,average * 1000
          ,adopen * 1000
          ,adhigh * 1000
          ,adlow * 1000
          ,adclose * 1000
          ,adaverage * 1000
          ,nmvolume
          ,nmvalue
          ,ptvolume
          ,ptvalue
          ,change * 1000
          ,adchange * 1000
          ,pctchange
          ,fk_date
     FROM staging.%(table)s
)
INSERT INTO %(schema)s.%(table)s
(
     symbol
     ,txdate
     ,updated_time
     ,trading_floor
     ,security_type
     ,basic_price
     ,ceiling_price
     ,floor_price
     ,open_price
     ,high_price
     ,low_price
     ,close_price
     ,average_price
     ,open_price_adjusted
     ,high_price_adjusted
     ,low_price_adjusted
     ,close_price_adjusted
     ,average_price_adjusted
     ,normal_match_volume
     ,normal_match_value
     ,put_through_volume
     ,put_through_value
     ,change
     ,ad_change
     ,pct_change
     ,fk_date  
)
SELECT * FROM insert_data;