CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
     symbol                  VARCHAR(20)
     ,txdate                 DATE
     ,updated_time           VARCHAR(10)
     ,trading_floor          VARCHAR(10)
     ,security_type          VARCHAR(10)
     ,basic_price            DECIMAL(20, 2)
     ,ceiling_price          DECIMAL(20, 2)
     ,floor_price            DECIMAL(20, 2)
     ,high_price             DECIMAL(20, 2)
     ,open_price             DECIMAL(20, 2)
     ,low_price              DECIMAL(20, 2)
     ,close_price            DECIMAL(20, 2)
     ,average_price          DECIMAL(20, 2)
     ,open_price_adjusted    DECIMAL(20, 2)
     ,high_price_adjusted    DECIMAL(20, 2)
     ,low_price_adjusted     DECIMAL(20, 2)
     ,close_price_adjusted   DECIMAL(20, 2)
     ,average_price_adjusted DECIMAL(20, 2)
     ,normal_match_volume    DECIMAL(20, 2)
     ,normal_match_value     DECIMAL(20, 2)
     ,put_through_volume     DECIMAL(20, 2)
     ,put_through_value      DECIMAL(20, 2)
     ,change                 DECIMAL(20, 2)
     ,ad_change              DECIMAL(20, 2)
     ,pct_change             DECIMAL(20, 2)
     ,fk_date                VARCHAR(8)
  ); 

CREATE TABLE IF NOT EXISTS staging.%(table)s
  (
     code          VARCHAR(20)
     ,date         DATE
     ,time         VARCHAR(10)
     ,floor        VARCHAR(10)
     ,type         VARCHAR(10)
     ,basicprice   DECIMAL(20, 2)
     ,ceilingprice DECIMAL(20, 2)
     ,floorprice   DECIMAL(20, 2)
     ,open         DECIMAL(20, 2)
     ,high         DECIMAL(20, 2)
     ,low          DECIMAL(20, 2)
     ,close        DECIMAL(20, 2)
     ,average      DECIMAL(20, 2)
     ,adopen       DECIMAL(20, 2)
     ,adhigh       DECIMAL(20, 2)
     ,adlow        DECIMAL(20, 2)
     ,adclose      DECIMAL(20, 2)
     ,adaverage    DECIMAL(20, 2)
     ,nmvolume     DECIMAL(20, 2)
     ,nmvalue      DECIMAL(20, 2)
     ,ptvolume     DECIMAL(20, 2)
     ,ptvalue      DECIMAL(20, 2)
     ,change       DECIMAL(20, 2)
     ,adchange     DECIMAL(20, 2)
     ,pctchange    DECIMAL(20, 2)
  );