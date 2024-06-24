CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
     code          VARCHAR(10)
     ,date         DATE
     ,time         VARCHAR(10)
     ,floor        VARCHAR(10)
     ,type         VARCHAR(10)
     ,basicprice   DECIMAL(38, 10)
     ,ceilingprice DECIMAL(38, 10)
     ,floorprice   DECIMAL(38, 10)
     ,open         DECIMAL(38, 10)
     ,high         DECIMAL(38, 10)
     ,low          DECIMAL(38, 10)
     ,close        DECIMAL(38, 10)
     ,average      DECIMAL(38, 10)
     ,adopen       DECIMAL(38, 10)
     ,adhigh       DECIMAL(38, 10)
     ,adlow        DECIMAL(38, 10)
     ,adclose      DECIMAL(38, 10)
     ,adaverage    DECIMAL(38, 10)
     ,nmvolume     DECIMAL(38, 10)
     ,nmvalue      DECIMAL(38, 10)
     ,ptvolume     DECIMAL(38, 10)
     ,ptvalue      DECIMAL(38, 10)
     ,change       DECIMAL(38, 10)
     ,adchange     DECIMAL(38, 10)
     ,pctchange    DECIMAL(38, 10)
     ,fk_date      INT
  ); 

CREATE TABLE IF NOT EXISTS staging.%(table)s
  (
     code          VARCHAR(10)
     ,date         DATE
     ,time         VARCHAR(10)
     ,floor        VARCHAR(10)
     ,type         VARCHAR(10)
     ,basicprice   DECIMAL(38, 10)
     ,ceilingprice DECIMAL(38, 10)
     ,floorprice   DECIMAL(38, 10)
     ,open         DECIMAL(38, 10)
     ,high         DECIMAL(38, 10)
     ,low          DECIMAL(38, 10)
     ,close        DECIMAL(38, 10)
     ,average      DECIMAL(38, 10)
     ,adopen       DECIMAL(38, 10)
     ,adhigh       DECIMAL(38, 10)
     ,adlow        DECIMAL(38, 10)
     ,adclose      DECIMAL(38, 10)
     ,adaverage    DECIMAL(38, 10)
     ,nmvolume     DECIMAL(38, 10)
     ,nmvalue      DECIMAL(38, 10)
     ,ptvolume     DECIMAL(38, 10)
     ,ptvalue      DECIMAL(38, 10)
     ,change       DECIMAL(38, 10)
     ,adchange     DECIMAL(38, 10)
     ,pctchange    DECIMAL(38, 10)
  );