DELETE FROM %(schema)s.%(table)s WHERE fk_date = %(fk_date)s;

WITH insert_data AS (
     SELECT 
          code
          ,companyid
          ,type
          ,floor
          ,indexcode
          ,isin
          ,status
          ,companyname
          ,companynameeng
          ,shortname
          ,shortnameeng
          ,listeddate
          ,delisteddate
          ,taxcode
          ,%(fk_date)s fk_date
     FROM staging.%(table)s
)
INSERT INTO %(schema)s.%(table)s
(
     code
     ,companyid
     ,type
     ,floor
     ,indexcode
     ,isin
     ,status
     ,companyname
     ,companynameeng
     ,shortname
     ,shortnameeng
     ,listeddate
     ,delisteddate
     ,taxcode
     ,fk_date  
)
SELECT * FROM insert_data;