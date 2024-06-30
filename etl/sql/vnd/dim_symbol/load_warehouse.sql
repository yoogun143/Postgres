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
     symbol
     ,company_id
     ,security_type
     ,trading_floor
     ,index_code
     ,isin
     ,status
     ,company_name
     ,company_name_eng
     ,short_name
     ,short_name_eng
     ,listed_date
     ,delisted_date
     ,taxcode
     ,fk_date  
)
SELECT * FROM insert_data;