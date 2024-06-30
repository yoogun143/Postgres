CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
     symbol            VARCHAR(200)
     ,company_id       INT
     ,security_type    VARCHAR(200)
     ,trading_floor    VARCHAR(5)
     ,index_code       VARCHAR(5)
     ,isin             VARCHAR(200)
     ,status           VARCHAR(100)
     ,company_name     TEXT
     ,company_name_eng TEXT
     ,short_name       TEXT
     ,short_name_eng   TEXT
     ,listed_date      DATE
     ,delisted_date    DATE
     ,taxcode          VARCHAR(200)
     ,fk_date          VARCHAR(8)
  ); 

CREATE TABLE IF NOT EXISTS staging.%(table)s
  (
     code            VARCHAR(200)
     ,companyid      INT
     ,type           VARCHAR(200)
     ,floor          VARCHAR(5)
     ,indexcode      VARCHAR(5)
     ,isin           VARCHAR(200)
     ,status         VARCHAR(100)
     ,companyname    TEXT
     ,companynameeng TEXT
     ,shortname      TEXT
     ,shortnameeng   TEXT
     ,listeddate     DATE
     ,delisteddate   DATE
     ,taxcode        VARCHAR(200)
  );