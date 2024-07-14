CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
     customer_name     VARCHAR(200)
     ,address          VARCHAR(200)
     ,email            VARCHAR(200)
     ,phone_number     VARCHAR(200)
     ,eff_date         DATE
     ,end_date         DATE
     ,valid            INT
  ); 

CREATE TABLE IF NOT EXISTS staging.%(table)s
  (
     customer_name     VARCHAR(200)
     ,address          VARCHAR(200)
     ,email            VARCHAR(200)
     ,phone_number     VARCHAR(200)
     ,fk_date          VARCHAR(8)
  );