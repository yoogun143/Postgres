CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
   location_id    INT
   ,location_name varchar(50)
   ,category      varchar(50)
   ,address       text
   ,lat           decimal(9,6)
   ,lon           decimal(9,6)
  ); 