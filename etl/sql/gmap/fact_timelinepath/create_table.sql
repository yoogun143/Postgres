CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
   txtime   bigint PRIMARY KEY
   ,lat     decimal(38,10)
   ,lon     decimal(38,10)
  ); 