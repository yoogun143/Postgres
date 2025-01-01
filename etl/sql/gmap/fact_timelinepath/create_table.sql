CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
   txtime   bigint PRIMARY KEY
   ,lat     decimal(9,6)
   ,lon     decimal(9,6)
  ); 