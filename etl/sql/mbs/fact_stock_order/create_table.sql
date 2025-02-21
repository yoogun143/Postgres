CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
    date               DATE
    ,account           VARCHAR(50)
    ,symbol            VARCHAR(50)  
    ,side              VARCHAR(50)
    ,order_quantity    INT
    ,order_price       DECIMAL(18,2)
    ,status            VARCHAR(50)
    ,order_number      VARCHAR(50)
    ,channel           VARCHAR(50)
    ,match_quantity    INT
    ,match_price       DECIMAL(18,2)
    ,match_time        VARCHAR(50)
  ); 