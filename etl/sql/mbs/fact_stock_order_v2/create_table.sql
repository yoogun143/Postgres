CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
    side            varchar(50)
    ,account        varchar(50)
    ,symbol         varchar(50)
    ,price          varchar(50)
    ,quantity       decimal(18,2)
    ,order_status   varchar(50)
    ,created_date   varchar(50)
    ,order_no       varchar(50)
    ,exchange       varchar(50)
    ,matched_value  decimal(18,2)
    ,channel        varchar(50)
    ,fill_quantity  decimal(18,2)
    ,avg_price      decimal(18,2)
    ,order_price    decimal(18,2)
    ,fill_value     decimal(18,2)
    ,account_code   varchar(50)
    ,share_code     varchar(50)
    ,order_time     varchar(50)
    ,order_date     date
    ,due_date       date
    ,matched_price  decimal(18,2)
    ,matched_volume decimal(18,2)  
  ); 