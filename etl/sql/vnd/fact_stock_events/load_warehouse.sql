DELETE FROM %(schema)s.%(table)s WHERE fk_date = %(fk_date)s;

WITH insert_data AS (
     SELECT 
          id
          ,code
          ,"group"
          ,type
          ,newsid
          ,status
          ,prevdate
          ,typedesc
          ,note
          ,price
          ,dividend
          ,ratio
          ,divperiod
          ,divyear
          ,numberofshares
          ,disclosuredate
          ,effectivedate
          ,expireddate
          ,actualdate
          ,tradingstartdate
          ,tradingenddate
          ,registerstartdate
          ,registerenddate
          ,locale
          ,%(fk_date)s fk_date
     FROM staging.%(table)s
)
INSERT INTO %(schema)s.%(table)s
(
     event_id
     ,symbol
     ,event_group
     ,event_type
     ,news_id
     ,status
     ,previous_date
     ,event_type_description
     ,note
     ,price
     ,dividend
     ,ratio
     ,dividend_period
     ,dividend_year
     ,stock_quantity
     ,disclosure_date
     ,effective_date
     ,expired_date
     ,actual_date
     ,trading_start_date
     ,trading_end_date
     ,register_start_date
     ,register_end_date
     ,locale
     ,fk_date  
)
SELECT * FROM insert_data;