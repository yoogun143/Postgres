CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
     event_id                VARCHAR(200)
     ,symbol                 VARCHAR(200)
     ,event_group            VARCHAR(20)
     ,event_type             VARCHAR(200)
     ,news_id                INT
     ,status                 VARCHAR(50)
     ,previous_date          DATE
     ,event_type_description TEXT
     ,note                   TEXT
     ,price                  DECIMAL(20, 2)
     ,dividend               DECIMAL(20, 2)
     ,ratio                  DECIMAL(20, 2)
     ,dividend_period        INT
     ,dividend_year          INT
     ,stock_quantity         DECIMAL(20, 2)
     ,disclosure_date        DATE
     ,effective_date         DATE
     ,expired_date           DATE
     ,actual_date            DATE
     ,trading_start_date     DATE
     ,trading_end_date       DATE
     ,register_start_date    DATE
     ,register_end_date      DATE
     ,locale                 VARCHAR(20)
     ,fk_date                VARCHAR(8)
  ); 

CREATE TABLE IF NOT EXISTS staging.%(table)s
  (
     id                 VARCHAR(200)
     ,code              VARCHAR(200)
     ,"group"           VARCHAR(20)
     ,type              VARCHAR(200)
     ,newsid            INT
     ,status            VARCHAR(50)
     ,prevdate          DATE
     ,typedesc          TEXT
     ,note              TEXT
     ,price             DECIMAL(20, 2)
     ,dividend          DECIMAL(20, 2)
     ,ratio             DECIMAL(20, 2)
     ,divperiod         INT
     ,divyear           INT
     ,numberofshares    DECIMAL(20, 2)
     ,disclosuredate    DATE
     ,effectivedate     DATE
     ,expireddate       DATE
     ,actualdate        DATE
     ,tradingstartdate  DATE
     ,tradingenddate    DATE
     ,registerstartdate DATE
     ,registerenddate   DATE
     ,locale            VARCHAR(20)
     ,fk_date           VARCHAR(8)
  );