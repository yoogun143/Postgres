CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
	  account          varchar(50),
	  "date"           date,
	  description      text,
	  txtype           varchar(50),
	  amount           numeric(18, 2),
	  category         varchar(50),
	  entry_type	   varchar(50)
  ); 