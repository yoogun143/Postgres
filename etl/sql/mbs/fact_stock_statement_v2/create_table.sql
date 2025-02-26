CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
	account      varchar(50),
	"date"       date,
	trans_no	 varchar(50),
	symbol       varchar(50),
	status       varchar(50),
	credit       numeric(18, 2),
	debit        numeric(18, 2),
	description  text
  ); 