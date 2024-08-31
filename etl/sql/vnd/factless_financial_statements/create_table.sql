CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
      symbol	              VARCHAR(200)
      ,item_code	          int
      ,report_type	        VARCHAR(20)
      ,model_type	          int
      ,numeric_value        decimal(20,2)
      ,fiscal_date	        date
      ,created_date	        date
      ,modified_date        date
      ,eff_date	            date
      ,end_date	            date
      ,valid	              BOOLEAN            
  ); 

CREATE TABLE IF NOT EXISTS staging.%(table)s
  (
      code	                VARCHAR(200)
      ,itemcode	            int
      ,reporttype	          VARCHAR(20)
      ,modeltype	          int
      ,numericvalue         decimal(20,2)
      ,fiscaldate	          date
      ,createddate	        date
      ,modifieddate         date
     ,fk_date               VARCHAR(8)
  );