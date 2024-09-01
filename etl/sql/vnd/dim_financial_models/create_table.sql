CREATE TABLE IF NOT EXISTS %(schema)s.%(table)s
  (
      model_type            int
      ,model_type_name      VARCHAR(20)
      ,form_type            VARCHAR(20)
      ,model_vn_desc        TEXT
      ,model_en_desc        TEXT
      ,company_form         varchar(20)
      ,note                 text                    
      ,code_list            text
      ,item_code            int
      ,ratio_code           varchar(200)
      ,item_vn_name         TEXT
      ,item_en_name         TEXT
      ,display_order        int
      ,display_level        int
      ,eff_date             date                    
      ,end_date             date                    
      ,valid                BOOLEAN                    
  ); 

CREATE TABLE IF NOT EXISTS staging.%(table)s
  (
      modeltype             int
      ,modeltypename        VARCHAR(20)
      ,formtype             VARCHAR(20)
      ,modelvndesc          TEXT
      ,modelendesc          TEXT
      ,companyform          VARCHAR(20)
      ,note                 TEXT
      ,codelist             TEXT
      ,itemcode             int
      ,ratiocode            VARCHAR(200)
      ,itemvnname           TEXT
      ,itemenname           TEXT
      ,displayorder         int
      ,displaylevel         int
     ,fk_date               VARCHAR(8)
  );