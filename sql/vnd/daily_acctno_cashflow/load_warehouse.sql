DELETE FROM %(schema)s.%(table)s WHERE fk_date = %(fk_date)s;

WITH insert_data AS (
     SELECT 
          code            
          ,date            
          ,time            
          ,floor           
          ,type            
          ,basicPrice      
          ,ceilingPrice    
          ,floorPrice      
          ,open            
          ,high            
          ,low             
          ,close           
          ,average         
          ,adOpen          
          ,adHigh          
          ,adLow           
          ,adClose         
          ,adAverage       
          ,nmVolume        
          ,nmValue         
          ,ptVolume        
          ,ptValue         
          ,change          
          ,adChange        
          ,pctChange       
          ,%(fk_date)s fk_date
     FROM staging.%(table)s
)
INSERT INTO %(schema)s.%(table)s
(
     code            
     ,date            
     ,time            
     ,floor           
     ,type            
     ,basicPrice      
     ,ceilingPrice    
     ,floorPrice      
     ,open            
     ,high            
     ,low             
     ,close           
     ,average         
     ,adOpen          
     ,adHigh          
     ,adLow           
     ,adClose         
     ,adAverage       
     ,nmVolume        
     ,nmValue         
     ,ptVolume        
     ,ptValue         
     ,change          
     ,adChange        
     ,pctChange     
     ,fk_date  
)
SELECT * FROM insert_data;