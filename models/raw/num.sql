{{ config(
    pre_hook="TRUNCATE TABLE SEC.RAW.NUM"
) }}

SELECT 
    VALUE:c1::STRING AS ADSH,         
    VALUE:c2::STRING AS TAG,          
    VALUE:c3::STRING AS VERSION,      
    TO_DATE(VALUE:c4::STRING, 'YYYYMMDD') AS DDATE,                  
    VALUE:c5::INT AS QTRS,                 
    VALUE:c6::STRING AS UOM,           
    VALUE:c7::STRING AS SEGMENTS,     
    VALUE:c8::STRING AS COREG,       
    VALUE:c9::INT AS VALUE,      
    VALUE:c10::STRING AS FOOTNOTE      
FROM SEC.OBJECTS.SEC_EXT_TABLE
WHERE METADATA$FILENAME LIKE '%2024/4/num.tsv'