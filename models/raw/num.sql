{{ config(
    pre_hook="TRUNCATE TABLE SEC.RAW.NUM"
) }}
{% set pattern = '%' + var('year',"2024") + '/' + var('qtr',"4") + '/num.tsv' %}

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
FROM {{source('stage_source', 'sec_ext_table')}}
WHERE METADATA$FILENAME LIKE '{{pattern}}'