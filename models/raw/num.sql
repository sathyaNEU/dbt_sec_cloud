{{ config(
    pre_hook=["TRUNCATE TABLE SEC.RAW.NUM", "alter external table {{ source('stage_source', 'sec_ext_table').render() }} refresh"]
) }}
{% set year = var('year', "2024") | string %}
{% set qtr = var('qtr', "4") | string %}
{% set pattern = '%' + year + '/' + qtr + '/num.tsv' %}

SELECT 
    VALUE:c1::STRING AS adsh,         
    VALUE:c2::STRING AS tag,          
    VALUE:c3::STRING AS version,      
    TRY_CAST(TO_DATE(VALUE:c4::STRING, 'YYYYMMDD') AS DATE) AS ddate,                  
    TRY_CAST(VALUE:c5::STRING AS INT) AS qtrs,                 
    VALUE:c6::STRING AS uom,           
    VALUE:c7::STRING AS segments,     
    VALUE:c8::STRING AS coreg,       
    TRY_CAST(VALUE:c9::STRING AS INT) AS value,      
    VALUE:c10::STRING AS footnote      
FROM {{source('stage_source', 'sec_ext_table')}}
WHERE METADATA$FILENAME LIKE '{{pattern}}'
