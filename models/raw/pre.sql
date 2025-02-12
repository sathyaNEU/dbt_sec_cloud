{{ config(
    pre_hook="TRUNCATE TABLE SEC.RAW.PRE"
) }}
{% set pattern = '%' + var('year',"2024") + '/' + var('qtr',"4") + '/pre.tsv' %}

SELECT 
    VALUE:c1::STRING AS ADSH,
    VALUE:c2::INT AS REPORT,
    VALUE:c3::INT AS LINE,
    VALUE:c4::STRING AS STMT,
    VALUE:c5::INT AS INPTH,
    VALUE:c6::STRING AS RFILE,
    VALUE:c7::STRING AS TAG,
    VALUE:c8::STRING AS VERSION,
    VALUE:c9::STRING AS PLABEL,
    VALUE:c10::INT AS NEGATING
FROM {{source('stage_source', 'sec_ext_table')}}
WHERE METADATA$FILENAME LIKE '{{pattern}}'