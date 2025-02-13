{{ config(
    pre_hook="TRUNCATE TABLE SEC.RAW.TAG"
) }}
{% set pattern = '%' + var('year',"2024") + '/' + var('qtr',"4") + '/tag.tsv' %}

SELECT 
    VALUE:c1::STRING AS TAG,
    VALUE:c2::STRING AS VERSION,
    VALUE:c3::INT AS CUSTOM,
    VALUE:c4::INT AS ABSTRACT,
    VALUE:c5::STRING AS DATATYPE,
    VALUE:c6::STRING AS IORD,
    VALUE:c7::STRING AS CRDR,
    VALUE:c8::STRING AS TLABEL,
    VALUE:c9::STRING AS DOC
FROM {{source('stage_source', 'sec_ext_table')}}
WHERE METADATA$FILENAME LIKE '{{pattern}}'