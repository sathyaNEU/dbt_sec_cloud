{{ config(
    unique_key=['CIK', 'NAME']
) }}

SELECT 
    CIK,
    NULL AS TICKER,
    NAME,
    COUNTRYBA,
    STPRBA,
    CITYBA,
    ZIPBA,
    BAS1,
    BAS2,
    BAPH,
    COUNTRYMA,
    STPRMA,
    CITYMA,
    ZIPMA,
    MAS1,
    MAS2,
    ACIKS,
    NCIKS,
    CURRENT_TIMESTAMP AS CREATED_DT,
    CURRENT_USER() AS CREATED_BY
FROM 
    raw.sub

    