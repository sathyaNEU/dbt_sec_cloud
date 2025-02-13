{{ config(pre_hook="TRUNCATE TABLE SEC.RAW.SUB") }}
{% set year = var('year', "2024") | string %}
{% set qtr = var('qtr', "4") | string %}
{% set pattern = '%' + year + '/' + qtr + '/sub.tsv' %}

select
    value:c1::string as adsh,
    value:c2::int as cik,
    value:c3::string as name,
    value:c4::int as sic,
    value:c5::string as countryba,
    value:c6::string as stprba,
    value:c7::string as cityba,
    value:c8::string as zipba,
    value:c9::string as bas1,
    value:c10::string as bas2,
    value:c11::string as baph,
    value:c12::string as countryma,
    value:c13::string as stprma,
    value:c14::string as cityma,
    value:c15::string as zipma,
    value:c16::string as mas1,
    value:c17::string as mas2,
    value:c18::string as countryinc,
    value:c19::string as stprinc,
    value:c20::int as ein,
    value:c21::string as former,
    value:c22::int as changed,
    value:c23::string as afs,
    value:c24::int as wksi,
    value:c25::int as fye,
    value:c26::string as form,
    to_date(value:c27::string, 'YYYYMMDD') as period,
    value:c28::int as fy,
    value:c29::string as fp,
    to_date(value:c30::string, 'YYYYMMDD') as filed,
    value:c31::string as accepted,
    value:c32::int as prevrpt,
    value:c33::int as detail,
    value:c34::string as instance,
    value:c35::int as nciks,
    value:c36::string as aciks
from {{source('stage_source', 'sec_ext_table')}}
where metadata$filename like '{{pattern}}'
