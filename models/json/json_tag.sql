{{ config(
    alias='tag'
) }}
select object_construct(*) as DATA from raw.tag