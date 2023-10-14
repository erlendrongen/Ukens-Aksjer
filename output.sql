
with endring as (
SELECT * 
FROM `quant-349811.alt.dnb_ukens_aksjer_endring` e
LEFT OUTER JOIN data.info i 
ON TRIM(
       REGEXP_REPLACE(
       REGEXP_REPLACE(
       REGEXP_REPLACE(
       REGEXP_REPLACE(
       REGEXP_REPLACE(lower(i.name), 'æ', 'a'), 
                                  'ø', 'o'), 
                                  'å', 'a'), 
                                  '\\.', ''),
                                  '-', ' '))
LIKE CONCAT('%', TRIM(
                   REGEXP_REPLACE(
                   REGEXP_REPLACE(
                   REGEXP_REPLACE(
                   REGEXP_REPLACE(
                   REGEXP_REPLACE(lower(e.selskap), 'æ', 'a'), 
                                                  'ø', 'o'), 
                                                  'å', 'a'), 
                                                  '\\.', ''),
                                                  '-', ' ')), '%')
AND i.Country LIKE 'Norway')
,
aksjer as (
SELECT * 
FROM `quant-349811.alt.dnb_ukens_aksjer_aksjer` e
LEFT OUTER JOIN data.info i 
ON TRIM(
       REGEXP_REPLACE(
       REGEXP_REPLACE(
       REGEXP_REPLACE(
       REGEXP_REPLACE(
       REGEXP_REPLACE(lower(i.name), 'æ', 'a'), 
                                  'ø', 'o'), 
                                  'å', 'a'), 
                                  '\\.', ''),
                                  '-', ' '))
LIKE CONCAT('%', TRIM(
                   REGEXP_REPLACE(
                   REGEXP_REPLACE(
                   REGEXP_REPLACE(
                   REGEXP_REPLACE(
                   REGEXP_REPLACE(lower(e.selskap), 'æ', 'a'), 
                                                  'ø', 'o'), 
                                                  'å', 'a'), 
                                                  '\\.', ''),
                                                  '-', ' ')), '%')
AND i.Country LIKE 'Norway'
)

select 
a.symbol
, a.name
, a.kurs_inn
, a.date
, a.selskap
, case 
    when lower(e.endring) like '%inn%' then 1 
    when lower(e.endring) like '%ut%' then -1
    else 0
    end as endring
, ua.GCS_Path
from 
  (select symbol, name, kurs_inn, date, selskap
  from
    (select a.symbol, a.name, a.kurs_inn, a.date, selskap from aksjer a
    union all
    select e.symbol, e.name, null, e.date, selskap from endring e where e.Endring like '%inn%' )
  qualify row_number() over (partition by date, selskap order by length(symbol) asc) = 1
  order by date asc, name desc) a
left outer join endring e on e.symbol = a.symbol and e.date = a.date
left outer join alt.dnb_ukens_aksjer ua on ua.load_date = a.date
order by a.date desc, a.name


