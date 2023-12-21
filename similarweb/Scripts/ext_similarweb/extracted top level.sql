/**/
create or replace table ext_similarweb.audience_demographics ENGINE = MergeTree ORDER by (extracted_date, row_id) partition by insertnumber as
select * from (
select 
JSON_VALUE(entries_arr,'$.connection' ) connection
,JSON_QUERY(entries_arr,'$.request' ) request
,JSON_QUERY(entries_arr,'$.response' ) response
,JSON_VALUE(entries_arr,'$.startedDateTime' ) startedDateTime
,JSON_VALUE(entries_arr,'$.time' ) time
,JSON_VALUE(entries_arr,'$._resourceType' ) resourceType
,valid
, now() extracted_date
, generateUUIDv4() row_id
, insertnumber
from (
select insertnumber,entries_arr, isValidJSON(entries_arr) valid from (
select 
JSONExtractArrayRaw(JSONExtractArrayRaw(JSON_QUERY(json, '$.log.entries'))[1]) entries_arr
, row_number() over () insertnumber
from raw_similarweb.audience_demographics
) array join entries_arr
)
) where resourceType like 'fetch'