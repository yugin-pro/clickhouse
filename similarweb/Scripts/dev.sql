-- for Dev

select 1 


DESCRIBE datamart.looker 



create or replace view datamart.looker as 
select event_date, count() query_count
from datamart.system_query_log final
group by 1
order by event_date desc
limit 10


CREATE DATABASE IF NOT EXISTS temp

create drop TABLE temp.similarweb_graph
(
    json String
)
ENGINE = MergeTree()
PRIMARY KEY (json)

truncate temp.similarweb 

select isValidJSON(json) v from temp.similarweb s


/*extr lvl 1*/
create or replace table temp.similarweb_extracted ENGINE = MergeTree ORDER by (extracted_date, row_id) as
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
from (
select entries_arr, isValidJSON(entries_arr) valid from (
select 
JSONExtractArrayRaw(JSONExtractArrayRaw(JSON_QUERY(json, '$.log.entries'))[1]) entries_arr
from temp.similarweb s 
) array join entries_arr
)

/*extr lvl request*/

create or replace table temp.similarweb_extracted_request ENGINE = MergeTree ORDER by row_id as
select * from temp.similarweb_extracted se 
where resourceType like 'fetch'
limit 1 

/*extr lvl response*/

create or replace table temp.similarweb_extracted_response ENGINE = MergeTree ORDER by  row_id as
select response from temp.similarweb_extracted se 
where resourceType like 'fetch'
limit 1 

/*extr lvl type fetch*/

create or replace table temp.similarweb_extracted_type_fetch ENGINE = MergeTree ORDER by row_id as
select row_id
,JSON_VALUE(req,'$.method' ) method
,JSON_VALUE(req,'$.url' ) url
,JSON_VALUE(resp,'$.content.text' ) content
from(
select row_id 
,JSONExtractArrayRaw(request)[1] req
,JSONExtractArrayRaw(response)[1] resp
from temp.similarweb_extracted se 
where resourceType like 'fetch'
)

/*assay the response data*/
select count(distinct url) endpoints
,count(distinct content) rsps
from temp.similarweb_extracted_type_fetch

/*transform type_fetch*/

select * from (
select *
,	lower(hex(MD5(api_endpoint))) api_endpoint_h
,	lower(hex(MD5(api_query))) api_query_h
,	lower(hex(MD5(response_data))) response_data_h
, 1 - row_number() over (partition by response_data_h) dublicate
from(
select row_id
, domain(url) dom
, path(url) api_endpoint
, queryString(decodeURLComponent(url)) api_query
, content response_data
from temp.similarweb_extracted_type_fetch
where `method` like 'GET'
)
) where dublicate = 0

/*create endpoints datatable*/

create or replace table temp.similarweb_endpoint_314405130f3070950e8fae947220bce3 ENGINE = MergeTree ORDER by row_id as
select * from (
select *
,	lower(hex(MD5(api_endpoint))) api_endpoint_h
,	lower(hex(MD5(api_query))) api_query_h
,	lower(hex(MD5(response_data))) response_data_h
, 1 - row_number() over (partition by response_data_h) dublicate
from(
select row_id
, domain(url) dom
, path(url) api_endpoint
, queryString(decodeURLComponent(url)) api_query
, content response_data
from temp.similarweb_extracted_type_fetch
where `method` like 'GET'
)
) where dublicate = 0 and api_endpoint_h='314405130f3070950e8fae947220bce3'