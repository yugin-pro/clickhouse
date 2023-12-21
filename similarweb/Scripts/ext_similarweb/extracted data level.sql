--ext_similarweb.audience_demographics
create or replace table ext_similarweb.audience_demographics_data ENGINE = MergeTree ORDER by row_id partition by insertnumber as
select * from (
select *
,	lower(hex(MD5(api_endpoint))) api_endpoint_h
,	lower(hex(MD5(api_query))) api_query_h
,	lower(hex(MD5(response_data))) response_data_h
, 1 - row_number() over (partition by insertnumber,api_query_h, response_data_h) dublicate
from (
select *
, domain(url) dom
, path(url) api_endpoint
, queryString(decodeURLComponent(url)) api_query
, content response_data
from(
select row_id,insertnumber
,JSON_VALUE(req,'$.method' ) method
,JSON_VALUE(req,'$.url' ) url
,JSON_VALUE(resp,'$.content.text' ) content
from(
select row_id ,insertnumber
,JSONExtractArrayRaw(request)[1] req
,JSONExtractArrayRaw(response)[1] resp
from ext_similarweb.audience_demographics
where  valid = 1
)
where method = 'GET'
)
)
) where dublicate = 0



--ext_similarweb.audience_geography
create or replace table ext_similarweb.audience_geography_data ENGINE = MergeTree ORDER by row_id partition by insertnumber as
select * from (
select *
,	lower(hex(MD5(api_endpoint))) api_endpoint_h
,	lower(hex(MD5(api_query))) api_query_h
,	lower(hex(MD5(response_data))) response_data_h
, 1 - row_number() over (partition by insertnumber,api_query_h, response_data_h) dublicate
from (
select *
, domain(url) dom
, path(url) api_endpoint
, queryString(decodeURLComponent(url)) api_query
, content response_data
from(
select row_id,insertnumber
,JSON_VALUE(req,'$.method' ) method
,JSON_VALUE(req,'$.url' ) url
,JSON_VALUE(resp,'$.content.text' ) content
from(
select row_id ,insertnumber
,JSONExtractArrayRaw(request)[1] req
,JSONExtractArrayRaw(response)[1] resp
from ext_similarweb.audience_geography
where  valid = 1
)
where method = 'GET'
)
)
) where dublicate = 0



--ext_similarweb.website_performance
create or replace table ext_similarweb.website_performance_data ENGINE = MergeTree ORDER by row_id partition by insertnumber as
select * from (
select *
,	lower(hex(MD5(api_endpoint))) api_endpoint_h
,	lower(hex(MD5(api_query))) api_query_h
,	lower(hex(MD5(response_data))) response_data_h
, 1 - row_number() over (partition by insertnumber,api_query_h, response_data_h) dublicate
from (
select *
, domain(url) dom
, path(url) api_endpoint
, queryString(decodeURLComponent(url)) api_query
, content response_data
from(
select row_id,insertnumber
,JSON_VALUE(req,'$.method' ) method
,JSON_VALUE(req,'$.url' ) url
,JSON_VALUE(resp,'$.content.text' ) content
from(
select row_id ,insertnumber
,JSONExtractArrayRaw(request)[1] req
,JSONExtractArrayRaw(response)[1] resp
from ext_similarweb.website_performance
where  valid = 1
)
where method = 'GET'
)
)
) where dublicate = 0