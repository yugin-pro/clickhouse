--47e858a2dca1ace46485463ca1e21872
create or replace table ext_similarweb.website_performance_data_endpoint_47e858a2dca1ace46485463ca1e21872 ENGINE = MergeTree ORDER by row_id as
select *  EXCEPT ( data_rows)
,JSON_VALUE(json,'$.mainDomainName' ) mainDomainName
,JSON_VALUE(json,'$.title' ) title
,JSON_VALUE(json,'$.globalRanking' ) globalRanking
,JSON_VALUE(json,'$.highestTrafficCountry' ) highestTrafficCountry
,JSON_VALUE(json,'$.highestTrafficCountryRanking' ) highestTrafficCountryRanking
,JSON_VALUE(json,'$.category' ) category
,JSON_VALUE(json,'$.categoryRanking' ) categoryRanking
,JSON_VALUE(json,'$.monthlyVisits' ) monthlyVisits
,JSON_VALUE(json,'$.country' ) country
 from (
select * 
,tupleElement(row_tuple,1) site_name
,tupleElement(row_tuple,2) json
from (
select *
,JSONExtractKeysAndValuesRaw(response_data) data_rows
from ext_similarweb.website_performance_data
where api_endpoint_h = '47e858a2dca1ace46485463ca1e21872'
) array join data_rows as row_tuple
)


--12a4d32f4d257caff4a5db29224bff1d
create or replace table ext_similarweb.website_performance_data_endpoint_12a4d32f4d257caff4a5db29224bff1d ENGINE = MergeTree ORDER by row_id as
select *  EXCEPT ( data_rows)
,JSON_VALUE(data_rows,'$.Domain' ) Domain_name
,JSON_VALUE(data_rows,'$.TotalVisits' ) TotalVisits
,JSON_VALUE(data_rows,'$.Change' ) change_value
,JSON_VALUE(data_rows,'$.ShareOfVisits' ) ShareOfVisits
 from (
select * 
from (
select *
,JSONExtractArrayRaw(JSONExtractArrayRaw(JSON_QUERY(response_data,'$.Data'))[1]) data_rows
from ext_similarweb.website_performance_data
where api_endpoint_h = '12a4d32f4d257caff4a5db29224bff1d'
) array join data_rows 
)


--104360b9d6392a6013ac8a046bbd4dbf
create or replace table ext_similarweb.website_performance_data_endpoint_104360b9d6392a6013ac8a046bbd4dbf ENGINE = MergeTree ORDER by row_id as
select * 
,JSON_VALUE(data_rows,'$.Source' ) source_name
,JSON_VALUE(data_rows,'$.Domain' ) domain_name
,JSON_VALUE(data_rows,'$.BounceRate.Value' ) BounceRate
,JSON_VALUE(data_rows,'$.AvgMonthVisits.Value' ) AvgMonthVisits
,JSON_VALUE(data_rows,'$.AvgVisitDuration.Value' ) AvgVisitDuration
,JSON_VALUE(data_rows,'$.PagesPerVisit.Value' ) PagesPerVisit
,JSON_VALUE(data_rows,'$.TotalPagesViews.Value' ) TotalPagesViews
 from (
select * 
from (
select *
,JSONExtractArrayRaw(JSONExtractArrayRaw(JSON_QUERY(response_data,'$.Data'))[1]) data_rows
from ext_similarweb.website_performance_data
where api_endpoint_h = '104360b9d6392a6013ac8a046bbd4dbf'
) array join data_rows 
)