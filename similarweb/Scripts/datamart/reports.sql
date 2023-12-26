create or replace table datamart.similarweb_engagement_overview_table ENGINE = MergeTree ORDER by row_id as
select 
* except(api_query_url)
,toDate(parseDateTimeBestEffort(extractURLParameter(api_query_url, 'from'))) date_from
,toDate(parseDateTimeBestEffort(extractURLParameter(api_query_url, 'to'))) date_to
,extractURLParameter(api_query_url, 'timeGranularity') timeGranularity
,extractURLParameter(api_query_url, 'webSource') webSource
,extractURLParameter(api_query_url, 'keys') keys_list
from (
select 
row_id,api_query , domain_name, BounceRate, AvgMonthVisits, AvgVisitDuration, PagesPerVisit, TotalPagesViews
, concat('https://example.com/?',api_query) api_query_url
from ext_similarweb.website_performance_data_endpoint_104360b9d6392a6013ac8a046bbd4dbf 
)
