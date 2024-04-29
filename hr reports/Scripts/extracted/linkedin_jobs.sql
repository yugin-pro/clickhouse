--extrat threads from row mails

--create or replace table extracted.linkedin_jobs ENGINE = ReplacingMergeTree ORDER by trackingId as
--truncate table extracted.linkedin_jobs
insert into table extracted.linkedin_jobs
select * except(included_api_chunk_stored_json)
, JSON_VALUE(included_api_chunk_stored_json, '$.jobPostingTitle') jobPostingTitle
, JSON_VALUE(included_api_chunk_stored_json, '$.jobPostingUrn') jobPostingUrn
, JSON_VALUE(included_api_chunk_stored_json, '$.trackingId') trackingId
, JSON_VALUE(included_api_chunk_stored_json, '$.primaryDescription.text') company
, JSON_VALUE(included_api_chunk_stored_json, '$.secondaryDescription.text') region
, JSON_VALUE(included_api_chunk_stored_json, '$.footerItems[0].timeAt') timeAt
, JSON_VALUE(included_api_chunk_stored_json, '$.footerItems[0].text.text') text_0
, JSON_VALUE(included_api_chunk_stored_json, '$.footerItems[1].text.text') text_1
, JSON_VALUE(included_api_chunk_stored_json, '$.footerItems[2].text.text') text_2
, JSON_VALUE(included_api_chunk_stored_json, '$.logo.actionTarget') actionTarget
, JSON_VALUE(included_api_chunk_stored_json, '$.title.text') title
from extracted.linkedin_20240428
array join included_api_chunk_stored_json
settings function_json_value_return_type_allow_complex=true;