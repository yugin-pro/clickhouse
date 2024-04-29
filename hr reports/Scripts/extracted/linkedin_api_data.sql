--extrat threads from row mails

create or replace table extracted.linkedin_20240428 ENGINE = MergeTree ORDER by id_chunk_stored as
select category,id_chunk_stored
,JSON_VALUE(api_chunk_stored_json, '$.request') request
,JSON_VALUE(api_chunk_stored_json, '$.ts') request_ts
,JSON_VALUE(api_chunk_stored_json,'$.data.metadata.keywords') metadata_keywords
,JSON_VALUE(api_chunk_stored_json, '$.data.paging.count') paging_count
,JSON_VALUE(api_chunk_stored_json, '$.data.paging.start') paging_start
,JSON_VALUE(api_chunk_stored_json, '$.data.paging.total') paging_total
,JSONExtractArrayRaw(JSON_VALUE(api_chunk_stored_json, '$.included')) included_api_chunk_stored_json
from (
select category, tupleElement(chunk_stored_json, 1) id_chunk_stored
, tupleElement(chunk_stored_json, 2) api_chunk_stored_json 
from (
select category
	,JSONExtractKeysAndValuesRaw(stored_json) chunk_stored_json
from (
select tupleElement(ess, 1) category
, tupleElement(ess, 2) stored_json
from(
select JSONExtractKeysAndValuesRaw(json_result) ess
from raw_firebase.decodeurl_51e7f_default_rtdb_linkedin_20240428
) array
join ess
)
) array
join chunk_stored_json
)
settings function_json_value_return_type_allow_complex=true;