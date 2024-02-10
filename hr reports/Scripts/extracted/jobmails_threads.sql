--extract threads from row mails

create or replace table extracted.jobmails_threads ENGINE = MergeTree ORDER by id as
select 
 JSON_VALUE(json_result,'$.Id' ) id
 ,JSON_VALUE(json_result,'$.LastMessageDate' ) LastMessageDate
 ,JSON_VALUE(json_result,'$.MessageCount' ) MessageCount
 ,JSON_VALUE(json_result,'$.Permalink' ) Permalink
from raw_firebase.decodeurl_51e7f_default_rtdb_jobmails

