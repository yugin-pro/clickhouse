--extrat threads from row mails

create or replace table extracted.jobmails_messages ENGINE = MergeTree ORDER by id as
select id thread_id
,JSON_VALUE(messages,'$.Id' ) id
,JSON_VALUE(messages,'$.Bcc' ) bcc
,JSON_VALUE(messages,'$.Cc' ) cc
,JSON_VALUE(messages,'$.Date' ) message_date
,JSON_VALUE(messages,'$.From' ) message_from
,JSON_VALUE(messages,'$.PlainBody' ) PlainBody
,JSON_VALUE(messages,'$.ReplyTo' ) ReplyTo
,JSON_VALUE(messages,'$.Subject' ) Subject
from (
select 
 JSON_VALUE(json_result,'$.Id' ) id
 ,JSONExtractArrayRaw(JSONExtractArrayRaw(JSON_QUERY(json_result,'$.Messages'))[1]) messages
from raw_firebase.decodeurl_51e7f_default_rtdb_jobmails
) array join messages