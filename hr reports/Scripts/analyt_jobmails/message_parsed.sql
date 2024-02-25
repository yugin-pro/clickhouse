--message_parsed
create or replace table analyt_jobmails.message_parsed ENGINE = MergeTree ORDER by id as
select 
	distinct id,thread_id, PlainBody,Subject,message_from 
from extracted.jobmails_messages

