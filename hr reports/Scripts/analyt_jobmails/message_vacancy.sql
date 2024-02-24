--message_vacancy


create or replace table analyt_jobmails.message_vacancy ENGINE = MergeTree ORDER by id as
select 
id
, match(PlainBody,'invite') vacancy
from extracted.jobmails_messages

