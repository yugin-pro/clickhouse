--message_declined


create or replace table analyt_jobmails.message_declined ENGINE = MergeTree ORDER by id as
select 
id
, match(PlainBody,'not.+to.+proceed') declined_1
from extracted.jobmails_messages

