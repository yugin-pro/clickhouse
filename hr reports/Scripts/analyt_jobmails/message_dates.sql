--message_dates


create or replace table analyt_jobmails.message_dates ENGINE = MergeTree ORDER by id as
select 
id
, parseDateTimeBestEffort(message_date) ts
, toDate(ts) date 
, toHour(ts) hours
, toMinute(ts) minutes 
from extracted.jobmails_messages

