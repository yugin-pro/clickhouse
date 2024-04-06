--message_parsed
create or replace table analyt_jobmails.message_parsed ENGINE = MergeTree ORDER by id as
select 
	 id, thread_id, PlainBody body,Subject subject, message_from 
	, parseDateTimeBestEffort(message_date) ts
	, toDate(ts) date 
	, toHour(ts) hours
	, toMinute(ts) minutes 
	, extract(message_from,'\"?([^"]*)\"? ?<') message_from_name
	, if(empty(extract(message_from,'<(.*)>')),message_from
	, extract(message_from,'<(.*)>')) message_from_email
	, extract(message_from_email,'\@(.*)') message_from_domain
	, match(message_from, 'yugin\.pro') is_replay
	, match(message_from, 'linkedin|greenhouse|teamtailor|jobs2web|myworkday|successfactors|homerun|sthree|emply|smartrecruiters|oraclecloud|werkzoeken|lever') is_service
from extracted.jobmails_messages

