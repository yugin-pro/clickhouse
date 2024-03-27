--message_organisation


create or replace table analyt_jobmails.message_organisation ENGINE = MergeTree ORDER by id as
select 
*
,extract(message_from, '@([^\> ]+)') mail_domain
,extract(message_from, '(.*)\<') mail_company
,extract(Subject, 'was\ sent\ to\ (.*)') easyapply_company
,extract(Subject, 'to\ (.*)') to_company
,extract(Subject, 'at\ (.*)') at_company
from extracted.jobmails_messages
join analyt_jobmails.message_new_applications using(id)
where is_new = 1
