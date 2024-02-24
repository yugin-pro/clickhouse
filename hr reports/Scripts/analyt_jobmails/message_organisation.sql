--message_organisation


create or replace table analyt_jobmails.message_organisation ENGINE = MergeTree ORDER by id as
select 
id
,extract(message_from, '@([^\> ]+)') mail_domain
,extract(message_from, '(.*)\<') mail_company
,extract(Subject, 'was\ sent\ to\ (.*)') easyapply_company
from extracted.jobmails_messages

