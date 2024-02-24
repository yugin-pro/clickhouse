--message_invites


create or replace table analyt_jobmails.message_invites ENGINE = MergeTree ORDER by id as
select 
id
, match(PlainBody,'invite') invite_1
from extracted.jobmails_messages

