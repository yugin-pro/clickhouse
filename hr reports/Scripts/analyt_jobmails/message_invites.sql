--message_invites


create or replace table analyt_jobmails.message_invites ENGINE = MergeTree ORDER by id as
select id,
m1 or m2  is_invitation
from (
select
id
, match(PlainBody,'interview') m1
, match(Subject,'interview') m2
--, match(Subject,'application successful') m3
--, match(Subject,'application was successfully submitted') m4
--, match(Subject,'pplication confirmation') m5 
--, match(Subject,'hank you for your application for') m6
--, match(Subject,'pplication received') m7
from extracted.jobmails_messages
)