--message_new_applications


create or replace table analyt_jobmails.message_new_applications ENGINE = MergeTree ORDER by id as
select id,
m1 or m2 or m3 or m4  or m5 or m6  or m7 or m8  is_new
from (
select
id
, match(PlainBody,'Your application was sent') m1
, match(Subject,'hank you for applying') m2
, match(Subject,'application successful') m3
, match(Subject,'application was successfully submitted') m4
, match(Subject,'pplication confirmation') m5 
, match(Subject,'hank you for your application for') m6
, match(Subject,'pplication received') m7
, match(Subject,'sollicitatie') m8
from extracted.jobmails_messages
)
