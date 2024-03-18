--message_declined


create or replace table analyt_jobmails.message_declined ENGINE = MergeTree ORDER by id as
select id,
m1 or m2 or m3 or m4  or m5 or m6  or m7 is_declined
from (
select
id
, match(PlainBody,'decided not to move forward') m1
, match(PlainBody,'with other candidates') m2
, match(PlainBody,'not be moving') m3
, match(PlainBody,'with other applicants') m4
, match(PlainBody,'selected a number of candidates') m5 
, match(PlainBody,'niet met') m6
, match(PlainBody,'not move') m7
from extracted.jobmails_messages
)