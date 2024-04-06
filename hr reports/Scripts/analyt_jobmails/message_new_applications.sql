--message_new_applications


create or replace table analyt_jobmails.message_new_applications ENGINE = MergeTree ORDER by id as
select id,
m1 or m2 or m3 or m4  or m5 or m6  or m7 or m8 or m9  or m10 is_new
from (
select
id
, match(PlainBody,'Your application was sent') m1
, match(Subject,'hanks? you for applying') m2
, match(Subject,'application successful') m3
, match(Subject,'application was successfully submitted') m4
, match(Subject,'pplication confirmation') m5 
, match(Subject,'hank you for your application') m6
, match(Subject,'pplication received') m7
, match(Subject,'sollicitatie') m8
, match(Subject,'we received') m9
, match(PlainBody,'we received your application') m10
, match(Subject,'elcome to') m10
, match(lower(Subject),'welcome to') m10
, match(lower(Subject),'thanks for applying at') m11
from extracted.jobmails_messages
)

-- is_new = 0
select * from analyt_jobmails.message_parsed
join analyt_jobmails.message_new_applications using(id)
where is_new = 0 and id not in (select distinct id from analyt_jobmails.message_organisation)

create or replace table analyt_jobmails.message_new_applications ENGINE = MergeTree ORDER by id as
select * from (
select * 
, row_number() over (partition by message_from order by ts asc) is_new
, 'direct' comunication_type
from (
select 
	id , message_from , Subject subject, ts
from analyt_jobmails.message_parsed
join analyt_jobmails.message_dates using(id)
where not match(message_from, 'yugin\.pro|linkedin|greenhouse')
)
) where is_new = 1

union all
-- by services
select * from (
select 
	id, message_from , Subject subject , ts
	, row_number() over (partition by Subject  order by ts asc) is_new
	, 'services' comunication_type
from analyt_jobmails.message_parsed
join analyt_jobmails.message_dates using(id)
where match(message_from, 'linkedin|greenhouse') and (Subject not like '%viewed%' and Subject not like '%Your application at%' and Subject not like '%mportant information%')
) where is_new = 1