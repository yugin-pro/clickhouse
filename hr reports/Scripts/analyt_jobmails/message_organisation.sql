--message_organisation


create or replace table analyt_jobmails.message_organisation ENGINE = MergeTree ORDER by id as
select 
id,thread_id,if(empty(organisation),'XXXXX',trim(organisation)) organisation
from (
select 
--*
id,thread_id
,case
	when message_from_domain = 'successfactors.eu' then message_from_name
	when empty(at_company) and  empty(to_company) and  empty(by_company) and  empty(by_name) and empty(by_email_start) then message_from_name 
	when empty(at_company) and  empty(to_company) and  empty(by_company) and  empty(by_name) then by_email_start
	when empty(at_company) and  empty(to_company) and  empty(by_company) then by_name
	when empty(at_company) and  empty(to_company) then by_company
	when empty(at_company) then to_company	
	else at_company
end as organisation
from (
select
--a.*
id,thread_id,message_from_name,message_from_domain
,trim(extract(subject, 'to ([0-9a-zA-Z ]*)')) to_company
,trim(extract(subject, 'at ([0-9a-zA-Z ]*)')) at_company
,trim(extract(subject, 'by ([0-9a-zA-Z ]*)')) by_company
,trim(extract(message_from_name, ',([0-9a-zA-Z ]*)')) by_name
,c.company by_email_start
from analyt_jobmails.message_parsed a
left join raw_manual.linkedin_organisation_by_email c on a.message_from_email = c.email
where 
a.is_replay = 0 and a.is_service = 1
)
--order by message_from_domain

union all

select id,thread_id,company from (
select id,thread_id,message_from_domain
from analyt_jobmails.message_parsed
join analyt_jobmails.message_status using (id)
where 
	is_replay = 0 and is_service = 0
) a
left join raw_manual.linkedin_organisation b on a.message_from_domain = b.domain

)
