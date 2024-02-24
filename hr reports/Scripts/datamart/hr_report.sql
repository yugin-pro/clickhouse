--hr_report 


create or replace table datamart.hr_report ENGINE = MergeTree ORDER by application_id as
select 
distinct id as application_id
, 'company' organisation
, 'vacansy' vacansy
, 0 is_declined
, 0 is_invited
,'2024-02-12' sended_date
,'2024-02-13' declined_date
,'2024-02-15' invited_date
from extracted.jobmails_messages

