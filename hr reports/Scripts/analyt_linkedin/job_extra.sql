--job_extra


create or replace table analyt_linkedin.job_extra ENGINE = ReplacingMergeTree(is_apropriate) ORDER by id as
select 
 id
,match(title,'(bi specialist)|(power bi)|(reporting)|(dataspecialist)|(conversion)|(analysis)|(analist)|(analyst)|(cro )|(sea )|(marketing automation)') is_apropriate
from analyt_linkedin.job_parsed final

