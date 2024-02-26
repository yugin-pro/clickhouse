--job_extra


create or replace table analyt_linkedin.job_extra ENGINE = ReplacingMergeTree() ORDER by id as
select 
 id
,match(title,'(nalist)|(nalyst)|(cro )|(sea )') is_apropriate
from analyt_linkedin.job_parsed final

