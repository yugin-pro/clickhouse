--job_extra

create or replace table analyt_linkedin.job_extra engine = MergeTree order by id as
select id, max(is_apropriate) is_apropriate from (
select id
, white_list_keyword_apropriate and not minus_keyword_detected and not company_black_list as is_apropriate 
from (
select 
 id
,match(lower(title),'(analytics)|(insight)|(automatis)|(sql)|(bi specialist)|(powerbi)|(power bi)|(reporting)|(dataspecialist)|(conversion)|(analysis)|(analist)|(analyst)|(cro )|(sea )|(marketing automation)|(customer journey)|(data)|(transformation)') white_list_keyword_apropriate
,match(lower(title),'(management)|(logistic)|(science)|(compliance )|(geo)|(dutch)|(cdd)|(biolog)|(marketeer)|(security)|(business)|(bio)|(soc\ )') minus_keyword_detected
,match(lower(company),'(^io$)|(iodigital)') company_black_list
from analyt_linkedin.job_parsed final
)
) group by id




CREATE TABLE analyt_linkedin.job_extra
(

    `id` UInt32,

    `is_apropriate` UInt8
)
ENGINE = ReplacingMergeTree(is_apropriate)
ORDER BY id
SETTINGS index_granularity = 8192;