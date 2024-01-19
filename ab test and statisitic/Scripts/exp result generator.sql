/*exp result generator*/

create or replace table datamart.experiment_data_generator_looker engine=MergeTree order by n as
select n,variant,device,os,is_conversion 
, floor(n/10,0) + 1 group_10
, floor(n/100,0) + 1 group_100
, floor(n/1000,0) + 1 group_1000
,is_conversion * sum(is_conversion) over (partition by variant order by n asc) as runnig_total_conversion
from (
select 
row_number() over() n,
 floor(randUniform(0,100),0) rn
,'a' variant
,if(rn <  50,'mobile','pc') device
,case
	when device = 'pc' and rn <  70 then 'windows'
	when device = 'mobile' and rn <  40 then 'ios'
	when device = 'mobile' and rn >  50 then 'android'
	else 'other'
end as os
,if(floor(randUniform(1,99),1) <= floor(randNormal(7, 2),1) ,1,0) is_conversion
from numbers(5700) 
union all
select 
row_number() over() n,
 floor(randUniform(0,100),0) rn
,'b' variant
,if(rn <  50,'mobile','pc') device
,case
	when device = 'pc' and rn <  70 then 'windows'
	when device = 'mobile' and rn <  40 then 'ios'
	when device = 'mobile' and rn >  50 then 'android'
	else 'other'
end as os
,if(floor(randUniform(1,99),1) <= floor(randNormal(7, 2),1) ,1,0) is_conversion
from numbers(5460) 
)