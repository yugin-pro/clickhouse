   INSERT INTO s3.olap_linkedin_vacancies_funnel
   select * from olap.linkedin_vacancies_funnel
   settings s3_truncate_on_insert = 1
  
   