   INSERT INTO s3.hr_report_linkedin_jobs_funnel
   select * from datamart.hr_report_linkedin_jobs_funnel
   settings s3_truncate_on_insert = 1