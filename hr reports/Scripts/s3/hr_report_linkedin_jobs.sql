   INSERT INTO s3.hr_report_linkedin_jobs
   select * from datamart.hr_report_linkedin_jobs
   settings s3_truncate_on_insert = 1