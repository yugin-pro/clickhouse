   INSERT INTO s3.hr_report_gmail_jobs
   select * from datamart.hr_report_gmail_jobs final
   settings s3_truncate_on_insert = 1
  