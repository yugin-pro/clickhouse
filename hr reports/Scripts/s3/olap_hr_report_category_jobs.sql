   INSERT INTO s3.olap_hr_report_category_jobs
   select * from olap.hr_report_category_jobs
   settings s3_truncate_on_insert = 1
  
