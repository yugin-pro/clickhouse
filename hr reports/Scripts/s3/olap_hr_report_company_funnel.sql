   INSERT INTO s3.olap_hr_report_company_funnel
   select * from olap.hr_report_company_funnel
   settings s3_truncate_on_insert = 1
  