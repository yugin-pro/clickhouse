   INSERT INTO s3.olap_hr_report_gmail_communications
   select * from olap.hr_report_gmail_communications
   settings s3_truncate_on_insert = 1
  
    