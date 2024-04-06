--dictionary upload

--create new
create table raw_manual.linkedin_organisation_by_email
(`email` String, `company` String)
engine = Log;

--delete previos
truncate table raw_manual.linkedin_organisation_by_email

--add rows
insert into table raw_manual.linkedin_organisation_by_email format CSV
amplifon@hcm.oraclecloud.com,Amplifon
babcock-jobnotification@noreply12.jobs2web.com,Babcock
klm-careers@noreply2.jobs2web.com,KLM Royal Dutch Airlines
msd@myworkday.com,MSD Netherlands
niet-beantwoorden@werkzoeken.nl,niet-beantwoorden
nike@myworkday.com,Nike
peaks@emails.homerun.co,Peaks
rabobank@myworkday.com,Rabobank
relx@myworkday.com,Elsevier
skins-cosmetics@emails.homerun.co,Skins Cosmetics
spectris@myworkday.com,Malvern Panalytical
takeaway@myworkday.com,Just Eat Takeaway.com
turntwo@emails.homerun.co,Turntwo