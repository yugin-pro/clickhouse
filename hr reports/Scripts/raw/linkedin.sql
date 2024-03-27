CREATE or replace TABLE raw_firebase.decodeurl_51e7f_default_rtdb_linkedin_20240224 
(
	`json_result` String
)
ENGINE = URL('https://firebasedatabase.app/linkedin/voyagerJobsDashJobCards/2024/2/24.json',
 'JSONAsString');