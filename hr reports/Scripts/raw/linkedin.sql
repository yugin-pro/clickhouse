CREATE TABLE raw_firebase.decodeurl_51e7f_default_rtdb_jobmails
(

    `json_result` String
)
ENGINE = URL('https://decodeurl-51e7f-default-rtdb.europe-west1.firebasedatabase.app/jobmails.json',
 'JSONAsString');