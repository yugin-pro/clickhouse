CREATE TABLE raw_firebase.decodeurl_51e7f_default_rtdb_jobmails
(

    `json_result` String
)
ENGINE = URL('https://firebasedatabase.app/jobmails.json',
 'JSONAsString');