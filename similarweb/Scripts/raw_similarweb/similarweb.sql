
/*create DB*/
CREATE DATABASE IF NOT EXISTS raw_similarweb
CREATE DATABASE IF NOT EXISTS ext_similarweb

/*create tables*/
create TABLE raw_similarweb.audience_demographics
(
    json String
)
ENGINE = MergeTree()
order by json

