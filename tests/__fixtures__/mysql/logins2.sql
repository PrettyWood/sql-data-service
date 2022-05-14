CREATE TABLE IF NOT EXISTS logins2 (
  username VARCHAR(255),
  login_text VARCHAR(255),
  type VARCHAR(255)
);

LOAD DATA INFILE '/data/logins2.csv'
INTO TABLE logins2
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(username, login_text, @type)
SET
type = NULLIF(@type,'')
;
