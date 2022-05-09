CREATE TABLE IF NOT EXISTS logins (
  username VARCHAR(255),
  login DATE,
  type VARCHAR(255)
);

LOAD DATA INFILE '/data/logins.csv'
INTO TABLE logins 
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(username, login, @type)
SET
type = NULLIF(@type,'')
;
