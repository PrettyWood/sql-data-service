CREATE TABLE IF NOT EXISTS labels2 (
  Label VARCHAR(255),
  Cartel VARCHAR(255),
  Value INT
);

LOAD DATA INFILE '/data/labels2.csv'
INTO TABLE labels2
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
