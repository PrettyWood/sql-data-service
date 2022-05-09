CREATE TABLE IF NOT EXISTS "labels"(
  "Label" TEXT,
  "Cartel" TEXT,
  "Value" INTEGER
);

COPY labels FROM '/data/labels.csv' DELIMITER ',' CSV HEADER;
