CREATE TABLE IF NOT EXISTS "labels2"(
  "Label" TEXT,
  "Cartel" TEXT,
  "Value" INTEGER
);

COPY labels2 FROM '/data/labels2.csv' DELIMITER ',' CSV HEADER;
