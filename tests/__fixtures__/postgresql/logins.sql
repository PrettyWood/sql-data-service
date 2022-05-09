CREATE TABLE IF NOT EXISTS "logins"(
  "username" TEXT,
  "login" DATE,
  "type" TEXT
);

COPY logins FROM '/data/logins.csv' DELIMITER ',' CSV HEADER;
