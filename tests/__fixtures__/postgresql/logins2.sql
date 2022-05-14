CREATE TABLE IF NOT EXISTS "logins2"(
  "username" TEXT,
  "login_text" TEXT,
  "type" TEXT
);

COPY logins2 FROM '/data/logins2.csv' DELIMITER ',' CSV HEADER;
