CREATE TABLE IF NOT EXISTS "users"(
  "username" TEXT,
  "age" INTEGER,
  "city" TEXT
);
COPY users FROM '/data/users.csv' DELIMITER ',' CSV HEADER;

CREATE TABLE IF NOT EXISTS "logins"(
  "username" TEXT,
  "login" DATE,
  "type" TEXT
);
COPY logins FROM '/data/logins.csv' DELIMITER ',' CSV HEADER;
