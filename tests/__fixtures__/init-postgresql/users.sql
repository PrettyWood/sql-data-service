CREATE TABLE IF NOT EXISTS "users"(
  "username" TEXT,
  "age" INTEGER,
  "city" TEXT
);
COPY users FROM '/data/users.csv' DELIMITER ',' CSV HEADER;
