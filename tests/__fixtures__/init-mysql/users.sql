CREATE TABLE IF NOT EXISTS users (
  username VARCHAR(255),
  age INT,
  city VARCHAR(255)
);

INSERT INTO users
  (username, age, city)
VALUES
  ('Eric',30,'Paris'),
  ('Chiara',31,'Firenze'),
  ('Pikachu',7,'Bourg Palette');
