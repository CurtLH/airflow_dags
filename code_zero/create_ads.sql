-- connect to the database
\c bedpage

-- create a table for the bedpage ads
CREATE TABLE ads_raw (
  id SERIAL PRIMARY KEY,
  datetime_load TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  s3_key VARCHAR,
  sha256 VARCHAR UNIQUE NOT NULL,
  ad JSONB
);

