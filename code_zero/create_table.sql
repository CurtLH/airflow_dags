-- create table for bedpage data
CREATE TABLE bedpage.raw2 (
  id SERIAL PRIMARY KEY,
  datetime_load TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  s3_key VARCHAR,
  filename VARCHAR,
  sha256 VARCHAR UNIQUE NOT NULL,
  data JSONB
 );
