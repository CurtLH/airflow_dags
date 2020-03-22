-- drop the existing database
DROP DATABASE IF EXISTS escorts;

-- create the database
CREATE DATABASE escorts;

-- connect to the database
\c escorts

-- create schema and initial table for bedpage
CREATE SCHEMA bedpage
  CREATE TABLE raw (
    id SERIAL PRIMARY KEY,
    datetime_load TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    s3_key VARCHAR,
    sha256 VARCHAR UNIQUE NOT NULL,
    ad JSONB
  )
;
