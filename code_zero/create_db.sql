-- drop the existing database
DROP DATABASE IF EXISTS bedpage;

-- recreate the database
CREATE DATABASE bedpage;

-- connect to the database
\c bedpage

-- create a table for the bedpage ads
CREATE TABLE ads (
  id SERIAL PRIMARY KEY,
  sha256 varchar,
  s3_key varchar,
  datetime_load timestamp default CURRENT_TIMESTAMP,
  ad_id varchar,
  city varchar,
  category varchar,
  url varchar,
  title varchar,
  body varchar,
  published_date timestamp,
  modified_date timestamp,
  phone varchar,
  email varchar,
  location varchar,
  age varchar
);

