-- drop the existing database
DROP DATABASE IF EXISTS escorts;

-- create the database
CREATE DATABASE escorts;

-- connect to the database
\c escorts

-- create schema for bedpage data
CREATE SCHEMA bedpage;

-- revoke privileges from 'public' role
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON DATABASE escorts FROM PUBLIC;

-- create readonly role
DROP ROLE readonly;
CREATE ROLE readonly;
GRANT CONNECT ON DATABASE escorts TO readonly;
GRANT USAGE ON SCHEMA bedpage TO readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA bedpage TO readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA bedpage GRANT SELECT ON TABLES TO readonly;

-- create read/write role
DROP ROLE readwrite;
CREATE ROLE readwrite;
GRANT CONNECT ON DATABASE escorts TO readwrite;
GRANT USAGE, CREATE ON SCHEMA bedpage TO readwrite;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bedpage TO readwrite;
ALTER DEFAULT PRIVILEGES IN SCHEMA bedpage GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO readwrite;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA bedpage TO readwrite;
ALTER DEFAULT PRIVILEGES IN SCHEMA bedpage GRANT USAGE ON SEQUENCES TO readwrite;

-- create users
DROP USER user1;
DROP USER user2;
CREATE USER user1 WITH PASSWORD 'some_secret_passwd';
CREATE USER user2 WITH PASSWORD 'some_secret_passwd';

-- grant privileges to users
GRANT readonly TO user1;
GRANT readwrite TO user2;
GRANT readwrite TO curtis;
