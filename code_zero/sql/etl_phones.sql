-- create table for bedpage phones
CREATE TABLE IF NOT EXISTS bedpage.phones (id numeric, phone text);

-- insert records that are not already in the table
INSERT INTO bedpage.phones (id, phone)
SELECT DISTINCT id,
                unnest(phone)
FROM bedpage.ads
WHERE phone IS NOT NULL
  AND NOT EXISTS
    (SELECT id
     FROM bedpage.phones
     WHERE id = bedpage.ads.id );

