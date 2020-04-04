-- drop table phones
DROP TABLE IF EXISTS bedpage.phones;

-- create table for bedpage ads
create table if not exists bedpage.phones ( 
  id numeric,
  phone text
);

-- insert records that are not already in the table
insert into bedpage.phones (
  id,
  phone
)
select
  distinct
  id,
  unnest(phone)
from bedpage.ads
where phone is not null and not exists (
  select id
  from bedpage.phones
  where id = bedpage.ads.id
);
