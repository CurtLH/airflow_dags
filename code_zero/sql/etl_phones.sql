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
  raw_id,
  unnest(string_to_array(phone::text, ';'))
from bedpage.ads
where not exists (
  select id
  from bedpage.phones
  where id = bedpage.ads.raw_id
);
