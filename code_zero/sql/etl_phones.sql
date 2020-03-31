-- create table for bedpage ads
create table if not exists bedpage.phones2 ( 
  id numeric,
  phone text
);

-- insert records that are not already in the table
insert into bedpage.phones2 (
  id,
  phone
)
select
  raw_id,
  unnest(string_to_array(phone::text, ';'))
from bedpage.ads2
where not exists (
  select id
  from bedpage.phones2
  where id = bedpage.ads2.raw_id
);
