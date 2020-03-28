
drop table bedpage.ads;

-- create table for bedpage ads
 create table if not exists bedpage.ads ( id numeric primary key,
post_id varchar,
date_published timestamp,
date_modified timestamp,
title varchar,
body varchar,
url varchar,
city varchar,
category varchar,
poster_age varchar,
email varchar,
phone varchar,
mobile varchar,
location varchar );

-- insert records that are not already in the table
 insert
	into
	bedpage.ads (id,
	post_id,
	date_published,
	date_modified,
	title,
	body,
	url,
	city,
	category,
	poster_age,
	email,
	phone,
	mobile,
	location)
select
	id,
	ad -> 'details' ->> 'post id' as post_id,
	(ad -> 'details' ->> 'datePublished')::timestamp as date_published,
	(ad -> 'details' ->> 'dateModified')::timestamp as date_modified,
	ad ->> 'title' as title,
	ad ->> 'body' as body,
	ad -> 'details' ->> 'url' as url,
	substring(split_part(ad -> 'details' ->> 'url', '.', 1), 9) as city,
	split_part(ad -> 'details' ->> 'url', '/', 4) as category,
	ad -> 'details' ->> 'poster''s age' as poster_age,
	ad -> 'details' ->> 'email' as email,
	ad ->> 'phone' as phone,
	ad -> 'details' ->> 'mobile' as mobile,
	ad -> 'details' ->> 'location' as location
from
	bedpage.raw
where
	not exists (
	select
		id
	from
		bedpage.ads
	where
		id = bedpage.raw.id );