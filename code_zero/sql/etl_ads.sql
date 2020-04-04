-- create table for bedpage ads
create table if not exists bedpage.ads ( 
	id numeric primary key,
	post_id numeric,
	date_published timestamp,
	date_modified timestamp,
	title text,
	body text,
	url text,
	city text,
	category text,
	poster_age text,
	email text,
	phone text[],
	mobile text,
	location text
);

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
	(ad -> 'details' ->> 'post id')::numeric as post_id,
	(ad -> 'details' ->> 'datePublished')::timestamp as date_published,
	(ad -> 'details' ->> 'dateModified')::timestamp as date_modified,
	ad ->> 'title' as title,
	ad ->> 'body' as body,
	ad -> 'details' ->> 'url' as url,
	substring(split_part(ad -> 'details' ->> 'url', '.', 1), 9) as city,
	split_part(ad -> 'details' ->> 'url', '/', 4) as category,
	ad -> 'details' ->> 'poster''s age' as poster_age,
	ad -> 'details' ->> 'email' as email,
	(string_to_array(ad ->> 'phone', ';')) as phone,
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
