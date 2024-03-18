update listing_snapshot t1
set dbt_valid_to = next_date
from (
select listing_id, scraped_date, lead(scraped_date) over (partition by listing_id order by scraped_date) as next_date
from listing_snapshot) t2
where t1.listing_id=t2.listing_id and t1.scraped_date=t2.scraped_date;

update host_snapshot t1
set dbt_valid_to = next_date
from (
select host_id, scraped_date, lead(scraped_date) over (partition by host_id order by scraped_date) as next_date
from listing_snapshot) t2
where t1.host_id=t2.host_id and t1.scraped_date=t2.scraped_date;

update property_type_snapshot t1
set dbt_valid_to = next_date
from (
select distinct(room_type), scraped_date, lead(scraped_date) over (partition by host_id order by scraped_date) as next_date
from property_type_snapshot) t2
where t1.room_type=t2.room_type and t1.scraped_date=t2.scraped_date;