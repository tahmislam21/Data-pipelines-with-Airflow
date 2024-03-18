-- Part 3a

WITH listing_neighbourhood_monthly_stats AS (
    SELECT
        lga_name AS listing_neighbourhood,
        month_year,
        SUM(median_age_people) / COUNT(*) AS median_age,
        SUM(tot_p_p) / COUNT(*) AS total_population,
        SUM(age_0_4_yr_p) / COUNT(*) AS age_0_4_population,
        SUM(age_5_14_yr_p) / COUNT(*) AS age_5_14_population,
        SUM(age_15_19_yr_p) / COUNT(*) AS age_15_19_population,
        SUM(age_20_24_yr_p) / COUNT(*) AS age_20_24_population,
        SUM(age_25_34_yr_p) / COUNT(*) AS age_25_34_population,
        AVG(CASE WHEN has_availability = TRUE THEN ((30 - availability_30) * price) END) AS avg_estimated_revenue_per_active_listing
    FROM warehouse.fact_listings
    LEFT JOIN warehouse.dim_lga ON warehouse.fact_listings.lga_code = warehouse.dim_lga.lga_code
    LEFT JOIN warehouse.dim_date ON warehouse.fact_listings.date_id = warehouse.dim_date.date_id
    LEFT JOIN warehouse.dim_listings ON warehouse.fact_listings.listing_id = warehouse.dim_listings.listing_id
    GROUP BY listing_neighbourhood, month_year
)
SELECT
    listing_neighbourhood,
    SUM(median_age) / COUNT(*) AS median_age,
    SUM(total_population) / COUNT(*) AS total_population,
    (SUM(age_0_4_population) / COUNT(*)) + (SUM(age_5_14_population) / COUNT(*)) + (SUM(age_15_19_population) / COUNT(*)) + (SUM(age_20_24_population) / COUNT(*)) + (SUM(age_25_34_population) / COUNT(*)) AS under_35_total_population,
    SUM(avg_estimated_revenue_per_active_listing) AS estimated_revenue_per_active_listing
FROM listing_neighbourhood_monthly_stats
GROUP BY listing_neighbourhood
ORDER BY estimated_revenue_per_active_listing DESC;


-- Part 3b

WITH listing_neighbourhoods_est_revenue AS (
    WITH listing_neighbourhood_monthly_stats AS (
        SELECT
            lga_name AS listing_neighbourhood,
            month_year,
            AVG(CASE WHEN has_availability = TRUE THEN ((30 - availability_30) * price) END) AS avg_estimated_revenue_per_active_listing
        FROM warehouse.fact_listings
        LEFT JOIN warehouse.dim_lga ON warehouse.fact_listings.lga_code = warehouse.dim_lga.lga_code
        LEFT JOIN warehouse.dim_listings ON warehouse.fact_listings.listing_id = warehouse.dim_listings.listing_id
        GROUP BY listing_neighbourhood, month_year
    )
    SELECT
        listing_neighbourhood,
        SUM(avg_estimated_revenue_per_active_listing) AS estimated_revenue_per_active_listing,
        ROW_NUMBER() OVER (ORDER BY estimated_revenue_per_active_listing DESC) AS rk_est_revenue
    FROM listing_neighbourhood_monthly_stats
    GROUP BY listing_neighbourhood
),
listing_neighbourhood_stays AS (
    SELECT
        lga_name AS listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        SUM(30 - availability_30) AS total_stays,
        ROW_NUMBER() OVER (PARTITION BY listing_neighbourhood ORDER BY total_stays DESC) AS rank_stays
    FROM warehouse.fact_listings
    LEFT JOIN warehouse.dim_lga ON warehouse.fact_listings.lga_code = warehouse.dim_lga.lga_code
    LEFT JOIN warehouse.dim_listings ON warehouse.fact_listings.listing_id = warehouse.dim_listings.listing_id
    GROUP BY listing_neighbourhood, property_type, room_type, accommodates
)
SELECT
    listing_neighbourhoods_est_revenue.listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    total_stays,
    estimated_revenue_per_active_listing
FROM listing_neighbourhoods_est_revenue
LEFT JOIN listing_neighbourhood_stays ON listing_neighbourhoods_est_revenue.listing_neighbourhood = listing_neighbourhood_stays.listing_neighbourhood
WHERE rk_est_revenue <= 5 AND rank_stays = 1
ORDER BY estimated_revenue_per_active_listing DESC;



-- Part 3c

create or replace view datamart.hosts_lga_same_as_listing_lga as 
with hosts_multiple_listings as (
    select 
    original_host_id,
    count(distinct(original_listing_id)) as num_listings
    from warehouse.fact_listings
    left join warehouse.dim_listings on warehouse.fact_listings.listing_id= warehouse.dim_listings.listing_id
    left join warehouse.dim_host on warehouse.fact_listings.host_id = warehouse.dim_host.host_id
    group by original_host_id
    having num_listings > 1
),
hosts_lga as (
    select 
    lga_name as host_lga,
    warehouse.dim_host.host_id,
    original_host_id,
    suburb_name
    from warehouse.fact_listings
    left join warehouse.dim_lga on warehouse.fact_listings.lga_code = warehouse.dim_lga.lga_code
    left join warehouse.dim_host on warehouse.fact_listings.host_id = warehouse.dim_host.host_id
    left join warehouse.dim_suburb on warehouse.fact_listings.suburb_id = warehouse.dim_suburb.suburb_id
    where suburb_name is not null
),
listings_lga as (
    select
    warehouse.dim_host.host_id,
    original_host_id,
    original_listing_id,
    lga_name as listing_lga
    from warehouse.fact_listings
    left join warehouse.dim_lga on warehouse.fact_listings.lga_code = warehouse.dim_lga.lga_code
    left join warehouse.dim_host on warehouse.fact_listings.host_id = warehouse.dim_host.host_id
    left join warehouse.dim_listings on warehouse.fact_listings.listing_id= warehouse.dim_listings.listing_id
)
select 
distinct(hosts_lga.host_id),
hosts_lga.original_host_id,
host_lga,
listing_lga,
case when host_lga = listing_lga then TRUE else FALSE END as hosts_lga_same_as_listing_lga,
num_listings
from hosts_lga
inner join listings_lga on hosts_lga.host_id = listings_lga.host_id
inner join hosts_multiple_listings on hosts_lga.original_host_id = hosts_multiple_listings.original_host_id;

-- view datamart.hosts_lga_same_as_listing_lga
select * from datamart.hosts_lga_same_as_listing_lga;

-- Finding the total count of cases where host lga was same as listing lga
select 
hosts_lga_same_as_listing_lga,
count(*) as total_count
from datamart.hosts_lga_same_as_listing_lga
group by hosts_lga_same_as_listing_lga;
