CREATE SCHEMA RAW;

---------------------------
-- Raw
---------------------------

CREATE TABLE RAW.Census_G01_NSW_LGA(

);

CREATE TABLE RAW.Census_G02_NSW_LGA(
    lga_code_2016 VARCHAR PRIMARY KEY,
    Median_age_persons	 INT NULL,
    Median_mortgage_repay_monthly INT NULL,
    Median_tot_prsnl_inc_weekly INT NULL,
    median_rent_weekly INT NULL,
    Median_tot_fam_inc_weekly INT NULL,
    average_num_psns_per_bedroom INT NULL,
    median_tot_hhd_inc_weekly INT NULL,
    average_household_size INT NULL

);


CREATE TABLE RAW.listings(
    listing_id NUMERIC PRIMARY KEY,
    scrape_id BIGINT NULL,
    scraped_date DATE NULL,
    host_id BIGINT NULL,
    host_name VARCHAR NULL,
    host_since VARCHAR NULL,
    host_is_superhost VARCHAR NULL,
    host_neighbourhood VARCHAR NULL,
    listing_neighbourhood VARCHAR NULL,
    property_type VARCHAR NULL,
    room_type VARCHAR NULL,
    accommodates NUMERIC NULL,
    price NUMERIC NULL,
    has_availability VARCHAR NULL,
    availability_30 NUMERIC NULL,
    number_of_reviews NUMERIC NULL,
    review_scores_rating NUMERIC NULL,
    review_scores_accuracy NUMERIC NULL,
    review_scores_cleanliness NUMERIC NULL,
    review_scores_checkin NUMERIC NULL,
    review_scores_communication NUMERIC NULL,
    review_scores_value NUMERIC NULL

);

CREATE TABLE RAW.NSW_LGA_CODE(
    lga_code int primary key,
    lga_name varchar NULL
);


CREATE TABLE RAW.NSW_LGA_SUBURB(
    lga_name VARCHAR,
    suburb_name VARCHAR NULL
);


