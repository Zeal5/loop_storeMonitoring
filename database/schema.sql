BEGIN;

CREATE TABLE IF NOT EXISTS stores (
    id BIGSERIAL PRIMARY KEY,
    store_id NUMERIC NOT NULL UNIQUE,
    local_time_zone VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS days_of_week (
    id BIGSERIAL PRIMARY KEY,
    day_name VARCHAR(10) NOT NULL,
    day_number INTEGER NOT NULL
);


CREATE TABLE IF NOT EXISTS business_hours (
    id BIGSERIAL PRIMARY KEY,
    store_id NUMERIC NOT NULL,
    day_of_week_id INTEGER NOT NULL,
    business_start_time TIME,
    business_end_time TIME
);

CREATE TABLE IF NOT EXISTS store_status (
    id BIGSERIAL PRIMARY KEY,
    store_id NUMERIC NOT NULL ,
    status VARCHAR(10) NOT NULL ,
    timestamp_utc timestamp 
);


END;