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
    store_id INTEGER REFERENCES stores(id),
    day_of_week_id INTEGER REFERENCES days_of_week(id),
    bussiness_start_time TIME,
    bussiness_end_time TIME
);

CREATE TABLE IF NOT EXISTS store_status (
    id BIGSERIAL PRIMARY KEY,
    store_id NUMERIC NOT NULL ,
    status VARCHAR(10) NOT NULL ,
    timestamp_utc VARCHAR(50) NOT NULL
);


END;