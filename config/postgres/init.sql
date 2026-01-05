-- Create schemas
CREATE SCHEMA raw;
CREATE SCHEMA staging;
CREATE SCHEMA production;

-- Create table
CREATE TABLE raw.INDEX_EXCHANGE (
    symbol VARCHAR(20)  PRIMARY KEY,
    currency VARCHAR(4) ,
    region VARCHAR(5) ,
    index_name TEXT , -- Here it's the long name, ex S&P 500
    exchange_timezone TEXT  -- Will be useful if we want to look at the "real" value of a company
);

-- This is the value of the indes, it's a time series
CREATE TABLE raw.INDEX_HISTORY (
    symbol VARCHAR(20) ,
    value_timestamp Varchar(50) ,
    value_open DECIMAL(30, 2) ,
    value_high DECIMAL(30, 2) ,
    value_low DECIMAL(30, 2) ,
    value_close DECIMAL(30, 2) ,
    volume BIGINT ,
    dividends DECIMAL(30, 2) ,
    stock_splits INT ,
    PRIMARY KEY (symbol, value_timestamp),
    CONSTRAINT fk_index
        FOREIGN KEY (symbol) REFERENCES raw.INDEX_EXCHANGE(symbol)
);

-- [ Forex ]

-- Infos of forex exchange
CREATE TABLE raw.FOREX_EXCHANGE (
    symbol VARCHAR(20)  PRIMARY KEY,
    forex_name VARCHAR(41) , -- This is for example "EUR/USD"
    region VARCHAR(5),
    exchange_timezone TEXT 
);

-- Forex values in timeseries

CREATE TABLE raw.FOREX_HISTORY (
    symbol VARCHAR(20) ,
    value_timestamp Varchar(50) ,
    value_open DECIMAL(30, 2) ,
    value_high DECIMAL(30, 2) ,
    value_low DECIMAL(30, 2) ,
    value_close DECIMAL(30, 2) ,
    dividends DECIMAL(30,2) , -- This is only for loading -> will be deleted later
    volume BIGINT ,
    stock_splits INT ,
    PRIMARY KEY (symbol, value_timestamp),
    CONSTRAINT fk_forex 
        FOREIGN KEY (symbol) REFERENCES raw.FOREX_EXCHANGE(symbol)
);

-- Futures infos
-- Infos of forex exchange
CREATE TABLE raw.FUTURES_EXCHANGE (
    symbol VARCHAR(20)  PRIMARY KEY,
    futures_name VARCHAR(41) ,
    region VARCHAR(5),
    currency VARCHAR(5),
    exchange_timezone TEXT
);

-- Futures values
CREATE TABLE raw.FUTURES_HISTORY (
    symbol VARCHAR(20) ,
    value_timestamp Varchar(50) ,
    value_open DECIMAL(30, 2) ,
    value_high DECIMAL(30, 2) ,
    value_low DECIMAL(30, 2) ,
    value_close DECIMAL(30, 2) ,
    dividends DECIMAL(30,2) , -- This is only for loading -> will be deleted later
    volume BIGINT ,
    stock_splits INT ,
    PRIMARY KEY (symbol, value_timestamp),
    CONSTRAINT fk_futures
        FOREIGN KEY (symbol) REFERENCES raw.FUTURES_EXCHANGE(symbol)
);


-- Crypto infos

CREATE TABLE raw.CRYPTO_EXCHANGE (
    symbol VARCHAR(20)  PRIMARY KEY,
    crypto_name VARCHAR(41) , -- This is for example "BTC-USD"
    region VARCHAR(5),
    exchange_timezone TEXT 
);

-- Crypto values

CREATE TABLE raw.CRYPTO_HISTORY (
    symbol VARCHAR(20) ,
    value_timestamp Varchar(50) ,
    value_open DECIMAL(30, 2) ,
    value_high DECIMAL(30, 2) ,
    value_low DECIMAL(30, 2) ,
    value_close DECIMAL(30, 2) ,
    dividends DECIMAL(30,2) , -- This is only for loading -> will be deleted later
    volume BIGINT ,
    stock_splits INT ,
    PRIMARY KEY (symbol, value_timestamp),
    CONSTRAINT fk_crypto
        FOREIGN KEY (symbol) REFERENCES raw.CRYPTO_EXCHANGE(symbol)
);


-- [ UCDP DATA ]
CREATE TABLE raw.CONFLICT(
    conflict_id INT , -- this is the real id of the conflict but, because this is repeated it would be a problem for PG
    "location" TEXT ,
    "year" VARCHAR(4) , -- This will be used with the id as the primary key

    -- Actors (side a, b, 2nd a, 2nd b)
    side_a TEXT ,
    side_b TEXT ,
    side_a_2nd TEXT,
    side_b_2nd TEXT,

    --  Actors IDs from GWNO
    gwno_a TEXT ,
    gwno_b TEXT ,
    gwno_b_2nd TEXT,
    gwno_a_2nd TEXT,

    -- Other infos
    region TEXT , 
    intensity_level INT ,
    cumulative_intensity INT ,
    type_of_conflict INT ,
    incompatibility INT ,
    territory_name TEXT,


    -- Dates
    "start_date" DATE , -- conflict start date
    "start_date2" DATE , -- episode start date
    ep_end_date DATE,
    PRIMARY KEY (conflict_id, "year")
);

CREATE TABLE raw.UCDP_ACTORS(
    actor_id INT,
    actor_name TEXT,
    actor_og_name TEXT,
    conflict_ids TEXT,
    org_level INT,
    PRIMARY KEY (actor_id)
);

CREATE TABLE raw.UCDP_GEOREFERENCE(
    "id" INT,
    conflict_new_id INT,
    country TEXT,
    region TEXT,
    PRIMARY KEY (id)
);

-- Permissions
GRANT CONNECT ON DATABASE stock TO admin;

GRANT USAGE, CREATE ON SCHEMA raw, staging, production TO admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw, staging, production 
GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON TABLES TO admin;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw, staging, production 
GRANT USAGE, SELECT ON SEQUENCES TO admin;

GRANT SELECT, INSERT, UPDATE, DELETE, TRUNCATE ON ALL TABLES IN SCHEMA raw, staging, production TO admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA raw, staging, production TO admin;