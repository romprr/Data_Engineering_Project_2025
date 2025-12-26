-- Create table
CREATE TABLE INDEX_EXCHANGE (
    symbol VARCHAR(20)  PRIMARY KEY,
    currency VARCHAR(4) ,
    region VARCHAR(5) ,
    index_name TEXT , -- Here it's the long name, ex S&P 500
    exchange_timezone TEXT  -- Will be useful if we want to look at the "real" value of a company
);

-- This is the value of the indes, it's a time series
CREATE TABLE INDEX_HISTORY (
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
        FOREIGN KEY (symbol) REFERENCES INDEX_EXCHANGE(symbol)
);

-- [ Forex ]

-- Infos of forex exchange
CREATE TABLE FOREX_EXCHANGE (
    symbol VARCHAR(20)  PRIMARY KEY,
    forex_name VARCHAR(41) , -- This is for example "EUR/USD"
    region VARCHAR(5),
    exchange_timezone TEXT 
);

-- Forex values in timeseries

CREATE TABLE FOREX_HISTORY (
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
        FOREIGN KEY (symbol) REFERENCES FOREX_EXCHANGE(symbol)
);

-- Futures infos
-- Infos of forex exchange
CREATE TABLE FUTURES_EXCHANGE (
    symbol VARCHAR(20)  PRIMARY KEY,
    futures_name VARCHAR(41) ,
    region VARCHAR(5),
    currency VARCHAR(5),
    exchange_timezone TEXT
);

-- Futures values
CREATE TABLE FUTURES_HISTORY (
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
        FOREIGN KEY (symbol) REFERENCES FUTURES_EXCHANGE(symbol)
);


-- Crypto infos

CREATE TABLE CRYPTO_EXCHANGE (
    symbol VARCHAR(20)  PRIMARY KEY,
    crypto_name VARCHAR(41) , -- This is for example "BTC-USD"
    region VARCHAR(5),
    exchange_timezone TEXT 
);

-- Crypto values

CREATE TABLE CRYPTO_HISTORY (
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
        FOREIGN KEY (symbol) REFERENCES CRYPTO_EXCHANGE(symbol)
);


-- [ UCDP DATA ]
CREATE TABLE CONFLICT(
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

CREATE TABLE UCDP_ACTORS(
    actor_id INT,
    actor_name TEXT,
    actor_og_name TEXT,
    PRIMARY KEY (actor_id)
);


-- Permissions
GRANT CONNECT ON DATABASE stock TO admin;
GRANT USAGE, CREATE ON SCHEMA public TO admin;
GRANT SELECT, INSERT, DELETE, UPDATE ON ALL TABLES IN SCHEMA public TO admin;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO admin;