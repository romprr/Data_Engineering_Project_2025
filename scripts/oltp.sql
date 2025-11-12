-- ===============================
-- Purpose: Represents all currencies used in the system.
-- Some currencies are only for denomination, some are tradable in forex.
-- Fields:
--   id           : Unique identifier for the currency
--   code         : Short currency code (USD, EUR, JPY)
--   name         : Full name of the currency
--   symbol       : Currency symbol ($, €, ¥)
--   is_tradable  : TRUE if this currency is tradable in forex, FALSE if only used for denomination
-- ===============================
CREATE TABLE CURRENCY (
    id SERIAL PRIMARY KEY,
    code VARCHAR(10) NOT NULL UNIQUE,
    name VARCHAR(100) NOT NULL,
    symbol VARCHAR(10),
    is_tradable BOOLEAN DEFAULT FALSE
);

-- ===============================
-- Purpose: Represents countries, used by companies, exchanges, and economic indicators.
-- Fields:
--   id           : Unique identifier for the country
--   name         : Country name (e.g., United States)
--   currency_id  : Foreign key linking to the country's official currency
--   region       : Optional classification (Europe, Asia, etc.)
-- ===============================
CREATE TABLE COUNTRY (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    currency_id INT REFERENCES CURRENCY(id),
    region VARCHAR(100)
);

-- ===============================
-- Purpose: Represents financial markets where assets are listed.
-- Fields:
--   id           : Unique identifier for the exchange
--   name         : Name of the exchange (NYSE, NASDAQ)
--   country_id   : Foreign key linking to the country where the exchange is located
--   timezone     : Timezone of the exchange for trading hours
-- ===============================
CREATE TABLE EXCHANGE (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    country_id INT REFERENCES COUNTRY(id),
    timezone VARCHAR(50)
);

-- ===============================
-- Purpose: Represents industry sectors for classifying companies.
-- Fields:
--   id           : Unique identifier for the sector
--   name         : Sector name (Technology, Finance, Energy, etc.)
-- ===============================
CREATE TABLE SECTOR (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);

-- ===============================
-- Purpose: Represents companies that issue stocks.
-- Fields:
--   id           : Unique identifier for the company
--   name         : Company name
--   country_id   : Foreign key linking to the company's country
--   sector_id    : Foreign key linking to the company's sector
-- ===============================
CREATE TABLE COMPANY (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    country_id INT REFERENCES COUNTRY(id),
    sector_id INT REFERENCES SECTOR(id)
);

-- ===============================
-- Purpose: Parent table for all tradable instruments (stocks, ETFs, commodities, indices, currencies).
-- Fields:
--   id           : Unique identifier for the asset
--   name         : Name of the asset
--   asset_type   : Type of asset (STOCK, ETF, COMMODITY, MARKET_INDEX, CURRENCY)
--   currency_id  : Currency used to price the asset
--   exchange_id  : Exchange where the asset is listed
--   description  : Optional detailed description of the asset
-- ===============================
CREATE TABLE ASSET (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    asset_type VARCHAR(50) NOT NULL,
    currency_id INT REFERENCES CURRENCY(id),
    exchange_id INT REFERENCES EXCHANGE(id),
    description TEXT
);

-- ===============================
-- Purpose: Represents shares of a company.
-- Fields:
--   id           : Unique identifier for the stock record
--   asset_id     : Foreign key linking to the parent ASSET
--   company_id   : Foreign key linking to the company issuing the stock
--   ticker       : Exchange-specific trading symbol
--   isin         : Global unique identifier for the stock
-- ===============================
CREATE TABLE STOCK (
    id SERIAL PRIMARY KEY,
    asset_id INT UNIQUE REFERENCES ASSET(id),
    company_id INT REFERENCES COMPANY(id),
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(20) UNIQUE NOT NULL
);

-- ===============================
-- Purpose: Represents Exchange-Traded Funds.
-- Fields:
--   id                   : Unique identifier for the ETF record
--   asset_id             : Foreign key linking to the parent ASSET
--   benchmark_index_id   : Asset ID of the index this ETF tracks
--   provider             : Company/fund manager issuing the ETF
--   ticker               : Trading symbol of the ETF
--   isin                 : Global unique identifier for the ETF
-- ===============================
CREATE TABLE ETF (
    id SERIAL PRIMARY KEY,
    asset_id INT UNIQUE REFERENCES ASSET(id),
    benchmark_index_id INT REFERENCES ASSET(id),
    provider VARCHAR(100),
    ticker VARCHAR(20) NOT NULL,
    isin VARCHAR(20) UNIQUE NOT NULL
);

-- ===============================
-- Purpose: Represents physical goods or raw materials traded in markets.
-- Fields:
--   id           : Unique identifier for the commodity record
--   asset_id     : Foreign key linking to the parent ASSET
--   category     : Type of commodity (Metal, Energy, Agriculture)
--   unit         : Unit of measurement (ounce, barrel, ton)
-- ===============================
CREATE TABLE COMMODITY (
    id SERIAL PRIMARY KEY,
    asset_id INT UNIQUE REFERENCES ASSET(id),
    category VARCHAR(50),
    unit VARCHAR(20)
);

-- ===============================
-- Purpose: Represents stock market indices.
-- Fields:
--   id           : Unique identifier for the index record
--   asset_id     : Foreign key linking to the parent ASSET
--   name         : Name of the index (S&P 500, CAC40)
--   region       : Geographic region of the index
-- ===============================
CREATE TABLE MARKET_INDEX (
    id SERIAL PRIMARY KEY,
    asset_id INT UNIQUE REFERENCES ASSET(id),
    name VARCHAR(200) NOT NULL,
    region VARCHAR(100)
);

-- ===============================
-- Purpose: Stores historical price data for all assets.
-- Fields:
--   id           : Unique identifier for the price record
--   asset_id     : Foreign key linking to the ASSET
--   date         : Date of the price data
--   open         : Opening price
--   high         : Highest price of the day
--   low          : Lowest price of the day
--   close        : Closing price
--   volume       : Number of units traded
-- ===============================
CREATE TABLE PRICE (
    id SERIAL PRIMARY KEY,
    asset_id INT REFERENCES ASSET(id),
    date DATE NOT NULL,
    open NUMERIC(15,4),
    high NUMERIC(15,4),
    low NUMERIC(15,4),
    close NUMERIC(15,4),
    volume BIGINT
);

-- ===============================
-- Purpose: Stores dividends paid by stocks or ETFs.
-- Fields:
--   id           : Unique identifier for the dividend record
--   asset_id     : Foreign key linking to the ASSET paying the dividend
--   date         : Payment date
--   amount       : Amount paid per share/unit
-- ===============================
CREATE TABLE DIVIDEND (
    id SERIAL PRIMARY KEY,
    asset_id INT REFERENCES ASSET(id),
    date DATE NOT NULL,
    amount NUMERIC(15,4) NOT NULL
);

-- ===============================
-- Purpose: Stores events affecting assets (earnings, splits, geopolitical, macro events)
-- Fields:
--   id           : Unique identifier for the event
--   asset_id     : Foreign key linking to the affected ASSET
--   type_event   : Type/category of event
--   title        : Short descriptive title
--   description  : Detailed description of the event
--   start_date   : Start date of the event
--   end_date     : End date of the event (optional)
-- ===============================
CREATE TABLE EVENT (
    id SERIAL PRIMARY KEY,
    asset_id INT REFERENCES ASSET(id),
    type_event VARCHAR(100),
    title VARCHAR(200),
    description TEXT,
    start_date DATE,
    end_date DATE
);

CREATE TABLE EVENT_ASSET (
    event_id INT REFERENCES EVENT(id),
    asset_id INT REFERENCES ASSET(id),
    PRIMARY KEY(event_id, asset_id)
);

-- ===============================
-- Purpose: Stores country-level macroeconomic indicators.
-- Fields:
--   id           : Unique identifier
--   country_id   : Foreign key linking to the country
--   type_indicator : Type of indicator (GDP, CPI, unemployment)
--   value        : Value of the indicator
--   date         : Date of measurement
-- ===============================
CREATE TABLE ECONOMIC_INDICATOR (
    id SERIAL PRIMARY KEY,
    country_id INT REFERENCES COUNTRY(id),
    type_indicator VARCHAR(100),
    value NUMERIC(15,4),
    date DATE NOT NULL
);

-- ===============================
-- Purpose: Stores company-level financial statements (quarterly or annual)
-- Fields:
--   id           : Unique identifier for the financial record
--   company_id   : Foreign key linking to the company
--   period_start : Start date of the reporting period
--   period_end   : End date of the reporting period
--   revenue      : Total revenue
--   net_income   : Net profit/loss
--   eps          : Earnings per share
--   assets       : Total assets
--   liabilities  : Total liabilities
--   equity       : Shareholder equity
-- ===============================
CREATE TABLE FINANCIALS (
    id SERIAL PRIMARY KEY,
    company_id INT REFERENCES COMPANY(id),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    revenue NUMERIC(20,2),
    net_income NUMERIC(20,2),
    eps NUMERIC(10,4),
    assets NUMERIC(20,2),
    liabilities NUMERIC(20,2),
    equity NUMERIC(20,2)
);