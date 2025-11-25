CREATE TABLE Continent (
    Id INT PRIMARY KEY,
    ContinentName VARCHAR(100) NOT NULL, 
    ContinentCode VARCHAR(10) UNIQUE NOT NULL
);

CREATE TABLE Country (
    Id INT PRIMARY KEY,
    CountryName VARCHAR(100) NOT NULL,
    CountryCode VARCHAR(10) UNIQUE NOT NULL,
    ContinentId INT NOT NULL, 
    FOREIGN KEY (ContinentId) REFERENCES Continent(Id)
);

CREATE TABLE Holder (
    Id INT PRIMARY KEY
);

CREATE TABLE Company (
    Id INT PRIMARY KEY,
    CompanyName VARCHAR(255) NOT NULL,
    CompanyCode VARCHAR(50) UNIQUE NOT NULL,
    CountryId INT,
    HolderId INT UNIQUE,
    FOREIGN KEY (CountryId) REFERENCES Country(Id),
    FOREIGN KEY (HolderId) REFERENCES Holder(Id)
);

CREATE TABLE Individual (
    Id INT PRIMARY KEY,
    FirstName VARCHAR(100) NOT NULL,
    LastName VARCHAR(100) NOT NULL,
    Residency INT,
    NetWorth DECIMAL(20,2),
    HolderId INT UNIQUE,
    FOREIGN KEY (HolderId) REFERENCES Holder(Id),
    FOREIGN KEY (Residency) REFERENCES Country(Id)
);

CREATE TABLE Market (
    Id INT PRIMARY KEY,
    MarketName VARCHAR(100) NOT NULL,
    MarketCode VARCHAR(10) UNIQUE NOT NULL,
    Status VARCHAR(50) NOT NULL CHECK (Status IN ('Bearish', 'Bullish', 'Stable')),
    CountryId INT NOT NULL,
    FOREIGN KEY (CountryId) REFERENCES Country(Id)
);

-- TODO
CREATE TABLE Asset (
    Id INT PRIMARY KEY,
    AssetType VARCHAR(50) NOT NULL CHECK (AssetType IN ('Forex', 'Commodity', 'Stock', 'ETF', 'Cryptocurrency')),
    Description TEXT,
    Provider INT NOT NULL, 
    Value DECIMAL(20,6) NOT NULL,
    Market INT NOT NULL, 
    FOREIGN KEY (Market) REFERENCES Market(Id),
    FOREIGN KEY (Provider) REFERENCES Company(Id)
);

CREATE TABLE Transaction (
    Id INT PRIMARY KEY,
    HolderId INT,
    AssetId INT,
    TransactionType VARCHAR(50) NOT NULL CHECK (TransactionType IN ('Buy', 'Sell')),
    Quantity DECIMAL(20,4) NOT NULL,
    PricePerUnit DECIMAL(20,2) NOT NULL,
    TransactionDate TIMESTAMP NOT NULL,
    FOREIGN KEY (HolderId) REFERENCES Holder(Id),
    FOREIGN KEY (AssetId) REFERENCES Asset(Id)
);

CREATE TABLE Event (
    Id INT PRIMARY KEY,
    EventType VARCHAR(100) NOT NULL CHECK (EventType IN ('Economic', 'Political', 'Natural Disaster', 'Technological', 'Social')),
    Title VARCHAR(255) NOT NULL,
    Description TEXT,
    EventDate TIMESTAMP NOT NULL
);

CREATE TABLE CountryEvent (
    CountryId INT,
    EventId INT,
    PRIMARY KEY (CountryId, EventId),
    FOREIGN KEY (CountryId) REFERENCES Country(Id),
    FOREIGN KEY (EventId) REFERENCES Event(Id)
);

CREATE TABLE AssetEvent (
    AssetId INT,
    EventId INT,
    Magnitude DECIMAL(10,2),
    TypeImpact VARCHAR(50) CHECK (TypeImpact IN ('Positive', 'Negative', 'Neutral')),
    Description TEXT,
    PRIMARY KEY (AssetId, EventId),
    FOREIGN KEY (AssetId) REFERENCES Asset(Id),
    FOREIGN KEY (EventId) REFERENCES Event(Id)
);

CREATE TABLE MarketEvent (
    MarketId INT,
    EventId INT,
    PRIMARY KEY (MarketId, EventId),
    FOREIGN KEY (MarketId) REFERENCES Market(Id),
    FOREIGN KEY (EventId) REFERENCES Event(Id)
);