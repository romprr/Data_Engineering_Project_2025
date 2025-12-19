-- Create application user
-- TODO check security for username and password
CREATE USER user WITH PASSWORD 'user';

-- Grant permissions to user
GRANT CONNECT ON DATABASE stock TO user;
GRANT USAGE ON SCHEMA public TO user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO user;
-- Ensure future tables also grant permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO user;

-- Create table
CREATE TABLE Customer (
    ID INT PRIMARY KEY,
    Name VARCHAR(100) NOT NULL,
    Email VARCHAR(100) UNIQUE, 
    Adress VARCHAR(255), 
    Phone VARCHAR(15) UNIQUE
);
