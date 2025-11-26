// Switch to the 'stocks' database
CREATE DATABASE stocks IF NOT EXISTS;

// Use the stocks database
:use stocks

// Create a "table" of stocks using nodes with label Stock
CREATE (:Stock {symbol: 'AAPL', name: 'Apple Inc.', price: 175.0});