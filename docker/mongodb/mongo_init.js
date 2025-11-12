// db = connect("localhost:27017");s
// Creation of the db
db = db.getSiblingDB("stocks_db");

// Create collections
db.createCollection("currency");
db.createCollection("country");
db.createCollection("exchange");
db.createCollection("sector");
db.createCollection("company");
db.createCollection("asset");
db.createCollection("stock");
db.createCollection("etf");
db.createCollection("commodity");
db.createCollection("market_index");
db.createCollection("price");
db.createCollection("event");
db.createCollection("economic_indicator");
db.createCollection("financials");
db.createCollection("buyer");
db.createCollection("seller");
db.createCollection("transaction");

console.log("Database and collections created successfully.");
console.log("Waiting for further data population...");