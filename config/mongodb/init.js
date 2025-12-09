// Variables
const dbName = "stocks";

// Creation of the db
db = db.getSiblingDB(dbName);

db.createCollection("stock_prices")
db.createCollection("companies_info")
db.createCollection("stock_transactions")
db.createCollection("crypto_prices")
db.createCollection("crypto_transactions")
db.createCollection("political_events")
db.createCollection("politicians")
db.createCollection("politicians_market_transactions")


// Creation of CRUD user in the stocks database
// TODO check env variables for user and password
db.createUser({
  user: "crud",
  pwd: "crud",
  roles: [
    { role: "readWrite", db: dbName }
  ]
});