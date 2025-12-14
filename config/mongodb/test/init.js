// Variables
const dbName = "raw_data_db";

// Creation of the db
db = db.getSiblingDB(dbName);

db.createCollection("cryptocurrencies_information")
db.createCollection("cryptocurrencies_values")
db.createCollection("forex_information")
db.createCollection("forex_values")
db.createCollection("futures_information")
db.createCollection("futures_values")
db.createCollection("indices_information")
db.createCollection("indices_values")
db.createCollection("worldwide_events")

// Creation of CRUD user in the stocks database
// TODO check env variables for user and password
db.createUser({
  user: "crud",
  pwd: "crud",
  roles: [
    { role: "readWrite", db: dbName }
  ]
});