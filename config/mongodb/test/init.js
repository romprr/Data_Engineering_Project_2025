// Variables
const dbName = "stocks";

// Creation of the db
db = db.getSiblingDB(dbName);

// Create collections TODO what collections are needed?
db.createCollection("customers")

// Sample data insertion
db.customers.insertMany([
    { name: "Alice", email: "alice@example.com" }, 
    { name: "Bob", email: "bob@example.com" }, 
    { name: "Charlie", email: "charlie@example.com", phone : "123-456-7890" }, 
    { name: "David", address : "123 Main St, Anytown, USA" }
]);

// Creation of CRUD user in the stocks database
// TODO check env variables for user and password
db.createUser({
  user: "crud",
  pwd: "crud",
  roles: [
    { role: "readWrite", db: dbName }
  ]
});