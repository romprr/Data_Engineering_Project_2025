// db = connect("localhost:27017");s
// Creation of the db
db = db.getSiblingDB("stocks");

// Create collections TODO what collections are needed?
db.createCollection("Country")


console.log("Database and collections created successfully.");
console.log("Waiting for further data population...");