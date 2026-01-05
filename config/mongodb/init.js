// Variables
const dbName = "raw_data_db";

// Creation of the db
db = db.getSiblingDB(dbName);

db.createCollection("ingestion"); 

// Creation of CRUD user in the stocks database
db.createUser({
  user: "crud",
  pwd: "crud",
  roles: [
    { role: "readWrite", db: dbName }
  ]
});