from pymongo import MongoClient
from typing import Any, Dict, List, Optional

class MongoDBClient:
    def __init__(
        self,
        uri: str,
        database: str,
        collection: str,
        **kwargs
    ):
        """
        Initialize the MongoDB client.

        :param uri: MongoDB connection URI.
        :param database: Database name.
        :param collection: Collection name.
        :param kwargs: Additional keyword arguments for MongoClient.
        """
        self.uri = uri
        self.database_name = database
        self.collection_name = collection
        self.client: Optional[MongoClient] = None
        self.db = None
        self.collection = None
        self.client_kwargs = kwargs

    def connect(self):
        """Connect to the MongoDB client and set database and collection."""
        self.client = MongoClient(self.uri, **self.client_kwargs)
        self.db = self.client[self.database_name]
        self.collection = self.db[self.collection_name]

    def disconnect(self):
        """Disconnect from the MongoDB client."""
        if self.client:
            self.client.close()
            self.client = None
            self.db = None
            self.collection = None

    def write(self, data: Dict[str, Any]) -> Any:
        """
        Insert a single document into the collection.

        :param data: Document to insert.
        :return: Inserted document ID.
        """
        if self.collection is None:
            raise RuntimeError("Not connected to MongoDB.")
        return self.collection.update_one({"_id" : data["_id"]}, {"$set": data}, upsert=True).upserted_id

    def write_many(self, data_list: List[Dict[str, Any]]) -> List[Any]:
        """
        Insert multiple documents into the collection.

        :param data_list: List of documents to insert.
        :return: List of inserted document IDs.
        """
        if not self.collection:
            raise RuntimeError("Not connected to MongoDB.")
        result = self.collection.insert_many(data_list)
        return result.inserted_ids

    def read(self, query: Dict[str, Any] = {}, projection: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Read documents from the collection.

        :param query: Query filter.
        :param projection: Fields to include or exclude.
        :return: List of documents.
        """
        if not self.collection:
            raise RuntimeError("Not connected to MongoDB.")
        cursor = self.collection.find(query, projection)
        return list(cursor)