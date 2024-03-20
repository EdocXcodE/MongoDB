from pymongo import MongoClient


def connect_to_mongodb():
    """
    Connect to MongoDB and return the client object.

    Returns:
        MongoClient: A client object representing the connection to MongoDB.

    Raises:
        Exception: If there is an error connecting to MongoDB.
    """
    try:
        # Connect to MongoDB using localhost and default port 27017
        client = MongoClient('127.0.0.1', 27017)
        print("Connection Successful!\n")
        return client
    except Exception as e:
        # Print error message if connection fails
        print(f"Error connecting to MongoDB: {e}")
        return None
