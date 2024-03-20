import bson.json_util
import os
from connectServer import connect_to_mongodb

# Call the function to get the MongoDB client object
client = connect_to_mongodb()
db = client["mflix"]
directory = 'sample_mflix'

# Iterate over each file in the directory
for filename in os.listdir(directory):
    if filename.endswith('.json'):
        file_path = os.path.join(directory, filename)
        col_name = os.path.splitext(filename)[0]  # Remove .json extension for collection name

        # Check if collection already exists, if not, create it
        if col_name not in db.list_collection_names():
            db.create_collection(col_name)
            collection = db[col_name]

            # Open JSON file and insert data into collection
            with open(file_path, 'r') as file:
                for line in file:
                    data = bson.json_util.loads(line)
                    collection.insert_one(data)
        else:
            print("Collection already exists!")
