
import os 
from pymongo import MongoClient


MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:adminpassword@mongo1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&authSource=admin&appName=mongosh+2.4.0")
client = MongoClient(MONGO_URI)
db = client["processed_logs_db"]  # Use your actual DB name
collection = db["cluster"]

def save_to_mongodb(message):
    try:
        collection.insert_one(message)

    except Exception as e:
        print(f"Error: {e}")



