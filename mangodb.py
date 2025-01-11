from pymongo import MongoClient
from datetime import datetime
client = MongoClient("mongodb://localhost:27017/")

db = client["test_db"]
collection = db["test_collection"]

def insert_records():
    print("Starting to insert 1 million records...")
    records = []
    for i in range(1, 1000001):
        record = {
            "record_id": i,
            "name": f"User_{i}",
            "email": f"user_{i}@example.com",
            "created_at": datetime.now()
        }
        records.append(record)
        if len(records) == 10000:
            collection.insert_many(records)
            records = []
            print(f"Inserted {i} records...")
    if records:
        collection.insert_many(records)
    print("Finished inserting 1 million records!")
def delete_latest_10_records():
    print("Deleting the latest 10 records...")
    latest_records = collection.find().sort("created_at", -1).limit(10)
    latest_ids = [record["_id"] for record in latest_records]
    result = collection.delete_many({"_id": {"$in": latest_ids}})
    print(f"Deleted {result.deleted_count} records.")

def fetch_record(record_id):
    print(f"Fetching record with record_id: {record_id}")
    record = collection.find_one({"record_id": record_id})
    if record:
        print(f"Record found: {record}")
    else:
        print("Record not found.")

def update_record(record_id, new_name):
    print(f"Updating record with record_id: {record_id}")
    result = collection.update_one(
        {"record_id": record_id},
        {"$set": {"name": new_name}}
    )
    if result.modified_count > 0:
        print(f"Record {record_id} updated successfully.")
    else:
        print("Record not found or not updated.")
def count_records():
    count = collection.count_documents({})
    print(f"Total records in the collection: {count}")

if __name__ == "__main__":
    insert_records()
    delete_latest_10_records()
    fetch_record(500)
    update_record(500, "Updated_User_500")
    count_records()
    client.close()
    print("MongoDB connection closed.")


f