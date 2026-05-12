"""
mongo_setup.py — MongoDB Initialization
=========================================
Creates the crime_db database, alert_logs collection,
and indexes for efficient querying.

Usage:
    docker exec -it streamlit python /app/db/mongo_setup.py
"""

from pymongo import MongoClient, ASCENDING, DESCENDING

MONGO_HOST = "mongodb"
MONGO_PORT = 27017
DB_NAME = "crime_db"


def setup_mongodb():
    """Initialize MongoDB collections and indexes."""
    client = MongoClient(host=MONGO_HOST, port=MONGO_PORT)
    db = client[DB_NAME]

    # Create the alert_logs collection (if not exists)
    if "alert_logs" not in db.list_collection_names():
        db.create_collection("alert_logs")
        print("✓ Created 'alert_logs' collection.")
    else:
        print("✓ 'alert_logs' collection already exists.")

    # Create indexes for efficient querying
    alert_logs = db["alert_logs"]
    alert_logs.create_index([("district", ASCENDING)], name="idx_district")
    alert_logs.create_index([("severity", ASCENDING)], name="idx_severity")
    alert_logs.create_index([("triggered_at", DESCENDING)], name="idx_triggered_at")
    alert_logs.create_index([("alert_id", ASCENDING)], unique=True, name="idx_alert_id")
    print("✓ Indexes created on alert_logs.")

    # Create the speed_layer_alerts collection (legacy compatibility)
    if "speed_layer_alerts" not in db.list_collection_names():
        db.create_collection("speed_layer_alerts")
        print("✓ Created 'speed_layer_alerts' collection.")

    client.close()
    print("\n✅ MongoDB setup complete.")


if __name__ == "__main__":
    setup_mongodb()
