import os

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError

load_dotenv()

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("MONGODB_DB", "appointment_system")

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
db = client[MONGO_DB_NAME]

accounts = db.accounts
sessions = db.sessions
services = db.services
time_slots = db.time_slots
appointments = db.appointments
staff_notes = db.staff_notes


def ensure_connection():
    try:
        client.admin.command("ping")
        print(f"[DB] Connected to MongoDB: {MONGO_DB_NAME}")
    except (ConnectionFailure, ServerSelectionTimeoutError) as e:
        print(f"[DB] ERROR: Could not connect to MongoDB at {MONGO_URI}")
        print(f"[DB] Details: {e}")
        raise


def ensure_indexes():
    accounts.create_index([("username", ASCENDING)], unique=True, sparse=True)
    accounts.create_index([("iin", ASCENDING)], unique=True, sparse=True)
    accounts.create_index([("phone", ASCENDING)], unique=True, sparse=True)
    accounts.create_index([("email", ASCENDING)], unique=True, sparse=True)
    sessions.create_index([("token", ASCENDING)], unique=True)
    services.create_index([("name", ASCENDING)])
    time_slots.create_index([("date", ASCENDING), ("start_time", ASCENDING)])
    appointments.create_index([("ticket_number", ASCENDING)], unique=True)
    appointments.create_index([("citizen_id", ASCENDING)])
    appointments.create_index([("date", ASCENDING), ("status", ASCENDING)])
    staff_notes.create_index([("appointment_id", ASCENDING)])
