from pymongo import MongoClient
from config import MONGODB_URI, DB_CUSTOMER, DB_MERCHANT

client = MongoClient(MONGODB_URI)

DATABASES = {
    "customer": client[DB_CUSTOMER],
    "merchant": client[DB_MERCHANT]
}
