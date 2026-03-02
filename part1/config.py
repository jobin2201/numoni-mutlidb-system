import os
from dotenv import load_dotenv

load_dotenv()

MONGODB_URI = os.getenv("MONGODB_URI")
DB_CUSTOMER = os.getenv("MONGO_DB_CUSTOMER")
DB_MERCHANT = os.getenv("MONGO_DB_MERCHANT")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_BASE_URL = os.getenv("GROQ_BASE_URL")
GROQ_MODEL = "llama-3.1-8b-instant"
