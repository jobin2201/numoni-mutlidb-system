import json
from llm import call_llm

def generate_query(user_query, db_type):
    prompt = f"""
Generate Mongo find query JSON.
Database: {db_type}
Question: {user_query}

Return format:
{{"collection":"name","filter":{{}}}}
"""

    result = call_llm(prompt)

    try:
        return json.loads(result)
    except:
        return None
