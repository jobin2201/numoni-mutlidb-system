from llm import call_llm

def detect_intent(user_query):
    prompt = f"""
Classify into one word:
customer OR merchant

Query: {user_query}
"""

    result = call_llm(prompt).lower()

    if "merchant" in result:
        return "merchant"
    return "customer"
