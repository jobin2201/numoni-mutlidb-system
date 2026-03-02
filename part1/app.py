from db import DATABASES
from router import detect_intent
from query_generator import generate_query

def execute_query(db, query):
    collection = query.get("collection")
    filter_q = query.get("filter", {})

    if not collection:
        return "Invalid query"

    return list(db[collection].find(filter_q).limit(5))


def chatbot():
    print("Numoni Chatbot (type exit)")

    while True:
        user_input = input("You: ")

        if user_input.lower() == "exit":
            break

        intent = detect_intent(user_input)
        print(f"[Using {intent} DB]")

        query = generate_query(user_input, intent)

        if not query:
            print("Query generation failed\n")
            continue

        db = DATABASES[intent]
        result = execute_query(db, query)

        print(result)
        print()


if __name__ == "__main__":
    chatbot()
