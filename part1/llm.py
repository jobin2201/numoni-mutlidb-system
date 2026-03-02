from openai import OpenAI
from config import GROQ_API_KEY, GROQ_BASE_URL, GROQ_MODEL

client = OpenAI(
    api_key=GROQ_API_KEY,
    base_url=GROQ_BASE_URL
)

def call_llm(prompt):
    response = client.chat.completions.create(
        model=GROQ_MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )
    return response.choices[0].message.content.strip()
