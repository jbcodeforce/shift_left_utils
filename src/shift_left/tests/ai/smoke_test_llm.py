# Using OpenAI Python SDK with Osaurus
from openai import OpenAI
import os
client = OpenAI(
  base_url=os.getenv("SL_LLM_BASE_URL"),
  api_key=os.getenv("SL_LLM_API_KEY")
)
try:
    completion = client.chat.completions.create(
        model=os.getenv("SL_LLM_MODEL"),
        messages=[
      {
        "role": "system",
        "content": "You are a helpful assistant."
      },
      {
        "role": "user",
        "content": "What is the capital of France?"
      }
    ]
    )
    print(completion.choices[0].message.content)
except Exception as e:
    print(f"Error: {e}")


