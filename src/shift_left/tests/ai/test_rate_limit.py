import requests
import json
import os

api_key = os.getenv("SL_LLM_API_KEY")

response = requests.get(
  url="https://openrouter.ai/api/v1/key",
  headers={
    "Authorization": f"Bearer {api_key}"
  }
)

print(json.dumps(response.json(), indent=2))
