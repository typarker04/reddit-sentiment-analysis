import os
from google import genai

try:
    # The SDK automatically looks for the GEMINI_API_KEY environment variable
    client = genai.Client()
    response = client.models.generate_content(
        model='gemini-3.8-flash',
        contents='Hello, respond with the word "Authenticated!"',
    )
    print(response.text)
except Exception as e:
    print(f"Authentication failed: {e}")
