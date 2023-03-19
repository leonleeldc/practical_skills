import openai
openai.api_key = "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"
completion = openai.ChatCompletion.create(
  model="gpt-3.5-turbo",
  messages=[{"role": "user", "content": "Tell the world about the ChatGPT API in the style of a pirate."}]
)

print(completion)