from openai import OpenAI
grok = OpenAI(
    api_key="xai-xhVvUt0BTS8GRVJTfQc05EWUQuIv4KXHh1fqFbLKeFAzyxDNBsk6FwOvW4737fouyJk5Cn0Cwp3bM0PV",
    base_url="https://api.x.ai/v1"
)
response = grok.chat.completions.create(
    model="
