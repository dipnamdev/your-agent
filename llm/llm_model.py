from openai import OpenAI
from config import DEEPSEEK_API_KEY

client = OpenAI(api_key=DEEPSEEK_API_KEY, base_url="https://api.deepseek.com")

def ask_llm(query, context, chat_history=None):
    """
    Sends the user query + context + recent chat history to DeepSeek-Chat
    and returns only the assistant's text response.
    """
    # Keep only last 5 turns
    recent_history = (chat_history or [])[-5:]

    messages = [{
        "role": "system",
        "content": (
           "You are a helpful AI assistant and act as an official representative of the company’s website. "
            "Your job is to answer customer questions using the provided website content as your main knowledge base. "
            "Do not say 'based on context' or 'from the website' — instead, answer as if you ARE the website. "
            "If information is clearly in the text, extract it directly and present it in simple, easy-to-understand language. "
            "If the text has partial hints (like ingredients, features, descriptions, or numbers), use reasoning to create a complete and helpful answer. "
            "It is okay to make small logical inferences (e.g., list 'benefits' from product features even if the word 'benefit' is not explicitly written). "
            "Always highlight the product’s value, benefits, and unique qualities in a promotional but natural tone. "
            "If something is truly missing, politely say you don’t know, and then provide company contact details (email, address, phone) so the user can follow up. "
            "NEVER mention that your knowledge comes from context or scraping. Always speak as if you are part of the company’s team. "
            "Keep answers clear, structured, and customer-friendly."
        )
    }]

    # Include chat history
    for user_msg, bot_msg in recent_history:
        messages.append({"role": "user", "content": user_msg})
        messages.append({"role": "assistant", "content": bot_msg})

    # Add the current query with context
    messages.append({
        "role": "user",
        "content": f"Website context:\n{context}\n\nUser question: {query}"
    })

    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=messages,
        stream=False
    )

    try:
        choice = response.choices[0]
        if hasattr(choice, "message"):
            return choice.message.content
        elif isinstance(choice, dict) and "message" in choice:
            return choice["message"]["content"]
        else:
            return str(response)
    except Exception:
        return str(response)
