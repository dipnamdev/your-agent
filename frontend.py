# frontend.py
import gradio as gr
import main
import os

chat_history = []  # keep chat state in memory

def prepare_site(url):
    status = main.process_site(url)
    # reset chat when site changes
    chat_history.clear()
    return status, []

def chat_with_site(query, url):
    if not url:
        return chat_history, "⚠️ Please enter a site URL first."

    # Call main answer pipeline
    answer = main.answer_question(query, url)

    # Update chat history
    chat_history.append(("user", query))
    chat_history.append(("assistant", answer))

    # Format for Gradio Chatbot
    messages = []
    for role, content in chat_history:
        if role == "user":
            messages.append(("You", content))
        else:
            messages.append(("Assistant", content))
    return messages, ""  # clear input box after response


with gr.Blocks(css="""
#chatbot .user {
    background-color: #DCF8C6;  /* light green */
    text-align: right;
    border-radius: 12px;
    padding: 8px;
}
#chatbot .assistant {
    background-color: #E8E8E8;  /* light gray */
    text-align: left;
    border-radius: 12px;
    padding: 8px;
}
""") as demo:
    gr.Markdown("## 🔎 Website Q&A Chat (like ChatGPT)")

    with gr.Row():
        url_input = gr.Textbox(label="Website URL", placeholder="https://example.com")
        process_btn = gr.Button("Process Site")
    process_output = gr.Textbox(label="Status / Index Path")

    chatbot = gr.Chatbot(label="Chat", elem_id="chatbot", height=500)
    with gr.Row():
        chat_input = gr.Textbox(label="Ask about site", placeholder="Type your question here...", scale=4)
        ask_btn = gr.Button("Ask", scale=1)

    # Events
    process_btn.click(prepare_site, inputs=url_input, outputs=[process_output, chatbot])
    ask_btn.click(chat_with_site, inputs=[chat_input, url_input], outputs=[chatbot, chat_input])

demo.launch(
    server_name=os.getenv("HOST", "0.0.0.0"),
    server_port=int(os.getenv("PORT", "7860")),
    share=False,
    auth=(
        (os.getenv("GRADIO_USERNAME"), os.getenv("GRADIO_PASSWORD"))
        if os.getenv("GRADIO_USERNAME") and os.getenv("GRADIO_PASSWORD")
        else None
    ),
)
