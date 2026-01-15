from file_utils import read_config_file
import json
import requests
import re
from pydantic import BaseModel
import os
import time

def stream_grok_agent_response(prompt: str, enable_web_search: bool = True, enable_x_search: bool = True) -> str:
    """
    Stream the assistant's reply from Grok 4.1 Fast using the Agent Tools API.
    This uses the xai-sdk and provides real-time search from X.com and the web.

    Parameters
    ----------
    prompt : str
        The user's prompt.
    enable_web_search : bool
        Enable web search tool. Default True.
    enable_x_search : bool  
        Enable X (Twitter) search tool. Default True.

    Returns
    -------
    str
        The concatenated response text.
    """
    try:
        from xai_sdk import Client
        from xai_sdk.chat import user, system
        from xai_sdk.tools import web_search, x_search
    except ImportError:
        print("xai-sdk not installed. Run: pip install xai-sdk>=1.3.1")
        return ""

    api_key = read_config_file("grok_api_key")
    if not api_key:
        print("Grok API key not found.")
        return ""

    client = Client(api_key=api_key, timeout=3600)

    # Build tools list based on parameters
    tools = []
    if enable_web_search:
        tools.append(web_search())
    if enable_x_search:
        tools.append(x_search())

    chat = client.chat.create(
        model="grok-4-1-fast-reasoning",  # Latest agentic tool-calling model
        tools=tools,
        max_turns=5,  # Allow multiple search iterations
    )

    chat.append(system("You are Grok, the best assistant for comprehensive and real-time market updates with access to live data from X and the web."))
    chat.append(user(prompt))

    full_response_parts: list[str] = []
    is_thinking = True

    for response, chunk in chat.stream():
        # Show tool calls as they happen
        for tool_call in chunk.tool_calls:
            print(f"\n[Tool: {tool_call.function.name}] {tool_call.function.arguments}")

        # Show thinking indicator
        if response.usage and response.usage.reasoning_tokens and is_thinking:
            print(f"\rThinking...", end="", flush=True)
            is_thinking = False

        # Stream content
        if chunk.content:
            print(chunk.content, end="", flush=True)
            full_response_parts.append(chunk.content)

    print()  # final newline

    # Print citations if available
    if response.citations:
        print("\n--- Sources ---")
        for url in response.citations:
            print(f"  • {url}")

    return "".join(full_response_parts)

if __name__ == "__main__":
    start_time = time.time()
    stream_grok_agent_response("""Tell me about the US stock market performance so far today?""")

    end_time = time.time()
    print(f"Time taken: {end_time - start_time} seconds")
