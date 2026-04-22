import os
from typing import Optional

import requests


def send_telegram_message(message: str, token: Optional[str] = None, chat_id: Optional[str] = None) -> bool:
    """Send a plain-text message to Telegram. Returns True on success."""
    bot_token = token or os.getenv("TELEGRAM_BOT_TOKEN")
    target_chat = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not target_chat:
        return False

    text = message.strip()
    if not text:
        return False

    if len(text) > 4096:
        text = text[:4080] + "\n\n...truncated..."

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    resp = requests.post(
        url,
        json={
            "chat_id": target_chat,
            "text": text,
            "disable_web_page_preview": True,
        },
        timeout=10,
    )
    resp.raise_for_status()
    return True
