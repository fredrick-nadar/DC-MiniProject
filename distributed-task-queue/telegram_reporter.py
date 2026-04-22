import os
from typing import Optional

import requests


def _resolve_credentials(token: Optional[str] = None, chat_id: Optional[str] = None) -> tuple[str, str] | tuple[None, None]:
    bot_token = token or os.getenv("TELEGRAM_BOT_TOKEN")
    target_chat = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not bot_token or not target_chat:
        return None, None
    return bot_token, target_chat


def send_telegram_message(message: str, token: Optional[str] = None, chat_id: Optional[str] = None) -> bool:
    """Send a plain-text message to Telegram. Returns True on success."""
    bot_token, target_chat = _resolve_credentials(token, chat_id)
    if not bot_token or not target_chat:
        return False

    text = message.strip()
    if not text:
        return False

    # Telegram Bot API limit for text messages is 4096 chars.
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


def send_telegram_photo(
    image_bytes: bytes,
    caption: str = "",
    *,
    filename: str = "session-report.png",
    mime_type: str = "image/png",
    token: Optional[str] = None,
    chat_id: Optional[str] = None,
) -> bool:
    """Send an image to Telegram with an optional caption."""
    bot_token, target_chat = _resolve_credentials(token, chat_id)
    if not bot_token or not target_chat or not image_bytes:
        return False

    final_caption = caption.strip()
    if len(final_caption) > 1024:
        final_caption = final_caption[:1000] + "\n\n...truncated..."

    url = f"https://api.telegram.org/bot{bot_token}/sendPhoto"
    resp = requests.post(
        url,
        data={
            "chat_id": target_chat,
            "caption": final_caption,
            "disable_notification": False,
        },
        files={
            "photo": (filename, image_bytes, mime_type),
        },
        timeout=20,
    )
    resp.raise_for_status()
    return True
