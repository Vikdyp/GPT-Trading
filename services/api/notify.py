# services/api/notify.py
import os, json

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "").strip()

try:
    import httpx
except Exception:
    httpx = None

def _discord_enabled() -> bool:
    return bool(DISCORD_WEBHOOK_URL and httpx)

def _telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and httpx)

async def notify(text: str, extra: dict | None = None) -> None:
    """
    Envoie d'abord sur Discord si DISCORD_WEBHOOK_URL est défini,
    sinon Telegram si configuré, sinon log console "[tg]/[dc]".
    """
    payload_text = text
    # si extra est fourni, on l’ajoute joliment
    if extra:
        try:
            payload_text += "\n```json\n" + json.dumps(extra, indent=2) + "\n```"
        except Exception:
            payload_text += f"\n{extra}"

    if _discord_enabled():
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                await cli.post(DISCORD_WEBHOOK_URL, json={"content": payload_text})
            return
        except Exception as e:
            print("[dc] send error:", e)

    if _telegram_enabled():
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                await cli.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": TELEGRAM_CHAT_ID, "text": payload_text, "disable_web_page_preview": True},
                )
            return
        except Exception as e:
            print("[tg] send error:", e)

    # fallback console
    print("[notify]", payload_text)
