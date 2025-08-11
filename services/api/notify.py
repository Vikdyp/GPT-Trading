# services/api/notify.py
import os
import json
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

# --- Envs ---
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()
DISCORD_ENABLE = os.getenv("DISCORD_ENABLE", "true").strip().lower() in ("1", "true", "yes", "on")

DISCORD_USERNAME = os.getenv("DISCORD_USERNAME", "").strip()
DISCORD_AVATAR_URL = os.getenv("DISCORD_AVATAR_URL", "").strip()

TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "").strip()

try:
    import httpx
except Exception:
    httpx = None

def _discord_enabled() -> bool:
    return bool(DISCORD_ENABLE and DISCORD_WEBHOOK_URL and httpx)

def _telegram_enabled() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID and httpx)

async def _discord_post(payload: Dict[str, Any]) -> None:
    if not _discord_enabled():
        raise RuntimeError("Discord désactivé, webhook manquant, ou httpx indisponible.")
    async with httpx.AsyncClient(timeout=10) as cli:
        r = await cli.post(DISCORD_WEBHOOK_URL, json=payload)
        if r.status_code == 429:
            try:
                data = r.json()
            except Exception:
                data = {}
            retry = float(data.get("retry_after", 1.5))
            print(f"[discord] 429 rate limited, retry_after={retry}s")
            await asyncio.sleep(retry)
            r = await cli.post(DISCORD_WEBHOOK_URL, json=payload)
        if r.status_code >= 400:
            try:
                body = r.json()
            except Exception:
                body = r.text
            raise RuntimeError(f"[discord] HTTP {r.status_code}: {body}")

def _color_from_text(t: str) -> int:
    t = (t or "").upper()
    # Couleurs Discord (RGB décimal)
    if "✅" in t or "SUCCESS" in t or "OK" in t:
        return 0x2ECC71  # vert
    if "⚠" in t or "WARN" in t or "ATTENTION" in t:
        return 0xE67E22  # orange
    if "❌" in t or "ERROR" in t or "ERR" in t or "FAIL" in t:
        return 0xE74C3C  # rouge
    return 0x3498DB  # bleu neutre

async def notify(text: str, extra: Optional[Dict[str, Any]] = None) -> None:
    """
    Envoie un message :
      - Discord (embed) si configuré et activé,
      - Telegram si configuré,
      - sinon fallback console.
    """
    # Discord
    if _discord_enabled():
        try:
            embed = {
                "description": text,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "color": _color_from_text(text),
            }
            if extra:
                fields = []
                for k, v in extra.items():
                    fields.append({"name": str(k), "value": str(v), "inline": True})
                embed["fields"] = fields

            payload: Dict[str, Any] = {"embeds": [embed]}
            if DISCORD_USERNAME:
                payload["username"] = DISCORD_USERNAME
            if DISCORD_AVATAR_URL:
                payload["avatar_url"] = DISCORD_AVATAR_URL

            await _discord_post(payload)
            print("[discord] sent:", text)
            return
        except Exception as e:
            print("[discord] send error:", repr(e))

    # Telegram
    if _telegram_enabled():
        try:
            async with httpx.AsyncClient(timeout=10) as cli:
                msg = text
                if extra:
                    try:
                        kv = " | ".join(f"{k}={v}" for k, v in extra.items())
                        msg = f"{text} — {kv}"
                    except Exception:
                        pass
                r = await cli.post(
                    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
                    json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "disable_web_page_preview": True},
                )
                if r.status_code >= 400:
                    raise RuntimeError(f"[tg] HTTP {r.status_code}: {r.text}")
            print("[tg] sent:", text)
            return
        except Exception as e:
            print("[tg] send error:", repr(e))

    # Fallback console
    msg = text
    if extra:
        try:
            kv = " | ".join(f"{k}={v}" for k, v in extra.items())
            msg = f"{text} — {kv}"
        except Exception:
            pass
    print("[notify]", msg)

async def notify_embed(
    *,
    title: str,
    description: str = "",
    level: str = "INFO",
    fields: Optional[List[Dict[str, Any]]] = None,
    color: Optional[int] = None,
) -> None:
    """Envoie un embed Discord (si possible), sinon fallback sur notify()."""
    fields = fields or []
    if _discord_enabled():
        try:
            embed: Dict[str, Any] = {
                "title": f"[{level}] {title}",
                "description": description,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            if color is not None:
                embed["color"] = int(color)
            if fields:
                embed["fields"] = [
                    {"name": str(f.get("name", "")), "value": str(f.get("value", "")), "inline": bool(f.get("inline", True))}
                    for f in fields
                ]
            payload: Dict[str, Any] = {"embeds": [embed]}
            if DISCORD_USERNAME:
                payload["username"] = DISCORD_USERNAME
            if DISCORD_AVATAR_URL:
                payload["avatar_url"] = DISCORD_AVATAR_URL

            await _discord_post(payload)
            print("[discord] embed sent:", title)
            return
        except Exception as e:
            print("[discord] send error:", repr(e))

    # Fallback si pas de Discord
    extra = {f.get("name", "field"): f.get("value", "") for f in fields} if fields else None
    await notify(f"[{level}] {title}\n{description}", extra)
