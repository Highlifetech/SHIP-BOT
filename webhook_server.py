"""
Lark Shipment Tracking Bot - Webhook Server

Runs as a persistent web server (via gunicorn on Railway).
When the bot is @mentioned in the HLT INBOUND DELIVERIES group chat,
Lark sends a POST to /webhook. The server:
  1. Answers the URL verification challenge (one-time setup).
  2. On @mention, runs the full shipment tracker and replies in-thread.
  3. Ignores ALL messages that are not a direct @mention of the bot.
  4. Ignores its own outgoing messages to prevent response loops.
  5. Deduplicates Lark retries using message_id with a 5-min TTL.

Deployed the same way as IronBot:
  - Procfile: web: gunicorn webhook_server:app --bind 0.0.0.0:$PORT
  - Railway environment variables (same as GitHub Secrets)
"""

import os
import json
import re
import logging
import threading
import time
import requests
from flask import Flask, request, jsonify
from main import run_tracker
from lark_client import LarkClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

LARK_APP_ID = os.environ.get("LARK_APP_ID", "")
BOT_NAME = os.environ.get("BOT_NAME", "API Inbound Shipments Tracker")

lark = LarkClient()

# Bot's own open_id - fetched at startup so we can detect @mentions reliably
BOT_OPEN_ID = None

# Deduplication: prevent double-processing if Lark retries the same event
# Key: message_id  Value: timestamp first seen
processed_message_ids = {}
DEDUP_TTL = 300  # 5 minutes

# Lock to prevent race condition on the dedup dict
_dedup_lock = threading.Lock()


def _fetch_bot_open_id():
    """Fetch the bot's own open_id from Lark API at startup."""
    global BOT_OPEN_ID
    try:
        url = lark.base_url + "/open-apis/bot/v3/info"
        resp = requests.get(url, headers=lark._headers(), timeout=10)
        data = resp.json()
        if data.get("code") == 0:
            BOT_OPEN_ID = data.get("bot", {}).get("open_id", "")
            logger.info("Bot open_id fetched: %s", BOT_OPEN_ID)
        else:
            logger.warning("Could not fetch bot info: %s", data)
    except Exception as e:
        logger.warning("Error fetching bot open_id: %s", e)


def _is_already_processed(message_id):
    """
    Thread-safe deduplication check.
    Returns True if message_id was already handled (within TTL).
    Registers the message_id if it's new.
    Also evicts expired entries to keep the dict small.
    """
    now = time.time()
    with _dedup_lock:
        # Evict expired entries
        expired = [mid for mid, ts in processed_message_ids.items() if now - ts > DEDUP_TTL]
        for mid in expired:
            del processed_message_ids[mid]

        if message_id in processed_message_ids:
            return True  # Already handled

        # Mark as handled NOW - before we even start processing
        processed_message_ids[message_id] = now
        return False


def _is_bot_message(event):
    """
    Return True if this event was sent BY the bot itself.
    We check:
      - sender.sender_type == "bot"
      - sender.sender_id.open_id matches our BOT_OPEN_ID
    This prevents infinite loops where the bot sees its own reply.
    """
    sender = event.get("sender", {})
    sender_type = sender.get("sender_type", "")
    if sender_type == "bot":
        return True
    # Also check open_id in case sender_type is missing
    sender_open_id = sender.get("sender_id", {}).get("open_id", "")
    if BOT_OPEN_ID and sender_open_id == BOT_OPEN_ID:
        return True
    return False


def _bot_is_mentioned(msg):
    """
    Return True ONLY if the bot was explicitly @mentioned in this message.
    Checks:
      - msg.mentions list contains an entry matching our open_id or BOT_NAME
    Does NOT respond to p2p/direct messages without @mention (avoids noise).
    """
    mentions = msg.get("mentions", [])
    for mention in mentions:
        mid = mention.get("id", {})
        mention_open_id = mid.get("open_id", "")
        mention_name = mention.get("name", "")

        if BOT_OPEN_ID and mention_open_id == BOT_OPEN_ID:
            return True
        if BOT_NAME and BOT_NAME.lower() in mention_name.lower():
            return True
    return False


def _run_and_reply(chat_id, message_id):
    """Run the full tracker and send summary back - called in background thread."""
    try:
        logger.info("@mention trigger: chat=%s message=%s", chat_id, message_id)
        run_tracker(
            dry_run=False,
            chat_id=chat_id,
            message_id=message_id,
        )
    except Exception as e:
        logger.error("Error during @mention-triggered run: %s", e)
        try:
            lark.send_group_message(
                "Error running shipment tracker: " + str(e)[:200],
                chat_id=chat_id,
                message_id=message_id,
            )
        except Exception:
            pass


# -------------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(silent=True) or {}

    # 1. URL verification challenge (one-time during Lark bot setup)
    if body.get("type") == "url_verification":
        logger.info("URL verification challenge answered")
        return jsonify({"challenge": body.get("challenge", "")})

    # 2. Only handle im.message.receive_v1 events
    header = body.get("header", {})
    event_type = header.get("event_type", "")
    if event_type and event_type != "im.message.receive_v1":
        logger.debug("Ignoring non-message event: %s", event_type)
        return jsonify({"code": 0})

    event = body.get("event", {})
    msg = event.get("message", {})

    # 3. Only handle text messages
    if msg.get("message_type") != "text":
        return jsonify({"code": 0})

    # 4. Ignore messages sent BY the bot (prevents infinite loops)
    if _is_bot_message(event):
        logger.info("Ignoring bot's own message")
        return jsonify({"code": 0})

    # 5. Deduplication - return 200 immediately if already processed
    message_id = msg.get("message_id", "")
    if not message_id:
        return jsonify({"code": 0})

    if _is_already_processed(message_id):
        logger.info("Duplicate message ignored: %s", message_id)
        return jsonify({"code": 0})

    # 6. Only respond if bot is explicitly @mentioned - no @mention = no response
    if not _bot_is_mentioned(msg):
        logger.info("Bot not @mentioned - ignoring message (id=%s)", message_id)
        return jsonify({"code": 0})

    # 7. Get chat_id and launch tracker in background thread
    chat_id = msg.get("chat_id", "")
    if not chat_id:
        return jsonify({"code": 0})

    logger.info("@mention confirmed in chat=%s - launching tracker (msg=%s)", chat_id, message_id)

    threading.Thread(
        target=_run_and_reply,
        args=(chat_id, message_id),
        daemon=True,
    ).start()

    # Return 200 immediately so Lark does not retry
    return jsonify({"code": 0})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "bot_open_id": BOT_OPEN_ID})


@app.route("/list-chats", methods=["GET"])
def list_chats():
    """Helper endpoint to look up the group chat ID."""
    try:
        url = lark.base_url + "/open-apis/im/v1/chats"
        resp = requests.get(url, headers=lark._headers(), params={"page_size": 100}, timeout=30)
        data = resp.json()
        if data.get("code") != 0:
            return jsonify({"error": data})
        chats = data.get("data", {}).get("items", [])
        result = [{"chat_id": c.get("chat_id"), "name": c.get("name", "")} for c in chats]
        return jsonify({"chats": result, "count": len(result)})
    except Exception as e:
        return jsonify({"error": str(e)})


# -------------------------------------------------------------------------
# Startup
# -------------------------------------------------------------------------

_fetch_bot_open_id()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting shipment tracker webhook server on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False)
