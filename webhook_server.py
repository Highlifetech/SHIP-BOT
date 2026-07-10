"""
Lark Shipment Tracking Bot - Webhook Server (now with chat)

Two jobs, both intact:
  1. TRACKER (unchanged): scheduled 8am/8pm ET scans post the full summary,
     and any run also refreshes the chat snapshot.
  2. CHAT (new): when @mentioned with a question, the bot answers
     conversationally from the latest scan snapshot (fast — no carrier scan).
     Saying "refresh" / "full summary" triggers a live scan instead.

Deployed on Railway:
  - Procfile: web: gunicorn webhook_server:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120
  - Environment variables match GitHub Secrets (now also ANTHROPIC_API_KEY, BOT_MODEL)
"""

import os
import json
import logging
import threading
import time
import requests
from flask import Flask, request, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
from main import run_tracker
from lark_client import LarkClient
import chat  # NEW: the chat brain

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

LARK_APP_ID = os.environ.get("LARK_APP_ID", "")
BOT_NAME = os.environ.get("BOT_NAME", "API Inbound Shipments Tracker")
LARK_CHAT_ID = os.environ.get("LARK_CHAT_ID", "")

lark = LarkClient()

BOT_OPEN_ID = None
processed_message_ids = {}
DEDUP_TTL = 300
_dedup_lock = threading.Lock()
EASTERN = pytz.timezone("America/New_York")


# -------------------------------------------------------------------------
# Scheduled jobs  (tracker — unchanged behavior, now also refreshes snapshot)
# -------------------------------------------------------------------------

def scheduled_full_summary():
    """Send full shipment summary - runs at 8am and 8pm Eastern."""
    logger.info("=== SCHEDULED FULL SUMMARY ===")
    try:
        results = run_tracker(dry_run=False, chat_id=LARK_CHAT_ID)
        chat.update_snapshot(results)          # NEW: keep chat answers current
        logger.info("Scheduled full summary complete")
    except Exception as e:
        logger.error("Scheduled full summary failed: %s", e)


def start_scheduler():
    scheduler = BackgroundScheduler(timezone=EASTERN)
    scheduler.add_job(
        scheduled_full_summary,
        CronTrigger(hour=8, minute=0, timezone=EASTERN),
        id="summary_8am", name="8am Full Summary", replace_existing=True,
    )
    scheduler.add_job(
        scheduled_full_summary,
        CronTrigger(hour=20, minute=0, timezone=EASTERN),
        id="summary_8pm", name="8pm Full Summary", replace_existing=True,
    )
    scheduler.start()
    logger.info("Scheduler started: 8am, 8pm summary (Eastern time)")
    return scheduler


# -------------------------------------------------------------------------
# Bot helpers
# -------------------------------------------------------------------------

def _fetch_bot_open_id():
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
    now = time.time()
    with _dedup_lock:
        expired = [mid for mid, ts in processed_message_ids.items() if now - ts > DEDUP_TTL]
        for mid in expired:
            del processed_message_ids[mid]
        if message_id in processed_message_ids:
            return True
        processed_message_ids[message_id] = now
        return False


def _is_bot_message(event):
    sender = event.get("sender", {})
    if sender.get("sender_type", "") == "bot":
        return True
    sender_open_id = sender.get("sender_id", {}).get("open_id", "")
    if BOT_OPEN_ID and sender_open_id == BOT_OPEN_ID:
        return True
    return False


def _bot_is_mentioned(msg):
    mentions = msg.get("mentions", [])
    for mention in mentions:
        mid = mention.get("id", {})
        if BOT_OPEN_ID and mid.get("open_id", "") == BOT_OPEN_ID:
            return True
        if BOT_NAME and BOT_NAME.lower() in mention.get("name", "").lower():
            return True
    return False


def _handle_message(chat_id, message_id, question):
    """Route an @mention: live scan for 'refresh', else a chat answer."""
    try:
        q = (question or "").strip()

        # Explicit live scan (also refreshes the chat snapshot).
        if not q or chat.is_full_summary_request(q):
            logger.info("Full-scan request in chat=%s", chat_id)
            results = run_tracker(dry_run=False, chat_id=chat_id, message_id=message_id)
            chat.update_snapshot(results)
            return

        # Conversational question. Warm the snapshot once if we have none yet
        # (e.g. right after a redeploy) so the first answer is grounded.
        if not chat.has_snapshot():
            lark.send_group_message(
                "One sec — pulling the latest shipment data…",
                chat_id=chat_id, message_id=message_id,
            )
            try:
                results = run_tracker(dry_run=True)   # read-only: no writes, no summary
                chat.update_snapshot(results)
            except Exception as e:
                logger.error("Snapshot warm-up scan failed: %s", e)

        chat.answer_and_reply(q, chat_id, message_id, lark)

    except Exception as e:
        logger.error("Error handling message: %s", e)
        try:
            lark.send_group_message(
                "Sorry — I hit an error on that one.",
                chat_id=chat_id, message_id=message_id,
            )
        except Exception:
            pass


# -------------------------------------------------------------------------
# Routes
# -------------------------------------------------------------------------

@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_json(silent=True) or {}

    if body.get("type") == "url_verification":
        return jsonify({"challenge": body.get("challenge", "")})

    header = body.get("header", {})
    event_type = header.get("event_type", "")
    if event_type and event_type != "im.message.receive_v1":
        return jsonify({"code": 0})

    event = body.get("event", {})
    msg = event.get("message", {})

    if msg.get("message_type") != "text":
        return jsonify({"code": 0})
    if _is_bot_message(event):
        return jsonify({"code": 0})

    message_id = msg.get("message_id", "")
    if not message_id:
        return jsonify({"code": 0})
    if _is_already_processed(message_id):
        return jsonify({"code": 0})

    # In group chats, only respond when @mentioned. In 1:1 (p2p), always respond.
    chat_type = msg.get("chat_type", "")
    if chat_type != "p2p" and not _bot_is_mentioned(msg):
        return jsonify({"code": 0})

    chat_id = msg.get("chat_id", "")
    if not chat_id:
        return jsonify({"code": 0})

    # Ack Lark instantly (return 200 now); do the work in a background thread.
    question = chat.extract_question(msg)
    threading.Thread(
        target=_handle_message, args=(chat_id, message_id, question), daemon=True
    ).start()
    return jsonify({"code": 0})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "bot_open_id": BOT_OPEN_ID, "has_snapshot": chat.has_snapshot()})


# -------------------------------------------------------------------------
# Startup
# -------------------------------------------------------------------------

_fetch_bot_open_id()
start_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting shipment tracker webhook server on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False)
