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
import card_builder  # NEW: the interactive tracker card
import card_lint  # NEW: validate cards before Lark has to
import dashboard  # NEW: the web dashboard routes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Bumped whenever the card/dashboard code changes. Logged at boot and exposed
# on /dashboard/health so "is the fix actually deployed?" is one HTTP call
# instead of a guess.
BUILD_ID = "2026-09-04.manual-entry.1"

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


def _active_snapshot():
    """Latest scan results, minus anything fully delivered."""
    results = chat._SNAPSHOT.get("results") or []
    return [r for r in results if not LarkClient._is_fully_delivered(r)]


CARD_SCHEMA = os.environ.get("CARD_SCHEMA", "2.0").strip()


def _card_variants(client="all", status="all"):
    """Every card layout to try, best first: (schema, builder).

    Lark answers an invalid card with a bare 400 and no field name, so a card
    that fails must have somewhere to fall back TO. Each candidate is linted
    against the documented schema before it is offered, so a malformed card is
    skipped here rather than being discovered by the chat.
    """
    active = _active_snapshot()
    sheet_count = len({(r.get("sheet_token") or "") for r in active})

    candidates = []
    if CARD_SCHEMA == "2.0":
        candidates.append(("2.0", card_builder.build_tracker_card_v2))
    candidates.append(("1.0", card_builder.build_tracker_card))

    for schema, build in candidates:
        try:
            card = build(active, client=client, status=status,
                         sheet_count=sheet_count)
        except Exception as e:
            logger.warning("%s failed to build (%s)", build.__name__, e)
            continue
        problems = card_lint.lint_v2(card) if schema == "2.0" \
            else card_lint.lint_v1(card)
        if problems:
            logger.error("%s produced an invalid card, skipping it: %s",
                         build.__name__, "; ".join(problems[:4]))
            continue
        yield schema, card


def _build_card(client="all", status="all"):
    """The best card layout that passes the linter."""
    for _schema, card in _card_variants(client, status):
        return card
    return card_builder.build_tracker_card(
        _active_snapshot(), client=client, status=status
    )


def _post_tracker_card(chat_id=None):
    """Send the tracker card, degrading one layout at a time.

    2.0 dashboard card -> 1.0 card -> plain text. Previously a rejected 2.0
    card dropped straight to plain text, which is why a single bad tag cost
    the team every button and filter instead of just the newer layout.
    """
    target = chat_id or LARK_CHAT_ID
    for schema, card in _card_variants():
        try:
            lark._send_card(None, target, card_json=json.dumps(card))
            logger.info("Tracker card sent (schema %s)", schema)
            return
        except Exception as e:
            logger.error("Card send failed on schema %s: %s", schema, e)
    logger.error("Every card layout failed; sending plain text")
    lark.send_group_message(
        card_builder.plain_text_fallback(_active_snapshot()), chat_id=target
    )


def _card_action_response(body):
    """Re-render the tracker card for a filter button / dropdown click."""
    event = body.get("event", body)
    action = event.get("action", {}) or body.get("action", {}) or {}
    value = action.get("value", {}) or {}

    client = value.get("client", "all") or "all"
    status = value.get("status", "all") or "all"

    # select_static puts the chosen option in `option`; buttons carry it in value.
    chosen = action.get("option") or ""
    kind = value.get("action", "")
    toast = None

    if kind == "client_filter":
        client = chosen or client
    elif kind == "status_filter":
        status = value.get("status", "all")
    elif kind == "mark_delivered":
        toast = _mark_delivered(chosen)

    card = _build_card(client=client, status=status)

    # Belt and braces: also patch the message directly, in case this Lark
    # tenant doesn't apply the card returned in the callback response.
    ctx = event.get("context", {}) or {}
    msg_id = ctx.get("open_message_id") or body.get("open_message_id") or ""
    if msg_id:
        threading.Thread(
            target=_safe_update_card, args=(msg_id, card), daemon=True
        ).start()

    if toast is None:
        label = ("All clients" if client == "all"
                 else client.replace("_", " ").title())
        toast = {"type": "info", "content": "Showing %s" % label}
    return {"toast": toast, "card": {"type": "raw", "data": card}}


def _mark_delivered(option_value):
    """Close out one shipment from the card and drop it from the snapshot."""
    target = card_builder.parse_delivered_value(option_value)
    if not target:
        return {"type": "error", "content": "Couldn't identify that shipment"}
    try:
        lark.mark_delivered(target["sheet_token"], target["tab"],
                            target["row_num"])
    except Exception as e:
        logger.error("mark_delivered failed for %s: %s", target, e)
        return {"type": "error", "content": "Sheet write failed — %s" % str(e)[:80]}

    results = chat._SNAPSHOT.get("results") or []
    for r in results:
        if ((r.get("sheet_token") or "") == target["sheet_token"]
                and r.get("row_num") == target["row_num"]):
            r["new_status"] = "Delivered"
            r["raw_status"] = "Marked delivered manually"
    return {"type": "success", "content": "Marked delivered"}


def _safe_update_card(message_id, card):
    try:
        lark.update_message_card(message_id, card)
    except Exception as e:
        logger.warning("Card patch failed for %s: %s", message_id, e)


def _handle_message(chat_id, message_id, question):
    """Route an @mention: live scan for 'refresh', else a chat answer."""
    try:
        q = (question or "").strip()

        # Reveal this chat's ID on request (for alert-routing config).
        _ql = q.lower()
        if "chat id" in _ql or "chatid" in _ql:
            logger.info("CHATID-REQUEST chat=%s", chat_id)
            lark.send_group_message("This chat's ID is: %s" % chat_id, chat_id=chat_id, message_id=message_id)
            return

        # Post the card summary on demand, from the latest snapshot.
        if _ql in ("card", "cardtest", "tracker", "board") or "card test" in _ql:
            logger.info("CARD request in chat=%s", chat_id)
            if not chat.has_snapshot():
                lark.send_group_message(
                    "One sec \u2014 pulling the latest shipment data\u2026",
                    chat_id=chat_id, message_id=message_id,
                )
                try:
                    chat.update_snapshot(run_tracker(dry_run=True))
                except Exception as e:
                    logger.error("Card snapshot warm-up failed: %s", e)
            _post_tracker_card(chat_id)
            return

        # Explicit live scan (also refreshes the chat snapshot).
        if not q or chat.is_full_summary_request(q):
            logger.info("Full-scan request in chat=%s", chat_id)
            results = run_tracker(dry_run=False, chat_id=LARK_CHAT_ID)  # summary always to deliveries, never URGENT
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

    if event_type == "card.action.trigger" or body.get("action"):
        try:
            logger.info("CARD-ACTION payload=%s", json.dumps(body)[:1800])
        except Exception:
            logger.info("CARD-ACTION keys=%s", list(body.keys()))
        try:
            return jsonify(_card_action_response(body))
        except Exception as e:
            logger.error("Card action failed: %s", e, exc_info=True)
            return jsonify({"toast": {"type": "error",
                                      "content": "Couldn't apply that filter"}})
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

def _warm_snapshot():
    """Prime the scan cache at boot.

    Without this the first person to open the dashboard pays for a full
    read-only scan of every sheet -- measured at 51s in production, close
    enough to gunicorn's 120s timeout to matter. Runs in the background so
    it never delays the worker coming up, and it is read-only: no sheet
    writes, no chat message.
    """
    try:
        results = run_tracker(dry_run=True)
        chat.update_snapshot(results)
        logger.info("Startup snapshot warmed: %d shipments", len(results))
    except Exception as e:
        logger.warning("Startup snapshot warm-up failed: %s", e)


logger.info("ShipBot build %s starting", BUILD_ID)
_fetch_bot_open_id()
dashboard.register(app, chat, run_tracker, lark)
threading.Thread(target=_warm_snapshot, daemon=True).start()
start_scheduler()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info("Starting shipment tracker webhook server on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False)
