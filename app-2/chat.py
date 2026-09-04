"""
Ship-Bot chat brain.

Turns Ship-Bot into a conversational assistant WITHOUT touching the tracker.
The scheduled scans still run and still post their summaries; this module just
lets people @mention the bot and ask questions about shipments in plain English.
"""

import os
import re
import time
import logging

logger = logging.getLogger(__name__)

# Default to the model Iron-Bot uses successfully on this account. Override with
# BOT_MODEL=claude-opus-4-8 once you've confirmed it's enabled for your key.
MODEL = os.environ.get("BOT_MODEL", "claude-sonnet-4-6").strip() or "claude-sonnet-4-6"
MAX_HISTORY_TURNS = 8
SNAPSHOT_MAX_AGE = 60 * 60

SYSTEM_PROMPT = (
    "You are Ship Bot, High Life Tech's in-house shipment assistant working inside "
    "the team's Lark chat. You are warm, sharp, and genuinely helpful — talk like a "
    "knowledgeable logistics coordinator, not a form. Keep answers tight and "
    "conversational: short paragraphs, plain language, no corporate filler.\n\n"
    "RULES:\n"
    "- Answer ONLY from the shipment data provided below. Do not invent tracking "
    "numbers, dates, or statuses.\n"
    "- If the answer isn't in the data, say so plainly and offer to run a full "
    "refresh (the user can say 'refresh' or 'full summary').\n"
    "- When someone asks about an order, customer, or tracking number, find the "
    "matching shipment(s) and give the status, ETA, carrier, and location in a "
    "sentence or two.\n"
    "- For 'what's stuck / delayed / needs attention', list the exception/delayed "
    "shipments concisely.\n"
    "- Prefer names and dates the person will recognize; include the tracking "
    "number when it's useful.\n"
    "- Never dump the entire list unless explicitly asked for a full summary."
)

_SNAPSHOT = {"results": [], "ts": 0.0}
_history = {}
_client = None


def _get_client():
    global _client
    if _client is not None:
        return _client
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not key:
        logger.warning("ANTHROPIC_API_KEY not set — chat replies disabled")
        return None
    try:
        import anthropic
        _client = anthropic.Anthropic(api_key=key)
    except Exception as e:  # noqa: BLE001
        logger.error("Failed to init Anthropic client: %s", e)
        _client = None
    return _client


def update_snapshot(results):
    if results is not None:
        _SNAPSHOT["results"] = results
        _SNAPSHOT["ts"] = time.time()
        logger.info("Chat snapshot updated: %d shipments", len(results))


def has_snapshot():
    return bool(_SNAPSHOT["results"])


FULL_SUMMARY_PATTERNS = re.compile(
    r"\b(refresh|re-?scan|rescan|full summary|full scan|run (the )?tracker|"
    r"scan now|update (the )?tracker|latest scan|pull latest)\b",
    re.IGNORECASE,
)


def is_full_summary_request(text):
    return bool(FULL_SUMMARY_PATTERNS.search(text or ""))


def extract_question(msg):
    import json
    try:
        content = json.loads(msg.get("content", "{}"))
        raw = (content.get("text") or "").strip()
    except Exception:  # noqa: BLE001
        return ""
    cleaned = re.sub(r"@\S+", "", raw).strip()
    return cleaned or raw


def _fmt_shipment(r):
    owner = r.get("tab") or r.get("recipient") or ""
    customer = r.get("customer") or r.get("recipient") or "Unknown"
    order = r.get("order_num") or ""
    carrier = r.get("carrier") or ""
    tracking = r.get("tracking_num") or ""
    status = r.get("new_status") or r.get("current_status") or ""
    raw = r.get("raw_status") or ""
    eta = r.get("delivery_date") or ""
    loc = r.get("location") or ""
    boxes = r.get("num_boxes") or ""
    bits = []
    if owner:
        bits.append(f"[{owner}]")
    if customer:
        bits.append(customer)
    if order:
        bits.append(f"order {order}")
    if carrier or tracking:
        bits.append(f"{carrier} {tracking}".strip())
    if boxes and str(boxes) not in ("", "1"):
        bits.append(f"{boxes} boxes")
    if status:
        bits.append(f"status: {status}")
    if raw and raw.lower() not in status.lower():
        bits.append(f"detail: {raw}")
    if eta:
        bits.append(f"ETA: {eta}")
    if loc:
        bits.append(f"location: {loc}")
    return " | ".join(bits)


def _shipments_context(results, limit=200):
    if not results:
        return "(No shipments in the latest snapshot yet.)"
    lines, seen = [], set()
    for r in results:
        tn = (r.get("tracking_num") or "").strip()
        key = (tn, r.get("order_num"), r.get("row_num"))
        if key in seen:
            continue
        seen.add(key)
        lines.append("- " + _fmt_shipment(r))
        if len(lines) >= limit:
            lines.append(f"... (+ more; {len(results)} total rows)")
            break
    return "\n".join(lines)


def _remember(chat_id, role, content):
    hist = _history.setdefault(chat_id, [])
    hist.append({"role": role, "content": content})
    if len(hist) > MAX_HISTORY_TURNS * 2:
        _history[chat_id] = hist[-MAX_HISTORY_TURNS * 2:]


def answer(question, chat_id):
    client = _get_client()
    if client is None:
        return ("Chat isn't configured yet — set ANTHROPIC_API_KEY on the Railway "
                "service and I'll be able to answer questions.")
    ctx = _shipments_context(_SNAPSHOT["results"])
    age = time.time() - (_SNAPSHOT["ts"] or 0)
    freshness = ""
    if _SNAPSHOT["ts"]:
        mins = int(age // 60)
        freshness = f"(Shipment data last refreshed ~{mins} min ago.)\n"
        if age > SNAPSHOT_MAX_AGE:
            freshness += "This snapshot is over an hour old; say 'refresh' for a live scan.\n"
    system = f"{SYSTEM_PROMPT}\n\n--- SHIPMENT DATA ---\n{freshness}{ctx}"
    messages = list(_history.get(chat_id, []))
    messages.append({"role": "user", "content": question})
    try:
        resp = client.messages.create(model=MODEL, max_tokens=1000, system=system, messages=messages)
        text = resp.content[0].text.strip()
    except Exception as e:  # noqa: BLE001
        logger.error("Anthropic call failed: %s", e)
        # Surface the real error while we're stabilizing (helps diagnose model/key issues).
        return f"I hit an error reaching the model ({MODEL}): {str(e)[:220]}"
    _remember(chat_id, "user", question)
    _remember(chat_id, "assistant", text)
    return text


def answer_and_reply(question, chat_id, message_id, lark):
    """Post an instant 'Working on it…' note, then replace it with the answer."""
    try:
        lark.send_group_message("_Working on it…_", chat_id=chat_id, message_id=message_id)
    except Exception:  # noqa: BLE001
        pass
    try:
        text = answer(question, chat_id)
    except Exception as e:  # noqa: BLE001
        logger.error("answer_and_reply failed: %s", e)
        text = "Something went wrong answering that — try again shortly."
    lark.send_group_message(text, chat_id=chat_id, message_id=message_id)
