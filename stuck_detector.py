"""
Stuck / No-Movement / Customs-Hold Detection

Adds a second, *exception-focused* alert to ShipBot. The normal daily summary
tells the team what is arriving; this module watches for shipments that have
STOPPED progressing and pings the founders channel so someone can chase them.

Three conditions are detected (all configurable in config.py):

  1. STUCK_NO_SCAN     -- a shipment's tracking status/location has not changed
                          for >= STUCK_DAYS days (default 3). "No new scan."
  2. NO_LOCATION_CHANGE-- the same location has repeated across >= NO_MOVE_MIN_OBS
                          consecutive checks (default 2) while still in transit.
  3. CUSTOMS_HOLD      -- a customs/clearance scan keeps repeating across
                          >= CUSTOMS_MIN_OBS consecutive checks (default 2).

How it knows something is "stuck"
---------------------------------
The carrier APIs do not hand us a reliable "last scan timestamp" across every
carrier, so instead we compare each shipment's *signature* (status + location +
raw status text) between runs. We persist a small JSON state file between
GitHub Actions runs (via actions/cache) and measure how long each signature has
stayed unchanged. When a signature stops changing, the shipment has stopped
moving.

Alert hygiene
-------------
We only alert ONCE per "stuck episode" and then again only when severity
escalates (e.g. a no-movement watch becomes a 3-day stall, or a 3-day stall
becomes a 7-day stall). As soon as a shipment moves again (signature changes)
the state resets, so if it stalls a second time later it will alert again.

Carriers with no API data (DPD / UniUni / "1ST", or any row that returned no
location and no raw status) are skipped -- we can't tell a genuine stall from a
carrier we simply can't see, so we stay quiet rather than cry wolf.

The module is intentionally side-effect-light and dependency-free (only stdlib
+ the existing LarkClient) so it is easy to unit test. The single public entry
point is run_stuck_detection(all_results, lark).
"""

import os
import json
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Thresholds -- read from config when available, with safe fallbacks so this
# module also works standalone (e.g. in tests).
# ---------------------------------------------------------------------------
try:
    from config import (
        LARK_FOUNDERS_CHAT_ID,
        SCAN_STATE_PATH,
        STUCK_DAYS,
        STUCK_ESCALATE_DAYS,
        NO_MOVE_MIN_OBS,
        CUSTOMS_MIN_OBS,
        CUSTOMS_KEYWORDS,
        STATE_PRUNE_DAYS,
    )
except Exception:  # pragma: no cover - fallbacks for standalone/testing
    LARK_FOUNDERS_CHAT_ID = os.environ.get("LARK_CHAT_ID_FOUNDERS", "")
    SCAN_STATE_PATH = os.environ.get("SCAN_STATE_PATH", "state/scan_state.json")
    STUCK_DAYS = float(os.environ.get("STUCK_DAYS", "3"))
    STUCK_ESCALATE_DAYS = float(os.environ.get("STUCK_ESCALATE_DAYS", "7"))
    NO_MOVE_MIN_OBS = int(os.environ.get("NO_MOVE_MIN_OBS", "2"))
    CUSTOMS_MIN_OBS = int(os.environ.get("CUSTOMS_MIN_OBS", "2"))
    CUSTOMS_KEYWORDS = ["customs", "clearance", "duty", "import control",
                        "awaiting clearance", "held in customs"]
    STATE_PRUNE_DAYS = int(os.environ.get("STATE_PRUNE_DAYS", "21"))

# Human-readable status labels the bot assigns (see main._to_dropdown / STATUS_MAP)
IN_TRANSIT_LIKE = {"In Transit", "Exception/Delay"}
DELIVERED = "Delivered"

# Reason codes -> (emoji, short label) for the founders message
REASON_LABELS = {
    "CUSTOMS_HOLD": ("[CUSTOMS]", "Possible customs hold"),
    "STUCK_NO_SCAN": ("[STUCK]", "No movement / no new scan"),
    "NO_LOCATION_CHANGE": ("[STALLED]", "Same location, not progressing"),
}


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------
def load_state(path=None):
    path = path or SCAN_STATE_PATH
    try:
        if os.path.exists(path):
            with open(path, "r") as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Could not load scan state (%s): %s", path, e)
    return {}


def save_state(state, path=None):
    path = path or SCAN_STATE_PATH
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        with open(path, "w") as f:
            json.dump(state, f, indent=2, sort_keys=True)
        logger.info("Scan state saved (%d shipments tracked)", len(state))
    except Exception as e:
        logger.warning("Could not save scan state (%s): %s", path, e)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _now():
    return datetime.now(timezone.utc)


def _parse(ts):
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return _now()


def _signature(result):
    """A stable fingerprint of where a shipment is right now.

    When this string stops changing between runs, the shipment has stopped
    moving. We deliberately ignore delivery-date estimates (they jitter).
    """
    status = (result.get("new_status") or "").strip()
    location = (result.get("location") or "").strip().lower()
    raw = (result.get("raw_status") or "").strip().lower()
    return "%s|%s|%s" % (status, location, raw)


def _has_real_scan_data(result):
    """True only when the carrier gave us something to reason about.

    Rows from carriers we can't query (DPD/UniUni/1ST) or that errored come
    back with no location, no raw status and a label-created/unknown status.
    We skip those so we never alert on a shipment we simply can't see.
    """
    status = (result.get("new_status") or "").strip()
    location = (result.get("location") or "").strip()
    raw = (result.get("raw_status") or "").strip()
    return bool(location) or bool(raw) or status in IN_TRANSIT_LIKE


def _is_customs(result):
    text = ((result.get("raw_status") or "") + " " +
            (result.get("new_status") or "")).lower()
    return any(kw in text for kw in CUSTOMS_KEYWORDS)


def _display_name(result):
    """Mirror LarkClient._shipment_line naming so alerts read consistently."""
    recipient = (result.get("recipient") or "").strip()
    customer = (result.get("customer") or "").strip()
    if recipient.upper() == "BRENDAN":
        return "Brendan"
    if recipient.upper() == "CUSTOMER DIRECT":
        return customer or "Unknown"
    return customer or recipient or "Unknown"


def _dedupe(all_results):
    """One record per tracking number (multi-row shipments share a number)."""
    by_tn = {}
    for r in all_results:
        tn = (r.get("tracking_num") or "").strip()
        if not tn:
            continue
        existing = by_tn.get(tn)
        # Prefer the record that actually carries scan data.
        if existing is None or (not _has_real_scan_data(existing)
                                and _has_real_scan_data(r)):
            by_tn[tn] = r
    return by_tn


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------
def _evaluate(result, entry, now):
    """Return (stage, reason, days_unchanged) for one shipment.

    stage 0 = healthy, 1 = stalled (watch), 2 = stuck/customs, 3 = escalated.
    reason is None when stage == 0.
    """
    status = (result.get("new_status") or "").strip()
    if status == DELIVERED:
        return 0, None, 0.0

    observations = entry.get("observations", 1)
    days_unchanged = (now - _parse(entry["last_change"])).total_seconds() / 86400.0
    location = (result.get("location") or "").strip()

    customs = _is_customs(result) and observations >= CUSTOMS_MIN_OBS
    stuck3 = days_unchanged >= STUCK_DAYS and status in IN_TRANSIT_LIKE
    stuck7 = days_unchanged >= STUCK_ESCALATE_DAYS and status in IN_TRANSIT_LIKE
    nomove = (bool(location) and observations >= NO_MOVE_MIN_OBS
              and status in IN_TRANSIT_LIKE)

    stage = 0
    if nomove:
        stage = 1
    if stuck3 or customs:
        stage = 2
    if stuck7:
        stage = 3

    if customs:
        reason = "CUSTOMS_HOLD"
    elif stuck3 or stuck7:
        reason = "STUCK_NO_SCAN"
    elif nomove:
        reason = "NO_LOCATION_CHANGE"
    else:
        reason = None
        stage = 0

    return stage, reason, days_unchanged


def detect(all_results, state, now=None):
    """Update state from this run's results and return the list of alerts.

    Pure function (no I/O, no network) so it can be unit tested. Mutates and
    returns 'state'; returns (alerts, state).
    """
    now = now or _now()
    now_iso = now.isoformat()
    alerts = []
    seen_tns = set()

    for tn, result in _dedupe(all_results).items():
        seen_tns.add(tn)

        if not _has_real_scan_data(result):
            # Can't see this shipment -- drop any stale state, stay quiet.
            state.pop(tn, None)
            continue

        sig = _signature(result)
        entry = state.get(tn)

        if entry is None or entry.get("sig") != sig:
            # New shipment, or it moved since last run -> reset the clock.
            entry = {
                "sig": sig,
                "first_seen": (entry or {}).get("first_seen", now_iso),
                "last_change": now_iso,
                "last_seen": now_iso,
                "observations": 1,
                "alerted_sig": "",
                "alerted_stage": 0,
            }
        else:
            entry["observations"] = entry.get("observations", 1) + 1
            entry["last_seen"] = now_iso

        # Keep latest context for the message.
        entry["carrier"] = (result.get("carrier") or "").strip()
        entry["name"] = _display_name(result)
        entry["tab"] = (result.get("tab") or "").strip()
        entry["location"] = (result.get("location") or "").strip()
        entry["raw_status"] = (result.get("raw_status") or "").strip()
        entry["num_boxes"] = (result.get("num_boxes") or "").strip()

        stage, reason, days = _evaluate(result, entry, now)

        if reason and (sig != entry.get("alerted_sig")
                       or stage > entry.get("alerted_stage", 0)):
            alerts.append({
                "tracking_num": tn,
                "carrier": entry["carrier"],
                "name": entry["name"],
                "tab": entry["tab"],
                "location": entry["location"],
                "raw_status": entry["raw_status"],
                "num_boxes": entry["num_boxes"],
                "reason": reason,
                "stage": stage,
                "days_unchanged": round(days, 1),
                "observations": entry["observations"],
            })
            entry["alerted_sig"] = sig
            entry["alerted_stage"] = stage

        state[tn] = entry

    # Prune shipments we haven't seen for a while (delivered/removed).
    stale = []
    for tn, entry in state.items():
        if tn in seen_tns:
            continue
        age_days = (now - _parse(entry.get("last_seen", now_iso))).total_seconds() / 86400.0
        if age_days >= STATE_PRUNE_DAYS:
            stale.append(tn)
    for tn in stale:
        state.pop(tn, None)

    return alerts, state


# ---------------------------------------------------------------------------
# Message formatting + send
# ---------------------------------------------------------------------------
def build_message(alerts):
    NL = chr(10)
    n = len(alerts)
    header = "**Shipments need attention (%d)**" % n
    lines = [header,
             NL + "These shipments have stopped progressing since the last check:"]

    # Group by reason for readability.
    order = ["CUSTOMS_HOLD", "STUCK_NO_SCAN", "NO_LOCATION_CHANGE"]
    by_reason = {}
    for a in alerts:
        by_reason.setdefault(a["reason"], []).append(a)

    for reason in order:
        items = by_reason.get(reason)
        if not items:
            continue
        tag, label = REASON_LABELS[reason]
        lines.append(NL + "**%s %s**" % (tag, label))
        for a in items:
            carrier = a.get("carrier", "") or "?"
            tracking = a.get("tracking_num", "N/A")
            name = a.get("name", "")
            loc = a.get("location", "")
            raw = a.get("raw_status", "")
            days = a.get("days_unchanged", 0)
            tab = a.get("tab", "")
            num_boxes = a.get("num_boxes", "")

            box_tag = ""
            if num_boxes and num_boxes not in ("0", "1"):
                box_tag = " (%s boxes)" % num_boxes

            bits = []
            if raw:
                bits.append(raw)
            if loc:
                bits.append("@ " + loc)
            if reason in ("STUCK_NO_SCAN", "NO_LOCATION_CHANGE") and days:
                bits.append("no change for %.0f day(s)" % days)
            if a.get("stage") == 3:
                bits.append("STILL STUCK")
            detail = " - ".join(bits) if bits else "no recent scan"

            tab_tag = " [%s]" % tab if tab else ""
            lines.append("- **%s** %s%s -- %s%s: %s"
                         % (carrier, tracking, box_tag, name, tab_tag, detail))

    return NL.join(lines)


def send_founders_alert(lark, alerts, chat_id=None):
    """Post a red-banner alert card to the founders channel.

    Reuses the existing LarkClient red card helpers so no changes to
    lark_client.py are required.
    """
    target = chat_id or LARK_FOUNDERS_CHAT_ID
    if not target:
        logger.warning("LARK_FOUNDERS_CHAT_ID not set -- skipping founders alert "
                       "(%d shipments would have been reported)", len(alerts))
        return False
    if not alerts:
        return False

    message = build_message(alerts)
    try:
        card = lark._build_alert_card(message)
        lark._send_card("", target, card_json=card)
        logger.info("Founders alert sent (%d shipments)", len(alerts))
        return True
    except Exception as e:
        logger.warning("Founders alert card failed (%s), trying plain text", e)
        try:
            lark._send_text(message, target)
            return True
        except Exception as e2:
            logger.error("Founders alert plain text also failed: %s", e2)
            return False


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
def run_stuck_detection(all_results, lark, state_path=None, chat_id=None):
    """Load state, detect stuck shipments, alert the founders channel, save state.

    Called once per run from main.run_tracker(). Safe to call even when no
    founders channel is configured (it just logs and saves state).
    """
    path = state_path or SCAN_STATE_PATH
    state = load_state(path)
    alerts, state = detect(all_results, state)

    if alerts:
        logger.info("Stuck detector flagged %d shipment(s): %s",
                    len(alerts), ", ".join(a["tracking_num"] for a in alerts))
        send_founders_alert(lark, alerts, chat_id=chat_id)
    else:
        logger.info("Stuck detector: nothing new to flag")

    save_state(state, path)
    return alerts
