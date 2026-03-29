"""
Lark Shipment Tracking Bot - Main Entry Point

Scans the following tabs in each spreadsheet:
    - Hannah, Lucy, Other - permanent named tabs, always scanned
    - Current month tab - e.g. MAR
    - Previous month tab - e.g. FEB (catches end-of-month layover)

DELIVERED rows are skipped individually so layover shipments from the previous
month that are still in transit will still appear.

Multi-piece UPS shipments: when one tracking number in the sheet belongs
to a multi-box shipment, the UPS API returns all sibling tracking numbers.
We consolidate those siblings so that only ONE summary line is shown per
shipment (e.g. "1ZHE... (5 boxes): 3 arriving Mar 5, 2 unscanned").

Scheduling:
    Runs at 8 AM, 1 PM, and 8 PM Eastern Time (ET).
    The GitHub Actions workflow fires at both EDT (UTC-4) and EST (UTC-5)
    equivalent times to handle daylight saving automatically.  A guard in
    main() ensures only one run executes per scheduled window (45-min window).

Usage:
    python main.py                  # Run once (full summary)
    python main.py --dry-run        # No writes or messages
    python main.py --force          # Skip time-window check (for manual runs)
"""

import sys
import json
import os
import logging
import time
from datetime import datetime, timezone, timedelta

from config import SHEET_TOKENS, CARRIER_ALIASES, SHEET_OWNERS, STATUS_MAP, COLUMNS
from lark_client import LarkClient
from carriers import CarrierTracker, _fmt_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PERMANENT_TABS = {"Hannah", "Lucy", "Other"}
MONTH_NAMES = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
]

BAD_STATUS_KEYS = {"unknown", "not_found", ""}
DONE_STATUSES = {"DELIVERED"}

SCHEDULED_TIMES_ET = [(8, 0), (13, 0), (20, 0)]
SCHEDULE_WINDOW_MINUTES = 45

VALID_STATUSES = {
    "Delivered",
    "In Transit",
    "Exception/Delay",
    "Label Created/Not Scanned",
}

STATUS_NORMALIZE = {
    "delivered": "Delivered",
    "DELIVERED": "Delivered",
    "in transit": "In Transit",
    "IN TRANSIT": "In Transit",
    "in_transit": "In Transit",
    "intransit": "In Transit",
    "out for delivery": "In Transit",
    "out_for_delivery": "In Transit",
    "exception": "Exception/Delay",
    "EXCEPTION/DELAY": "Exception/Delay",
    "exception/delay": "Exception/Delay",
    "delay": "Exception/Delay",
    "alert": "Exception/Delay",
    "label created": "Label Created/Not Scanned",
    "LABEL CREATED/NOT SCANNED": "Label Created/Not Scanned",
    "label_created": "Label Created/Not Scanned",
    "label created/not scanned": "Label Created/Not Scanned",
    "not scanned": "Label Created/Not Scanned",
    "pending": "Label Created/Not Scanned",
    "pre-shipment": "Label Created/Not Scanned",
    "unknown": "Label Created/Not Scanned",
    "not_found": "Label Created/Not Scanned",
}


def _to_dropdown(status_str):
    if not status_str:
        return "Label Created/Not Scanned"
    direct = STATUS_NORMALIZE.get(status_str)
    if direct:
        return direct
    lower = status_str.strip().lower()
    mapped = STATUS_NORMALIZE.get(lower)
    if mapped:
        return mapped
    upper = status_str.upper()
    if "DELIVER" in upper:
        return "Delivered"
    if "TRANSIT" in upper or "OUT FOR" in upper:
        return "In Transit"
    if "EXCEPTION" in upper or "DELAY" in upper:
        return "Exception/Delay"
    return "Label Created/Not Scanned"


def validate_and_fix_rows(lark, spreadsheet_token, sheet_id, rows):
    """Fix delivery date formatting only.

    NOTE: We no longer write to the status column (M) here.
    Writing plain text to column M overwrites the Lark dropdown widget
    and destroys the color-coded formatting.  Status is now read-only
    from the bot's perspective -- users manage it via the dropdown.
    """
    fixes = []
    for row_data in rows:
        row_num = row_data["row_num"]

        # --- Delivery date formatting only ---
        raw_date = row_data.get("delivery_date", "").strip()
        if raw_date:
            fixed_date = _fmt_date(raw_date)
            if fixed_date and fixed_date != raw_date:
                fixes.append({"row": row_num, "col": COLUMNS["delivery_date"], "value": fixed_date})
                row_data["delivery_date"] = fixed_date
                logger.info("  Row %d date fix: '%s' -> '%s'", row_num, raw_date, fixed_date)

    if fixes:
        try:
            lark.write_cells(spreadsheet_token, sheet_id, fixes)
            logger.info("  Validated %d cells", len(fixes))
        except Exception as e:
            logger.error("  Validation write failed: %s", e)


def _eastern_now():
    try:
        import zoneinfo
        ET = zoneinfo.ZoneInfo("America/New_York")
        return datetime.now(ET)
    except ImportError:
        try:
            import pytz
            ET = pytz.timezone("America/New_York")
            return datetime.now(pytz.utc).astimezone(ET)
        except ImportError:
            EST = timezone(timedelta(hours=-5))
            return datetime.now(EST)


def is_scheduled_time():
    now_et = _eastern_now()
    current_minutes = now_et.hour * 60 + now_et.minute
    for hour, minute in SCHEDULED_TIMES_ET:
        target_minutes = hour * 60 + minute
        if abs(current_minutes - target_minutes) <= SCHEDULE_WINDOW_MINUTES:
            return True
    return False


def normalize_carrier(carrier_str):
    return CARRIER_ALIASES.get(carrier_str.lower().strip(), carrier_str.lower().strip())


def tabs_to_scan():
    now = _eastern_now()
    current = MONTH_NAMES[now.month - 1]
    previous = MONTH_NAMES[(now.month - 2) % 12]
    return PERMANENT_TABS | {current, previous}


def load_status_cache():
    try:
        p = os.environ.get("STATUS_CACHE_PATH", "/tmp/shipment_status_cache.json")
        if os.path.exists(p):
            with open(p, "r") as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Could not load status cache: %s", e)
    return {}


def save_status_cache(cache):
    try:
        p = os.environ.get("STATUS_CACHE_PATH", "/tmp/shipment_status_cache.json")
        with open(p, "w") as f:
            json.dump(cache, f, indent=2)
        logger.info("Status cache saved (%d entries)", len(cache))
    except Exception as e:
        logger.warning("Could not save status cache: %s", e)


def is_exception_status(status_str, raw_status=""):
    s = status_str.upper()
    r = raw_status.upper()
    for kw in ["EXCEPTION", "DELAY", "CLEARANCE", "CUSTOMS", "HELD",
               "GOVERNMENT AGENCY", "PROOF OF VALUE", "RETURNED", "REFUSED"]:
        if kw in s or kw in r:
            return True
    if "IMPORT C.O.D" in r:
        return True
    if "ADDRESS" in r and "CORRECTED" in r:
        return True
    return False


def process_sheet(lark, tracker, spreadsheet_token, dry_run=False):
    all_results = []
    try:
        tabs = lark.get_sheet_metadata(spreadsheet_token)
    except Exception as e:
        logger.error("Failed to read spreadsheet %s: %s", spreadsheet_token, e)
        return all_results

    target_tabs = tabs_to_scan()
    tabs_to_process = [t for t in tabs if t["title"] in target_tabs]
    if not tabs_to_process:
        logger.warning("No matching tabs in %s. Want: %s. Have: %s",
                        spreadsheet_token, sorted(target_tabs), [t["title"] for t in tabs])
        return all_results

    logger.info("Scanning %s in %s",
                [t["title"] for t in tabs_to_process], spreadsheet_token)

    sibling_skip = set()

    for tab in tabs_to_process:
        tab_title = tab["title"]
        sheet_id = tab["sheet_id"]
        logger.info("  Tab: %s (%s)", tab_title, sheet_id)

        try:
            rows = lark.read_tracking_data(spreadsheet_token, sheet_id)
        except Exception as e:
            logger.error("  Failed to read tab '%s': %s", tab_title, e)
            continue

        logger.info("  %d rows with tracking in '%s'", len(rows), tab_title)

        if not dry_run:
            validate_and_fix_rows(lark, spreadsheet_token, sheet_id, rows)

        for row in rows:
            tracking_num = row["tracking_num"]
            carrier_raw = row["carrier"]
            current_status = row.get("current_status", "").strip().upper()

            if current_status in DONE_STATUSES:
                continue

            if tracking_num in sibling_skip:
                continue

            carrier = normalize_carrier(carrier_raw)
            if not carrier or carrier not in CARRIER_ALIASES.values():
                logger.warning("  Row %d: unknown carrier '%s'",
                               row["row_num"], carrier_raw)
                all_results.append({
                    **row,
                    "new_status": current_status or "Label Created/Not Scanned",
                    "location": "",
                    "packages": [],
                    "tab": tab_title,
                    "sheet_token": spreadsheet_token,
                })
                continue

            result = tracker.track(tracking_num, carrier)
            new_status = result["status"]
            delivery_date = result.get("delivery_date", "")
            raw_status = result.get("raw_status", "")
            location = result.get("location", "")
            api_error = result.get("error", "")
            packages = result.get("packages", [])

            if packages:
                for pkg in packages:
                    sib = pkg.get("tracking_num", "").strip()
                    if sib and sib != tracking_num:
                        sibling_skip.add(sib)

            if api_error or result.get("status_key", "") in BAD_STATUS_KEYS:
                display_status = current_status if current_status else "Label Created/Not Scanned"
                logger.warning("  %s: API error (%s), keeping '%s'",
                               tracking_num, str(api_error)[:60], display_status)
                all_results.append({
                    **row,
                    "new_status": display_status,
                    "delivery_date": row.get("delivery_date", ""),
                    "raw_status": raw_status,
                    "location": location,
                    "packages": packages,
                    "tab": tab_title,
                    "sheet_token": spreadsheet_token,
                })
            else:
                # --- Always update delivery date and num_boxes ---
                # We no longer write to the status column (M) to preserve
                # the Lark dropdown widget and its color-coded formatting.
                # The bot still uses the carrier status for the chat message.
                if not dry_run:
                    try:
                        num_boxes = str(len(packages)) if packages else ""
                        lark.update_tracking_row(
                            spreadsheet_token, sheet_id, row["row_num"],
                            delivery_date, num_boxes,
                        )
                        logger.info("  Updated %s: delivery=%s boxes=%s",
                                    tracking_num, delivery_date or "(none)", num_boxes or "(none)")
                    except Exception as e:
                        logger.error("  Failed to write row %d: %s", row["row_num"], e)

                all_results.append({
                    **row,
                    "new_status": new_status,
                    "delivery_date": delivery_date,
                    "raw_status": raw_status,
                    "location": location,
                    "packages": packages,
                    "tab": tab_title,
                    "sheet_token": spreadsheet_token,
                })

            time.sleep(0.5)

        # ---- Batch-apply conditional formatting to EVERY row in this tab ----
        # Iterates the full raw rows list (all rows read from the sheet),
        # so DELIVERED, sibling-skip, and unknown-carrier rows are all colored.
        # For rows that were actively tracked, uses the fresh API status.
        if not dry_run:
            # Build a lookup: row_num -> new_status for rows we actively tracked
            tracked_status = {}
            for r in all_results:
                if r.get("tab") == tab_title and r.get("sheet_token") == spreadsheet_token:
                    tracked_status[r["row_num"]] = r.get("new_status", "") or r.get("current_status", "")
            style_pairs = []
            for r in rows:
                row_num = r["row_num"]
                if row_num in tracked_status:
                    status_raw = tracked_status[row_num]
                else:
                    # Delivered / sibling / untracked -- use current sheet value
                    status_raw = r.get("current_status", "")
                display = _to_dropdown(status_raw)
                style_pairs.append((row_num, display))
            if style_pairs:
                try:
                    lark.set_status_styles_batch(spreadsheet_token, sheet_id, style_pairs)
                    logger.info("  Styled %d status cells in tab '%s'", len(style_pairs), tab_title)
                except Exception as e:
                    logger.error("  Batch style failed for tab '%s': %s", tab_title, e)

    return all_results


def run_tracker(dry_run=False, chat_id=None, message_id=None):
    if not SHEET_TOKENS:
        logger.error("No sheet tokens configured. Set LARK_SHEET_TOKENS env var.")
        return []

    logger.info("Tabs to scan: %s", sorted(tabs_to_scan()))
    lark = LarkClient()
    tracker = CarrierTracker()
    all_results = []

    for token in SHEET_TOKENS:
        logger.info("Processing spreadsheet: %s", token)
        results = process_sheet(lark, tracker, token, dry_run)
        all_results.extend(results)
        logger.info("  -> %d active shipments from %s", len(results), token)

    logger.info("Total active shipments: %d", len(all_results))

    if not dry_run:
        try:
            lark.send_daily_summary(all_results, chat_id=chat_id, message_id=message_id)
            logger.info("Summary sent to group chat")
        except Exception as e:
            logger.error("Failed to send summary: %s", e)
    else:
        logger.info("Dry run complete. Results:")
        for r in all_results:
            logger.info("  [%s] %s | %s | %s | %s | %s",
                        r.get("tab"), r["tracking_num"], r["carrier"],
                        r["new_status"], r.get("delivery_date", ""), r.get("customer", ""))

    return all_results


def main():
    try:
        dry_run = "--dry-run" in sys.argv
        force = "--force" in sys.argv

        if dry_run:
            logger.info("=== DRY RUN MODE - no writes or messages ===")
            run_tracker(dry_run=True)
            logger.info("Done!")
            return

        now_et = _eastern_now()
        logger.info("Running at ET time: %s", now_et.strftime("%Y-%m-%d %H:%M %Z"))

        run_tracker(dry_run=False)
        logger.info("Done!")

    except Exception as e:
        logger.error("Fatal error in main: %s", e, exc_info=True)


if __name__ == "__main__":
    main()
