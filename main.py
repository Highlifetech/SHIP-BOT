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
  equivalent times to handle daylight saving automatically. A guard in
  main() ensures only one run executes per scheduled window (45-min window).

Usage:
    python main.py               # Run once (full summary)
    python main.py --dry-run     # No writes or messages
    python main.py --force       # Skip time-window check (for manual runs)
"""

import sys
import json
import os
import logging
import time
from datetime import datetime, timezone, timedelta
from config import SHEET_TOKENS, CARRIER_ALIASES, SHEET_OWNERS
from lark_client import LarkClient
from carriers import CarrierTracker

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
BAD_STATUS_KEYS = {"unknown", "not_found", ""}  # internal status keys that mean "no real update"
DONE_STATUSES = {"DELIVERED"}

# Scheduled send times in Eastern Time (hour, minute)
SCHEDULED_TIMES_ET = [(8, 0), (13, 0), (20, 0)]   # 8 AM, 1 PM, 8 PM
# Allow this many minutes on either side of a scheduled time
SCHEDULE_WINDOW_MINUTES = 45

# EST = UTC-5 (standard); EDT = UTC-4 (daylight saving)
# We use pytz-aware Eastern time so DST is handled correctly.
def _eastern_now():
    """Return current datetime in US/Eastern (handles EDT vs EST automatically)."""
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
            # Fallback: assume EST (UTC-5) — will be off by 1 hr during summer
            EST = timezone(timedelta(hours=-5))
            return datetime.now(EST)


def is_scheduled_time():
    """
    Return True if the current ET time is within SCHEDULE_WINDOW_MINUTES of
    any scheduled send time.  This prevents double-firing when both the EDT
    and EST cron entries trigger on the same day.
    """
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
    """Load last-known statuses from cache file."""
    try:
        STATUS_CACHE_PATH = os.environ.get("STATUS_CACHE_PATH", "/tmp/shipment_status_cache.json")
        if os.path.exists(STATUS_CACHE_PATH):
            with open(STATUS_CACHE_PATH, "r") as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Could not load status cache: %s", e)
    return {}


def save_status_cache(cache):
    """Save current statuses to cache file."""
    try:
        STATUS_CACHE_PATH = os.environ.get("STATUS_CACHE_PATH", "/tmp/shipment_status_cache.json")
        with open(STATUS_CACHE_PATH, "w") as f:
            json.dump(cache, f, indent=2)
        logger.info("Status cache saved (%d entries)", len(cache))
    except Exception as e:
        logger.warning("Could not save status cache: %s", e)


def is_exception_status(status_str, raw_status=""):
    """Return True if the status string indicates a new problem."""
    s = status_str.upper()
    r = raw_status.upper()
    if "EXCEPTION" in s or "EXCEPTION" in r: return True
    if "DELAY" in s or "DELAY" in r: return True
    if "CLEARANCE" in r: return True
    if "IMPORT C.O.D" in r: return True
    if "CUSTOMS" in r: return True
    if "HELD" in r: return True
    if "GOVERNMENT AGENCY" in r: return True
    if "PROOF OF VALUE" in r: return True
    if "RETURNED" in r: return True
    if "REFUSED" in r: return True
    if "ADDRESS" in r and "CORRECTED" in r: return True
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
        logger.warning(
            "No matching tabs in %s. Want: %s. Have: %s",
            spreadsheet_token,
            sorted(target_tabs),
            [t["title"] for t in tabs],
        )
        return all_results

    logger.info("Scanning %s in %s", [t["title"] for t in tabs_to_process], spreadsheet_token)

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

        for row in rows:
            tracking_num = row["tracking_num"]
            carrier_raw = row["carrier"]
            current_status = row.get("current_status", "").strip().upper()

            if current_status in DONE_STATUSES:
                logger.info("  Skipping %s - already DELIVERED", tracking_num)
                continue
            if tracking_num in sibling_skip:
                logger.info("  Skipping %s - already covered by multi-box parent", tracking_num)
                continue

            carrier = normalize_carrier(carrier_raw)
            if not carrier or carrier not in CARRIER_ALIASES.values():
                logger.warning("  Row %d: unknown carrier '%s'", row["row_num"], carrier_raw)
                all_results.append({
                    **row,
                                  "new_status": current_status or "LABEL CREATED/NOT SCANNED",
                    "packages": [],
                    "tab": tab_title,
                    "sheet_token": spreadsheet_token,
                })
                continue

            result = tracker.track(tracking_num, carrier)
            new_status = result["status"]
            delivery_date = result.get("delivery_date", "")
            raw_status = result.get("raw_status", "")
            api_error = result.get("error", "")
            packages = result.get("packages", [])

            if packages:
                for pkg in packages:
                    sib = pkg.get("tracking_num", "").strip()
                    if sib and sib != tracking_num:
                        sibling_skip.add(sib)

                      if api_error or result.get("status_key", "") in BAD_STATUS_KEYS:
                            display_status = current_status if current_status else "LABEL CREATED/NOT SCANNED"
                logger.warning(
                    "  %s: API error (%s), keeping '%s'",
                    tracking_num, str(api_error)[:60], display_status,
                )
                all_results.append({
                    **row,
                    "new_status": display_status,
                    "delivery_date": row.get("delivery_date", ""),
                    "raw_status": raw_status,
                    "packages": packages,
                    "tab": tab_title,
                    "sheet_token": spreadsheet_token,
                })
            else:
                if not dry_run and new_status.upper() != current_status:
                    try:
                        lark.update_tracking_row(
                            spreadsheet_token, sheet_id, row["row_num"],
                            new_status, delivery_date,
                        )
                        logger.info("  Updated %s: %s -> %s", tracking_num, current_status, new_status)
                    except Exception as e:
                        logger.error("  Failed to write row %d: %s", row["row_num"], e)

                all_results.append({
                    **row,
                    "new_status": new_status,
                    "delivery_date": delivery_date,
                    "raw_status": raw_status,
                    "packages": packages,
                    "tab": tab_title,
                    "sheet_token": spreadsheet_token,
                })
            time.sleep(0.5)
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
            logger.info(
                "  [%s] %s | %s | %s | %s | %s",
                r.get("tab"), r["tracking_num"], r["carrier"],
                r["new_status"], r.get("delivery_date", ""), r.get("customer", ""),
            )

    return all_results


def main():
    dry_run = "--dry-run" in sys.argv
    force = "--force" in sys.argv

    if dry_run:
        logger.info("=== DRY RUN MODE - no writes or messages ===")
        run_tracker(dry_run=True)
        logger.info("Done!")
        return

    if not force and not is_scheduled_time():
        now_et = _eastern_now()
        logger.info(
            "Current ET time %s is not within %d min of a scheduled send time %s. "
            "Skipping. Use --force to override.",
            now_et.strftime("%H:%M"), SCHEDULE_WINDOW_MINUTES, SCHEDULED_TIMES_ET
        )
        return

    now_et = _eastern_now()
    logger.info("Running at ET time: %s", now_et.strftime("%Y-%m-%d %H:%M %Z"))
    run_tracker(dry_run=False)
    logger.info("Done!")


if __name__ == "__main__":
    main()
