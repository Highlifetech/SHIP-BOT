"""
One-time cleanup script: fix Status (col M) and Delivery Date (col Q) values.

Status column:
  - Maps any non-standard value to one of the 4 valid dropdown options:
    DELIVERED, IN TRANSIT, EXCEPTION/DELAY, LABEL CREATED/NOT SCANNED

Delivery Date column:
  - Normalises every date to MM-DD-YYYY format.

Usage:
    python cleanup.py              # live run -- writes corrections to sheets
    python cleanup.py --dry-run    # preview only, no writes
"""

import sys
import logging
import time
from datetime import datetime

from config import SHEET_TOKENS, COLUMNS, HEADER_ROW, SKIP_TABS
from lark_client import LarkClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# -- Valid dropdown values ---------------------------------------------------
VALID_STATUSES = {
    "DELIVERED",
    "IN TRANSIT",
    "EXCEPTION/DELAY",
    "LABEL CREATED/NOT SCANNED",
}

# Maps old / malformed status strings -> correct dropdown value
STATUS_CLEANUP_MAP = {
    # lowercase / mixed-case variants
    "delivered":                  "DELIVERED",
    "in transit":                 "IN TRANSIT",
    "in_transit":                 "IN TRANSIT",
    "intransit":                  "IN TRANSIT",
    "out for delivery":           "IN TRANSIT",
    "out_for_delivery":           "IN TRANSIT",
    "exception":                  "EXCEPTION/DELAY",
    "exception/delay":            "EXCEPTION/DELAY",
    "delay":                      "EXCEPTION/DELAY",
    "label created":              "LABEL CREATED/NOT SCANNED",
    "label_created":              "LABEL CREATED/NOT SCANNED",
    "label created/not scanned":  "LABEL CREATED/NOT SCANNED",
    "not scanned":                "LABEL CREATED/NOT SCANNED",
    "pending":                    "LABEL CREATED/NOT SCANNED",
    "pre-shipment":               "LABEL CREATED/NOT SCANNED",
    "pre-shipment info sent":     "LABEL CREATED/NOT SCANNED",
    "unknown":                    "LABEL CREATED/NOT SCANNED",
    "not_found":                  "LABEL CREATED/NOT SCANNED",
    "not found":                  "LABEL CREATED/NOT SCANNED",
    "awaiting shipment":          "LABEL CREATED/NOT SCANNED",
    "shipment information sent":  "LABEL CREATED/NOT SCANNED",
    "alert":                      "EXCEPTION/DELAY",
}

# Column letters
COL_STATUS = COLUMNS["status"]          # M
COL_DATE   = COLUMNS["delivery_date"]   # Q


# -- Date helpers ------------------------------------------------------------
def normalise_date(raw):
    """Return MM-DD-YYYY if parseable, else return the original string unchanged."""
    if not raw or not raw.strip():
        return ""
    val = raw.strip()

    # Already correct format?
    if _is_mm_dd_yyyy(val):
        return val

    # Try common input formats
    for fmt in (
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y%m%d",
    ):
        try:
            dt = datetime.strptime(val[:max(len(val), 19)], fmt)
            return dt.strftime("%m-%d-%Y")
        except (ValueError, TypeError):
            continue
    return val  # unrecognised -- leave as-is


def _is_mm_dd_yyyy(val):
    """Return True if val matches MM-DD-YYYY exactly."""
    try:
        datetime.strptime(val, "%m-%d-%Y")
        return True
    except (ValueError, TypeError):
        return False


# -- Core cleanup ------------------------------------------------------------
def cleanup_tab(lark, spreadsheet_token, sheet_id, tab_title, dry_run):
    """Read every row in one tab, collect needed fixes, and batch-write them."""
    start_row = HEADER_ROW + 1
    rows = lark.read_sheet_range(
        spreadsheet_token, sheet_id,
        start_col="A", end_col="Q",
        start_row=start_row, end_row=500,
    )

    fixes = []          # list of {"row": int, "col": str, "value": str}
    status_fixed = 0
    date_fixed = 0

    for i, row in enumerate(rows):
        if not isinstance(row, list):
            continue
        while len(row) < 17:
            row.append("")

        row_num = start_row + i

        # -- Status (col M, index 12) ----------------------------------------
        raw_status = str(row[12] or "").strip()
        if raw_status and raw_status not in VALID_STATUSES:
            lookup = raw_status.lower()
            new_status = STATUS_CLEANUP_MAP.get(lookup)
            if new_status is None:
                # Fuzzy fallback: check if any keyword matches
                upper = raw_status.upper()
                if "DELIVER" in upper:
                    new_status = "DELIVERED"
                elif "TRANSIT" in upper or "OUT FOR" in upper:
                    new_status = "IN TRANSIT"
                elif "EXCEPTION" in upper or "DELAY" in upper:
                    new_status = "EXCEPTION/DELAY"
                else:
                    new_status = "LABEL CREATED/NOT SCANNED"

            fixes.append({"row": row_num, "col": COL_STATUS, "value": new_status})
            status_fixed += 1
            logger.info(
                "  [%s] Row %d status: '%s' -> '%s'",
                tab_title, row_num, raw_status, new_status,
            )

        # -- Delivery Date (col Q, index 16) ---------------------------------
        raw_date = str(row[16] or "").strip()
        if raw_date:
            new_date = normalise_date(raw_date)
            if new_date != raw_date:
                fixes.append({"row": row_num, "col": COL_DATE, "value": new_date})
                date_fixed += 1
                logger.info(
                    "  [%s] Row %d date: '%s' -> '%s'",
                    tab_title, row_num, raw_date, new_date,
                )

    # -- Write ----------------------------------------------------------------
    if fixes and not dry_run:
        lark.write_cells(spreadsheet_token, sheet_id, fixes)

    logger.info(
        "  [%s] Done -- %d status fixes, %d date fixes (%s)",
        tab_title, status_fixed, date_fixed,
        "DRY RUN" if dry_run else "WRITTEN",
    )
    return status_fixed, date_fixed


def main():
    dry_run = "--dry-run" in sys.argv
    if dry_run:
        logger.info("=== DRY RUN -- no writes ===")

    lark = LarkClient()
    total_status = 0
    total_date = 0

    for token in SHEET_TOKENS:
        logger.info("Spreadsheet: %s", token)
        try:
            tabs = lark.get_sheet_metadata(token)
        except Exception as e:
            logger.error("  Failed to read spreadsheet %s: %s", token, e)
            continue

        for tab in tabs:
            title = tab["title"]
            sheet_id = tab["sheet_id"]
            if title in SKIP_TABS:
                continue
            logger.info("  Tab: %s", title)
            try:
                sf, df = cleanup_tab(lark, token, sheet_id, title, dry_run)
                total_status += sf
                total_date += df
            except Exception as e:
                logger.error("  Error in tab '%s': %s", title, e)
            time.sleep(0.3)

    logger.info(
        "=== Cleanup complete: %d status fixes, %d date fixes ===",
        total_status, total_date,
    )


if __name__ == "__main__":
    main()
