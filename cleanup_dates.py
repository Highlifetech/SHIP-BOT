"""
One-off maintenance: standardize Delivery Date formatting and pull stray
delivery dates out of the Notes column.

For every target sheet (Hannah + Lucy inbound sheets and the 9 Hannah bulk
client sheets) and every month tab (TEMPLATE is skipped automatically):
  * If the Notes column contains a date, move it into the Delivery Date
    column (only when that cell is empty) and remove the date from Notes.
  * Reformat every Delivery Date value to M/D/YYYY (e.g. 7/1/2026).

Columns are located by header name, so it works whether the Delivery Date
column is L (bulk sheets) or Q (inbound sheets). A date is only cleared from
Notes when a Delivery Date column exists to hold it, so no date is ever lost.

Run with --dry-run (or DRY_RUN=true) to print every change without writing.
"""
import logging
import os
import re
import sys

from lark_client import LarkClient
from config import (
    SHEET_TOKENS,
    SHEET_OWNERS,
    CLIENT_SHEET_TOKENS,
    header_row_for,
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cleanup_dates")

DRY_RUN = ("--dry-run" in sys.argv
           or os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes"))

MAX_DATA_ROWS = 800

# Matches a full date: three numeric parts separated by / - or . (e.g.
# 7/1/2026, 07-01-2026, 2026.07.01). Requires two separators so partial
# fractions like "3/4" are ignored.
DATE_RE = re.compile(r"(\d{1,4})[/\-.](\d{1,2})[/\-.](\d{2,4})")


def _target_tokens():
    """Hannah + Lucy inbound sheets plus the 9 Hannah bulk client sheets."""
    def hannah_or_lucy(tok):
        owner = (SHEET_OWNERS.get(tok, "") or "").lower()
        return "hannah" in owner or "lucy" in owner
    main = [t for t in SHEET_TOKENS if hannah_or_lucy(t)]
    return list(dict.fromkeys(main + list(CLIENT_SHEET_TOKENS)))


def idx_to_col(i):
    """0-indexed column number -> spreadsheet letter (A, B, ... Z, AA...)."""
    s = ""
    i += 1
    while i:
        i, r = divmod(i - 1, 26)
        s = chr(65 + r) + s
    return s


def parse_date(text):
    """Extract (month, day, year) from a free-form string, or None."""
    if not text:
        return None
    m = DATE_RE.search(str(text))
    if not m:
        return None
    a, b, c = (int(x) for x in m.groups())
    if len(m.group(1)) == 4:            # YYYY-MM-DD
        year, month, day = a, b, c
    else:                               # M/D/YYYY or M/D/YY
        month, day, year = a, b, c
        if year < 100:
            year += 2000
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return month, day, year


def fmt(mdy):
    month, day, year = mdy
    return f"{month}/{day}/{year}"


def find_col(header, *keywords):
    """Index of first header cell containing all keywords (case-insensitive)."""
    for i, h in enumerate(header):
        text = str(h or "").strip().lower()
        if all(k in text for k in keywords):
            return i
    return -1


def cell(row, i):
    if i < 0 or i >= len(row) or row[i] is None:
        return ""
    return str(row[i]).strip()


def process_tab(client, token, owner, sid, title, hrow):
    rows = client.read_sheet_range(token, sid, "A", "Z", 1, hrow + MAX_DATA_ROWS)
    if not rows or len(rows) < hrow:
        return 0
    header = rows[hrow - 1]
    date_i = find_col(header, "delivery", "date")
    notes_i = find_col(header, "note")
    if date_i < 0 and notes_i < 0:
        return 0
    updates = []
    for offset, row in enumerate(rows[hrow:]):
        rownum = hrow + 1 + offset
        date_val = cell(row, date_i)
        notes_val = cell(row, notes_i)
        # 1) reformat an existing delivery date
        if date_i >= 0 and date_val:
            pd = parse_date(date_val)
            if pd:
                nf = fmt(pd)
                if nf != date_val:
                    updates.append({"col": idx_to_col(date_i),
                                    "row": rownum, "value": nf})
                    date_val = nf
        # 2) pull a date out of Notes -- only when we have a date column to
        #    move it into, so a date is never simply deleted.
        if notes_i >= 0 and notes_val and date_i >= 0:
            pd = parse_date(notes_val)
            if pd:
                nf = fmt(pd)
                if not date_val:
                    updates.append({"col": idx_to_col(date_i),
                                    "row": rownum, "value": nf})
                stripped = DATE_RE.sub("", notes_val).strip(" ,;-\t").strip()
                updates.append({"col": idx_to_col(notes_i),
                                "row": rownum, "value": stripped})
    if updates:
        logger.info("%s / %s -- %d change(s):", owner, title, len(updates))
        for u in updates:
            logger.info("    %s%s = %r", u["col"], u["row"], u["value"])
        if not DRY_RUN:
            client.write_cells(token, sid, updates)
    return len(updates)


def main():
    mode = "DRY RUN" if DRY_RUN else "LIVE"
    logger.info("=== Delivery-date cleanup (%s) ===", mode)
    client = LarkClient()
    tokens = _target_tokens()
    logger.info("Target sheets: %d", len(tokens))
    total = 0
    for token in tokens:
        owner = SHEET_OWNERS.get(token, token[:10])
        try:
            tabs = client.get_sheet_metadata(token) or []
        except Exception as e:
            logger.error("Failed metadata for %s (%s): %s", owner, token, e)
            continue
        hrow = header_row_for(token)
        logger.info("--- %s (%s): %d tab(s), header row %d ---",
                    owner, token, len(tabs), hrow)
        for tab in tabs:
            try:
                total += process_tab(client, token, owner,
                                     tab["sheet_id"], tab["title"], hrow)
            except Exception as e:
                logger.error("  %s / %s failed: %s", owner, tab.get("title"), e)
    logger.info("=== %s complete: %d cell change(s) %s ===",
                mode, total, "(not written)" if DRY_RUN else "written")


if __name__ == "__main__":
    main()
