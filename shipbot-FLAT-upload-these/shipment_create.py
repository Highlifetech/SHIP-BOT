"""
Creating a shipment from the dashboard -- the app half of ShipBot.

Everything before this module reads the sheets. This one writes to them, and
it is the step that turns a tracker into a shipping tool: Hannah picks the
order lines she's actually packing, says how many of each and which box they
went in, types the tracking and the ship-to, and ShipBot writes the rows.
The packing lists then fall out of what she entered, because a shipment in
the sheets already *is* every row sharing a tracking number.

    open_orders(results)   what's ordered but not yet shipped, by board
    boards(results)        the sub-boards, for the filter
    validate(payload)      what's wrong before anything is written
    create(lark, ...)      appends the rows and returns the new shipment

Why unfulfilled lines come from the sheets rather than the client portal:
the portal's sales orders (SO-DW-0017 and friends) have no API yet, and the
inbound sheets already carry qty ordered against qty shipped on every line.
Anything ordered with quantity still outstanding, or with no tracking on it,
is an open line. When the portal grows an orders endpoint, this is the one
function that changes.
"""

import logging
import re
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

MAX_BOXES = 60
MAX_LINES = 200


def _int(v):
    v = str(v or "").strip().replace(",", "")
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def _s(v):
    return str(v or "").strip()


def _now_et():
    return datetime.now(timezone(timedelta(hours=-4)))


# ------------------------------------------------------------- open orders

def line_key(r):
    """A stable handle for one order line, so the browser can name it back."""
    return "%s:%s:%s" % (_s(r.get("sheet_token"))[:12], _s(r.get("tab")),
                         r.get("row_num"))


def open_orders(results):
    """Order lines with quantity still to ship.

    A line is open when more was ordered than has shipped, or when it has a
    quantity but no tracking number against it yet.
    """
    out = []
    for r in results or []:
        ordered = _int(r.get("qty_expected"))
        shipped = _int(r.get("qty_shipped"))
        tracking = _s(r.get("tracking_num"))
        product = _s(r.get("product_name"))
        order_num = _s(r.get("order_num"))
        if not ordered and not product:
            continue
        remaining = max(0, ordered - shipped)
        if not remaining and tracking:
            continue          # fully shipped and on its way
        if not remaining and not ordered:
            continue          # nothing to go on
        out.append({
            "key": line_key(r),
            "board": _s(r.get("tab")),
            "owner": _s(r.get("section")) or _s(r.get("tab")),
            "client": _s(r.get("customer")) or _s(r.get("recipient")),
            "order_num": order_num,
            "product": product,
            "photo": _s(r.get("product_photo")),
            "ordered": ordered,
            "shipped": shipped,
            "remaining": remaining or ordered,
            "sheet_token": _s(r.get("sheet_token")),
            "tab": _s(r.get("tab")),
            "row_num": r.get("row_num"),
            "tracked": bool(tracking),
        })
    out.sort(key=lambda o: (o["client"].lower(), o["order_num"],
                            o["product"].lower()))
    return out


def boards(results):
    """Every sub-board an order line can come from, with its open count."""
    counts = {}
    for o in open_orders(results):
        name = o["board"] or "Other"
        entry = counts.setdefault(name, {"board": name, "lines": 0,
                                         "units": 0,
                                         "token": o["sheet_token"]})
        entry["lines"] += 1
        entry["units"] += o["remaining"]
    return sorted(counts.values(), key=lambda b: b["board"].lower())


def clients(results):
    names = {o["client"] for o in open_orders(results) if o["client"]}
    return sorted(names, key=str.lower)


# -------------------------------------------------------------- validation

TRACKING_OK = re.compile(r"^[A-Z0-9]{8,40}$")


def validate(payload, available=None):
    """Everything wrong with a proposed shipment, in plain words.

    `available` maps a line key to how many units are still open, so the form
    can't ship more than was ordered.
    """
    problems = []
    lines = payload.get("lines") or []
    tracking = _s(payload.get("tracking")).upper()
    boxes = _int(payload.get("boxes"))

    if not lines:
        problems.append("Pick at least one order line to ship.")
    if len(lines) > MAX_LINES:
        problems.append("That's more than %d lines -- split it into two "
                        "shipments." % MAX_LINES)
    if not _s(payload.get("client")):
        problems.append("Client is required.")
    if not tracking:
        problems.append("Tracking number is required.")
    elif not TRACKING_OK.match(tracking):
        problems.append("That tracking number doesn't look right -- 8 to 40 "
                        "letters and digits, no spaces.")
    if not _s(payload.get("carrier")):
        problems.append("Carrier is required.")
    if boxes < 1:
        problems.append("A shipment needs at least one box.")
    if boxes > MAX_BOXES:
        problems.append("%d boxes is more than this form handles." % boxes)

    seen = set()
    for i, ln in enumerate(lines, start=1):
        qty = _int(ln.get("qty"))
        box = _int(ln.get("box"))
        key = _s(ln.get("key"))
        label = _s(ln.get("product")) or ("line %d" % i)
        if qty <= 0:
            problems.append("%s has no quantity." % label)
        if box < 1 or box > max(boxes, 1):
            problems.append("%s is assigned to box %d, which doesn't exist."
                            % (label, box))
        if key and key in seen:
            problems.append("%s appears twice." % label)
        seen.add(key)
        if available is not None and key in available and qty > available[key]:
            problems.append("%s: shipping %d but only %d are still open."
                            % (label, qty, available[key]))

    used = {_int(ln.get("box")) for ln in lines}
    empty = [n for n in range(1, boxes + 1) if n not in used]
    if empty and boxes:
        problems.append("Box%s %s empty -- put something in %s or drop the "
                        "box count." % ("es" if len(empty) > 1 else "",
                                        ", ".join(str(n) for n in empty) +
                                        (" are" if len(empty) > 1 else " is"),
                                        "them" if len(empty) > 1 else "it"))
    return problems


# ------------------------------------------------------------------ writing

def next_row(results, sheet_token, tab):
    """The first free row on a tab, from what the last scan saw."""
    rows = [r.get("row_num") or 0 for r in results or []
            if _s(r.get("sheet_token")) == sheet_token and _s(r.get("tab")) == tab]
    return (max(rows) if rows else 2) + 1


def plan(payload, results, columns):
    """The cells that would be written, without writing them.

    Separated from create() so the dashboard can show exactly what lands in
    the sheet before anyone commits, and so this is testable without Lark.
    """
    lines = sorted(payload.get("lines") or [],
                   key=lambda l: (_int(l.get("box")), _s(l.get("product"))))
    token = _s(payload.get("sheet_token"))
    tab = _s(payload.get("tab"))
    start = _int(payload.get("start_row")) or next_row(results, token, tab)
    tracking = _s(payload.get("tracking")).upper()

    def col(name):
        return _s(columns.get(name))

    updates, preview = [], []
    for n, ln in enumerate(lines):
        row = start + n
        # Tracking, carrier and box count are typed once on the first row and
        # carried down -- the shape every other part of ShipBot already reads.
        head = (n == 0)
        cells = {
            "customer": _s(payload.get("client")),
            "recipient": _s(payload.get("ship_to_name")) or _s(payload.get("client")),
            "order_num": _s(ln.get("order_num")) or _s(payload.get("po_ref")),
            "product_name": _s(ln.get("product")),
            "product_photo": _s(ln.get("photo")),
            "qty_shipped": str(_int(ln.get("qty"))),
            "qty_expected": str(_int(ln.get("ordered")) or _int(ln.get("qty"))),
            "box_num": str(_int(ln.get("box")) or 1),
            "tracking_num": tracking if head else "",
            "carrier": _s(payload.get("carrier")).upper() if head else "",
            "num_boxes": str(_int(payload.get("boxes"))) if head else "",
            "notes": _s(payload.get("notes")) if head else "",
            "status": "Label Created/Not Scanned" if head else "",
        }
        for field, value in cells.items():
            letter = col(field)
            if letter and value:
                updates.append({"row": row, "col": letter, "value": value})
        preview.append({"row": row, "box": _int(ln.get("box")) or 1,
                        "product": cells["product_name"],
                        "qty": cells["qty_shipped"],
                        "tracking": cells["tracking_num"]})

    return {"sheet_token": token, "tab": tab, "start_row": start,
            "rows": len(lines), "updates": updates, "preview": preview,
            "tracking": tracking}


def create(lark, results, payload, columns, sheet_id=None):
    """Write the shipment into the sheet and hand back what was created."""
    problems = validate(payload,
                        available={o["key"]: o["remaining"]
                                   for o in open_orders(results)})
    if problems:
        raise ValueError("; ".join(problems))

    work = plan(payload, results, columns)
    if not work["updates"]:
        raise ValueError("Nothing to write -- no mapped columns for these "
                         "fields. Check the column mapping for this sheet.")

    if sheet_id is None:
        sheet_id = _resolve_sheet_id(lark, work["sheet_token"], work["tab"])
    lark.write_cells(work["sheet_token"], sheet_id, work["updates"])
    logger.info("Created shipment %s: %d rows on %s starting at %d",
                work["tracking"], work["rows"], work["tab"], work["start_row"])

    return {"tracking": work["tracking"], "rows": work["rows"],
            "tab": work["tab"], "start_row": work["start_row"],
            "boxes": _int(payload.get("boxes")),
            "created": _now_et().strftime("%B %-d, %Y at %-I:%M %p")}


def _resolve_sheet_id(lark, sheet_token, tab):
    for meta in lark.get_sheet_metadata(sheet_token) or []:
        if _s(meta.get("title")) == tab or _s(meta.get("sheet_id")) == tab:
            return meta.get("sheet_id")
    raise ValueError("Can't find the tab %r on that spreadsheet." % tab)
