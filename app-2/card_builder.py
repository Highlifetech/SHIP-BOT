"""
Ship-Bot chat card builder.

Renders the shipment summary as a real Lark interactive card instead of one
long markdown blob:

    +--------------------------------------------------+
    | 📦 HLT Shipment Tracker                          |
    |  🟢 12 delivered  🔵 41 in transit  ⚪ 12 …      |
    |  [ All ] [ Flagged ] [ In transit ] [ Not scanned]|
    |  [ Filter by client            v ]                |
    |  [ ✓ Mark as delivered         v ]                |
    |  ---                                              |
    |  Hannah                                           |
    |   🔴 HLT-SO6583 · Half Evil · Customs delay · FedEx|
    |  ---                                              |
    |  7Brew Coffee                                     |
    |   🔵 HLT-SO6566 · Import scan, Louisville KY · UPS|
    +--------------------------------------------------+

A section is a BOOK OF BUSINESS: the shared inbound sheets stay as Hannah /
Lucy / Other, and each dedicated client sheet is its own section, so Hannah
and Lucy read as clients in their own right. The customer for each row is
shown on the line. The status buttons and the client dropdown are wired to
`card.action.trigger`; the webhook re-renders this card from the latest scan
snapshot and returns it, so filtering is instant and costs no carrier calls.

"Mark as delivered" writes Delivered + today's date back to the sheet, for
the shipments carriers never post a final scan for.

The card is data-only -- it never calls Lark or the carriers -- so it is
cheap to build, easy to unit test, and safe to call from the webhook thread.
"""

import re
import logging
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

from config import SHEET_OWNERS

logger = logging.getLogger(__name__)

# --- glyphs -----------------------------------------------------------------
DOT_DELIVERED = "\U0001F7E2"   # green
DOT_ARRIVING = "\U0001F7E0"    # orange
DOT_TRANSIT = "\U0001F535"     # blue
DOT_UNSCANNED = "⚪"       # white
DOT_FLAGGED = "\U0001F534"     # red
SEP = "·"                 # middle dot

# Staff names that prefix a client sheet title ("2026 HANNAH 7BREW ...").
# Stripped so the card groups by CLIENT, not by who owns the sheet.
STAFF_NAMES = {"hannah", "lucy", "chen", "korey", "gabrielle", "brieanne",
               "elle", "brendan", "other"}

# Status buckets
FLAGGED = "flagged"        # exception, customs hold, refused, returned
ARRIVING = "arriving"      # out for delivery / landing today
TRANSIT = "transit"        # moving normally
UNSCANNED = "unscanned"    # label created, carrier hasn't scanned it
DELIVERED = "delivered"    # done -- counted in the header, not listed

BUCKET_DOT = {
    FLAGGED: DOT_FLAGGED,
    ARRIVING: DOT_ARRIVING,
    TRANSIT: DOT_TRANSIT,
    UNSCANNED: DOT_UNSCANNED,
    DELIVERED: DOT_DELIVERED,
}

# Order shipments appear inside a client section
BUCKET_ORDER = {FLAGGED: 0, ARRIVING: 1, TRANSIT: 2, UNSCANNED: 3, DELIVERED: 4}

# Which buckets each status tab keeps
TAB_BUCKETS = {
    "all": {FLAGGED, ARRIVING, TRANSIT, UNSCANNED},
    FLAGGED: {FLAGGED},
    TRANSIT: {ARRIVING, TRANSIT},
    UNSCANNED: {UNSCANNED},
}

STATUS_TABS = [
    ("all", "All"),
    (FLAGGED, "Flagged"),
    (TRANSIT, "In transit"),
    (UNSCANNED, "Not scanned"),
]

MAX_LINES_PER_CLIENT = 16
MAX_CLIENTS = 12

# An unscanned label this many days past its own ETA counts as flagged --
# and gets escalated to the urgent channel by stuck_detector.
OVERDUE_DAYS = 1

# Carrier boilerplate trimmed out of exception text
NOISE_PHRASES = [
    "We will deliver your package as soon as possible.",
    "We are experiencing transit delays.",
    "Your package will be delivered on the scheduled delivery date.",
    "This does not impact the scheduled delivery date.",
]

# Carriers write paragraphs; the card has one line. These are the sentences
# that turn up over and over, rewritten to what someone actually needs to know.
SHORTHAND = [
    ("forwarded to a ups facility in the destination city",
     "Forwarded to destination facility"),
    ("the package will be forwarded to", "Forwarded to"),
    ("shipment information sent to", "Label created, awaiting"),
    ("departed from facility", "Departed"),
    ("arrived at facility", "Arrived"),
    ("loaded on delivery vehicle", "On the truck"),
    ("in transit to next facility", "In transit"),
    ("customs clearance delay", "Customs delay"),
    ("held for payment of duties", "Held for duties"),
    ("processing at ups facility", "At UPS facility"),
    # "Your package has been released by a government agency" was being cut
    # to "...released by a", which reads as an unfinished sentence rather
    # than the good news it is.
    ("released by a government agency", "Cleared customs"),
    ("has been released by", "Released by"),
    ("is being held in customs", "Held in customs"),
    ("awaiting customs clearance", "Awaiting customs"),
    ("delivery attempted", "Delivery attempted"),
    ("delivery will be rescheduled", "Delivery rescheduled"),
    ("address information needed", "Needs an address"),
    ("incorrect address", "Bad address"),
]
MAX_DETAIL_CHARS = 110

# Carrier names as people write them, not as the sheet shouts them
CARRIER_DISPLAY = {
    "FEDEX": "FedEx", "FED EX": "FedEx", "UPS": "UPS", "USPS": "USPS",
    "DHL": "DHL", "ROYALMAIL": "Royal Mail", "ROYAL MAIL": "Royal Mail",
    "SFEXPRESS": "SF Express", "SF": "SF Express", "DPD": "DPD",
    "UNIUNI": "UniUni", "4PX": "4PX", "FOURPX": "4PX", "FIRST": "1ST",
}


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def _parse_date(raw):
    clean = str(raw or "").strip()[:10]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            return datetime.strptime(clean, fmt).date()
        except (ValueError, TypeError):
            continue
    return None


def days_overdue(r):
    """How many days past its own ETA an unscanned shipment is."""
    eta = _parse_date(r.get("delivery_date"))
    if not eta:
        return 0
    return max(0, (_now_et().date() - eta).days)


def bucket_for(r):
    """Map a tracked row onto one of the card buckets."""
    status = (r.get("new_status") or r.get("current_status") or "").upper()
    raw = (r.get("raw_status") or "").upper()

    if "EXCEPTION" in status or "DELAY" in status:
        return FLAGGED
    for kw in ("CUSTOMS", "CLEARANCE", "HELD", "RETURNED", "REFUSED",
               "ADDRESS CORRECT"):
        if kw in raw:
            return FLAGGED
    if "OUT FOR DELIVERY" in raw or "ON VEHICLE" in raw:
        return ARRIVING
    if "DELIVER" in status:
        return DELIVERED

    packages = r.get("packages") or []
    unscanned = ("LABEL CREATED" in status or "NOT SCANNED" in status
                 or (packages and not any(p.get("scanned") for p in packages)))
    if unscanned:
        # A label cut two weeks ago whose ETA has already passed is not
        # "waiting to be scanned" -- it's a problem nobody has looked at.
        return FLAGGED if days_overdue(r) >= OVERDUE_DAYS else UNSCANNED
    return TRANSIT


def slugify(name):
    s = re.sub(r"[^a-z0-9]+", "_", (name or "").lower()).strip("_")
    return s or "unknown"


def _strip_staff(name):
    """'Hannah 7Brew Coffee' -> '7Brew Coffee';  'Hannah' -> ''."""
    words = [w for w in (name or "").split() if w.lower() not in STAFF_NAMES]
    # "MFused 副本" and friends: a duplicated sheet is not a separate client.
    words = [w for w in words if w.lower() not in COPY_MARKERS]
    return " ".join(words).strip()


# Sheet-title noise that marks a duplicate, not a client
COPY_MARKERS = {"副本", "copy", "(copy)", "copy)", "backup", "old", "test"}

PLACEHOLDER_CUSTOMERS = {"", "-", "--", "N/A", "NA", "NONE", "UNKNOWN", "TBD",
                         "CUSTOMER DIRECT", "BRENDAN"}


def raw_client_for(r):
    """The client name exactly as this row spells it.

    Client sheets carry the client in the sheet title; the shared HANNAH /
    LUCY / OTHER sheets carry it in the Customer column -- often as two
    lines, "Contact name" over "Company", in which case the company wins.
    """
    owner = SHEET_OWNERS.get((r.get("sheet_token") or "").strip(), "")
    from_sheet = _strip_staff(owner)
    if from_sheet:
        return from_sheet, True  # authoritative: the sheet IS the client

    customer = (r.get("customer") or "").strip()
    lines = [l.strip() for l in re.split(r"[\r\n]+", customer) if l.strip()]
    if len(lines) > 1:
        customer = lines[-1]          # "Mike Christman / Spectrum Promo"
    elif lines:
        customer = lines[0]

    if customer and customer.upper() not in PLACEHOLDER_CUSTOMERS:
        return (customer if customer != customer.upper() else customer.title()), False

    recipient = (r.get("recipient") or "").strip()
    if recipient and recipient.upper() not in PLACEHOLDER_CUSTOMERS:
        return recipient.title(), False
    return "Unassigned", False


def _stem(name):
    """Collapsed comparison key: '7 Brew (MKE)' -> '7brewmke'."""
    base = re.sub(r"\(.*?\)", " ", name or "")     # drop "(MKE)", "(1)"
    return re.sub(r"[^a-z0-9]+", "", base.lower())


def _same_client(a, b, min_stem=5):
    """True when two spellings are the same client.

    '7Brew Coffee' / '7 Brew (MKE)' share the stem '7brew';
    'Craftworks' / 'Craftworks Design' share 'craftworks'.
    """
    sa, sb = _stem(a), _stem(b)
    if not sa or not sb:
        return False
    if sa == sb:
        return True
    short, long_ = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
    return len(short) >= min_stem and long_.startswith(short)


def build_client_map(results):
    """Map every spelling in this scan to one canonical client name.

    Names taken from a client sheet win, because that's the spelling the
    team already uses for the account; otherwise the fullest spelling wins.
    """
    seen = {}   # raw name -> authoritative?
    for r in results or []:
        name, authoritative = raw_client_for(r)
        seen[name] = seen.get(name, False) or authoritative

    canonical = {}
    groups = []   # [(canonical_name, authoritative, [members])]
    for name in sorted(seen, key=lambda n: (not seen[n], -len(n), n)):
        for g in groups:
            if _same_client(g[0], name):
                g[2].append(name)
                break
        else:
            groups.append((name, seen[name], [name]))

    for canon, _auth, members in groups:
        for m in members:
            canonical[m] = canon
    return canonical


def client_for(r, client_map=None):
    name = raw_client_for(r)[0]
    return (client_map or {}).get(name, name)


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------
# A section is a book of business, not a single customer: the shared inbound
# sheets stay as Hannah / Lucy / Other, and each dedicated client sheet is its
# own section. That way Hannah and Lucy read as clients in their own right and
# the dropdown filters between them.

PERMANENT_SECTIONS = ["Hannah", "Lucy", "Other"]


def section_for(r):
    owner = SHEET_OWNERS.get((r.get("sheet_token") or "").strip(), "") or "Other"
    return _strip_staff(owner) or owner


def line_name(r, section):
    """Who the shipment is for, shown on the line under a section header."""
    name = raw_client_for(r)[0]
    if not name or name == "Unassigned" or _same_client(name, section):
        return ""
    return name


def group_by_section(results):
    """-> OrderedDict {section: [rows]}, permanent books first."""
    groups = {}
    for r in results:
        groups.setdefault(section_for(r), []).append(r)

    def sort_key(item):
        name, rows = item
        if name in PERMANENT_SECTIONS:
            return (0, PERMANENT_SECTIONS.index(name), "")
        flagged = sum(1 for r in rows if bucket_for(r) == FLAGGED)
        return (1, -flagged, name.lower())

    ordered = OrderedDict()
    for name, rows in sorted(groups.items(), key=sort_key):
        ordered[name] = sorted(
            rows, key=lambda r: (BUCKET_ORDER[bucket_for(r)], _ref(r))
        )
    return ordered


def mark_delivered_value(r):
    """Opaque handle a 'mark delivered' option sends back to the webhook."""
    return "md|%s|%s|%s" % (r.get("sheet_token", ""), r.get("row_num", ""),
                            (r.get("tab") or "")[:24])


def parse_delivered_value(value):
    parts = (value or "").split("|")
    if len(parts) != 4 or parts[0] != "md":
        return None
    token, row, tab = parts[1], parts[2], parts[3]
    if not token or not str(row).isdigit():
        return None
    return {"sheet_token": token, "row_num": int(row), "tab": tab}


# ---------------------------------------------------------------------------
# Line rendering
# ---------------------------------------------------------------------------

def _tracking_url(tracking, carrier):
    c = (carrier or "").strip().upper()
    if c.startswith("FED"):
        return "https://www.fedex.com/fedextrack/?trknbr=%s" % tracking
    if c == "UPS":
        return "https://www.ups.com/track?tracknum=%s" % tracking
    if c == "USPS":
        return "https://tools.usps.com/go/TrackConfirmAction?tLabels=%s" % tracking
    if c.startswith("DHL"):
        return "https://www.dhl.com/en/express/tracking.html?AWB=%s" % tracking
    if c.startswith("PUROLATOR"):
        return "https://www.purolator.com/en/shipping/tracker?pin=%s" % tracking
    if c.startswith("ROYAL"):
        return ("https://www.royalmail.com/track-your-item#/tracking-results/%s"
                % tracking)
    # 17track resolves the carriers with no usable public tracking page of
    # their own (EWS, DPD, UniUni, 4PX), so every row gets a working link.
    if tracking:
        return "https://t.17track.net/en#nums=%s" % tracking
    return ""


def _short_date(raw):
    """'2026-09-08' -> 'Sep 8'; today/tomorrow get words instead."""
    if not raw:
        return ""
    clean = str(raw).strip()[:10]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(clean, fmt).date()
        except (ValueError, TypeError):
            continue
        today = datetime.now(timezone(timedelta(hours=-4))).date()
        if dt == today:
            return "Today"
        if dt == today + timedelta(days=1):
            return "Tomorrow"
        return "%s %d" % (dt.strftime("%b"), dt.day)
    return clean


def _detail(r, bucket, terse=False):
    """The human-readable middle of a shipment line.

    `terse` drops the day count, for callers that have already put how late
    the shipment is at the front of the line -- "16d late ... Label created 16
    days ago" said the same thing twice and crowded out the carrier's own
    words, which are the part nobody can guess.
    """
    raw = (r.get("raw_status") or "").strip()
    location = (r.get("location") or "").strip()
    city = location.replace(" - ", ",").split(",")[0].strip() if location else ""

    overdue = days_overdue(r)
    if terse:
        unscanned_text = "Never scanned"
    else:
        unscanned_text = (
            "Label created %d days ago, never scanned" % overdue
            if overdue >= OVERDUE_DAYS else "Label created, not yet scanned"
        )

    status = (r.get("new_status") or "").upper()
    if raw and not ("LABEL CREATED" in status or "NOT SCANNED" in status):
        text = _tidy(raw)
    elif "LABEL CREATED" in status or "NOT SCANNED" in status:
        text = unscanned_text
    elif bucket in (DELIVERED, ARRIVING):
        text = "Delivered" if bucket == DELIVERED else "Out for delivery"
    elif bucket == FLAGGED:
        text = "Exception"
    else:
        text = "In transit"

    # "US" / "GB" alone is a country code, not a place worth appending.
    if len(city) <= 3 and city.isupper():
        city = ""
    if city and city.lower() not in text.lower():
        text = "%s, %s" % (text, city)
    return text


def _tidy(raw):
    """Trim carrier boilerplate and cap the length of a status blurb."""
    text = raw.strip()
    for phrase in NOISE_PHRASES:
        text = text.replace(phrase, "")
    lowered = text.lower()
    for needle, short in SHORTHAND:
        idx = lowered.find(needle)
        if idx != -1:
            text = short + text[idx + len(needle):]
            break
    text = re.sub(r"\s*/\s*", " / ", text)
    text = re.sub(r"\s+", " ", text).strip(" /")
    if len(text) > MAX_DETAIL_CHARS:
        cut = text[:MAX_DETAIL_CHARS].rsplit(" ", 1)[0]
        text = cut.rstrip(".,;/ ") + "…"
    text = text.rstrip(". ")
    return text[:1].upper() + text[1:] if text else text


# main.auto_shipment_id() invents an ID for rows whose Shipment ID column is
# blank -- carrier prefix plus the last four characters of the tracking number
# ("FDX-8062", "SHP-43YQ", "DHL-9390"). Those aren't real references anyone can
# look up, so they don't belong in front of a shipment; the tracking number
# does. Real IDs from the sheet (HLT-SO6613, HLT-007-L) are shown as written.
AUTO_ID = re.compile(r"^(FDX|UPS|USP|DHL|RM|SF|SHP)-[A-Z0-9]{4}$")


def _ref(r):
    """Shipment reference shown in bold at the head of the line."""
    sid = (r.get("shipment_id") or "").strip()
    if sid and not AUTO_ID.match(sid.upper()):
        return sid
    order = (r.get("order_num") or "").strip()
    if order:
        return order
    return (r.get("tracking_num") or "").strip() or "No tracking #"


def shipment_line(r, bucket=None, section=""):
    """One shipment, in the order people read it:

        <dot> tracking# (linked) - order no - customer - what's happening - carrier - ETA

    The tracking number leads because it's the handle: the thing you click,
    paste into an email, or read out on a call.
    """
    bucket = bucket or bucket_for(r)
    tracking = (r.get("tracking_num") or "").strip()
    carrier = (r.get("carrier") or "").strip().upper()
    url = _tracking_url(tracking, carrier)

    parts = []

    # How late it is goes first, where the eye lands. Fourteen days stuck and
    # three days stuck were reading with identical weight, buried mid-sentence
    # in the carrier's own words.
    late = days_overdue(r)
    if late and bucket in (FLAGGED, UNSCANNED):
        parts.append("**%dd late**" % late)

    if tracking:
        parts.append("[**%s**](%s)" % (tracking, url) if url
                     else "**%s**" % tracking)

    ref = _ref(r)                       # order / shipment no from the sheet
    if ref and ref != tracking:
        parts.append(ref)

    # Customer, or failing that whose desk it is. A line that names nobody
    # reads as broken data -- and several sheet rows carry the tracking
    # number on a continuation line with the client only on the head row.
    name = line_name(r, section) or ("" if section else section_for(r))
    if name:
        parts.append(name)

    parts.append(_detail(r, bucket, terse=bool(late)))   # what's happening

    if carrier:
        # Unknown short all-caps codes (EWS, GLS) stay as written.
        parts.append("`%s`" % CARRIER_DISPLAY.get(
            carrier, carrier if len(carrier) <= 5 else carrier.title()))

    # A date on its own is read as a promise. When the date has already gone
    # by, say so -- "Aug 27" and "ETA was Aug 27" are opposite news, and the
    # card was showing them identically.
    date = _short_date(r.get("delivery_date"))
    if date:
        if date in ("Today", "Tomorrow"):
            parts.append("**%s**" % date)
        elif days_overdue(r):
            parts.append("<font color='grey'>ETA was %s</font>" % date)
        else:
            parts.append(date)

    boxes = str(r.get("num_boxes") or "").strip()
    packages = r.get("packages") or []
    n_boxes = len(packages) if len(packages) > 1 else (
        int(boxes) if boxes.isdigit() and int(boxes) > 1 else 0
    )
    if n_boxes:
        parts.append("%d boxes" % n_boxes)

    return BUCKET_DOT[bucket] + " " + (" %s " % SEP).join(parts)


# ---------------------------------------------------------------------------
# Grouping
# ---------------------------------------------------------------------------

def dedupe(results):
    """One line per tracking number (sheets repeat it across item rows)."""
    seen, unique = set(), []
    for r in results or []:
        tn = (r.get("tracking_num") or "").strip()
        key = tn or "row:%s:%s" % (r.get("sheet_token"), r.get("row_num"))
        if key in seen:
            continue
        seen.add(key)
        unique.append(r)
    return unique


def group_by_client(results, client_map=None):
    """-> OrderedDict {client: [rows]} sorted by urgency then size."""
    if client_map is None:
        client_map = build_client_map(results)
    groups = {}
    for r in results:
        groups.setdefault(client_for(r, client_map), []).append(r)

    def sort_key(item):
        client, rows = item
        flagged = sum(1 for r in rows if bucket_for(r) == FLAGGED)
        return (-flagged, -len(rows), client.lower())

    ordered = OrderedDict()
    for client, rows in sorted(groups.items(), key=sort_key):
        ordered[client] = sorted(
            rows, key=lambda r: (BUCKET_ORDER[bucket_for(r)], _ref(r))
        )
    return ordered


def counts(results):
    out = {FLAGGED: 0, ARRIVING: 0, TRANSIT: 0, UNSCANNED: 0, DELIVERED: 0}
    for r in results:
        out[bucket_for(r)] += 1
    return out


# ---------------------------------------------------------------------------
# Card
# ---------------------------------------------------------------------------

def _now_et():
    return datetime.now(timezone(timedelta(hours=-4)))


def _delivered_label(r):
    """Short, unambiguous label for the 'mark as delivered' dropdown."""
    bits = [_ref(r)]
    name = line_name(r, "")
    if name:
        bits.append(name)
    carrier = (r.get("carrier") or "").strip().upper()
    if carrier and len(bits) < 2:
        bits.append(CARRIER_DISPLAY.get(carrier, carrier))
    return (" %s " % SEP).join(bits)[:70]


def build_tracker_card(results, client="all", status="all", sheet_count=None):
    """Build the Lark interactive card JSON (dict).

    `client` is a slug from the dropdown, `status` one of the tab keys.
    Both default to "all".
    """
    rows = dedupe(results or [])
    totals = counts(rows)

    # Delivered shipments are counted in the header but never listed --
    # nobody needs to read a line about a box that already arrived.
    listable = [r for r in rows if bucket_for(r) != DELIVERED]
    all_sections = list(group_by_section(listable).keys())

    keep = TAB_BUCKETS.get(status or "all", TAB_BUCKETS["all"])
    filtered = [r for r in listable if bucket_for(r) in keep]
    if client and client != "all":
        filtered = [r for r in filtered if slugify(section_for(r)) == client]

    grouped = group_by_section(filtered)

    summary = "   ".join([
        "%s **%d** delivered" % (DOT_DELIVERED, totals[DELIVERED]),
        "%s **%d** in transit" % (DOT_TRANSIT, totals[TRANSIT] + totals[ARRIVING]),
        "%s **%d** not scanned" % (DOT_UNSCANNED, totals[UNSCANNED]),
        "%s **%d** flagged" % (DOT_FLAGGED, totals[FLAGGED]),
    ])

    elements = [
        {"tag": "div", "text": {"tag": "lark_md", "content": summary}},
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "button",
                    "text": {"tag": "plain_text", "content": label},
                    "type": "primary" if key == status else "default",
                    "value": {"action": "status_filter", "status": key,
                              "client": client},
                }
                for key, label in STATUS_TABS
            ],
        },
        {
            "tag": "action",
            "actions": [
                {
                    "tag": "select_static",
                    "placeholder": {"tag": "plain_text",
                                    "content": "Filter by client"},
                    "initial_option": client if client != "all" else "all",
                    "value": {"action": "client_filter", "status": status},
                    "options": [
                        {"text": {"tag": "plain_text", "content": "All clients"},
                         "value": "all"}
                    ] + [
                        {"text": {"tag": "plain_text", "content": name},
                         "value": slugify(name)}
                        for name in all_sections
                    ],
                }
            ],
        },
    ]

    # Manual override: mark something delivered when the carrier never will.
    # Scoped to what's on screen, so filtering to a client shortens the list.
    if filtered:
        elements.append({
            "tag": "action",
            "actions": [{
                "tag": "select_static",
                "placeholder": {"tag": "plain_text",
                                "content": "✓ Mark as delivered"},
                "value": {"action": "mark_delivered", "status": status,
                          "client": client},
                "options": [
                    {"text": {"tag": "plain_text",
                              "content": _delivered_label(r)},
                     "value": mark_delivered_value(r)}
                    for r in filtered[:MAX_DELIVER_OPTIONS]
                ],
            }],
        })
    else:
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md",
                     "content": "_Nothing matches that filter right now._"},
        })

    for name in list(grouped)[:MAX_CLIENTS]:
        section_rows = grouped[name]
        shown = section_rows[:MAX_LINES_PER_CLIENT]
        body = "\n".join(shipment_line(r, section=name) for r in shown)
        overflow = len(section_rows) - len(shown)
        if overflow > 0:
            body += "\n_+%d more in %s_" % (overflow, name)
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": "**%s**" % name},
        })
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": body}})

    hidden = max(0, len(grouped) - MAX_CLIENTS)
    note_bits = ["Updated %s ET" % _now_et().strftime("%b %-d, %-I:%M %p")]
    if sheet_count:
        note_bits.append("%d sheets" % sheet_count)
    note_bits.append("%d open shipments" % len(listable))
    if hidden:
        note_bits.append("%d more sections hidden -- use the filter" % hidden)

    elements.append({"tag": "hr"})
    elements.append({
        "tag": "note",
        "elements": [{"tag": "plain_text",
                      "content": (" %s " % SEP).join(note_bits)}],
    })

    header_title = "\U0001F4E6 HLT Shipment Tracker"
    if client != "all":
        label = next((c for c in all_sections if slugify(c) == client), None)
        if label:
            header_title += " %s %s" % (SEP, label)

    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "red" if totals[FLAGGED] else "blue",
            "title": {"tag": "plain_text", "content": header_title},
        },
        "elements": elements,
    }


def plain_text_fallback(results):
    """Text version used when the card API rejects the payload."""
    rows = [r for r in dedupe(results or []) if bucket_for(r) != DELIVERED]
    if not rows:
        return "All shipments delivered. Nothing to track."
    totals = counts(rows)
    lines = ["HLT Shipment Tracker",
             "%d in transit, %d not scanned, %d flagged"
             % (totals[TRANSIT] + totals[ARRIVING], totals[UNSCANNED],
                totals[FLAGGED])]
    for name, section_rows in group_by_section(rows).items():
        lines.append("")
        lines.append(name)
        for r in section_rows[:MAX_LINES_PER_CLIENT]:
            b = bucket_for(r)
            who = line_name(r, name)
            lines.append("  %s %s%s %s %s" % (
                BUCKET_DOT[b], _ref(r), (" " + SEP + " " + who) if who else "",
                SEP, _detail(r, b)))
    return "\n".join(lines)


# ===========================================================================
# Card JSON 2.0 -- the dashboard layout
# ===========================================================================
# Card 1.0 can only stack markdown lines, which is why the first version read
# like a list. Card 2.0 gives us a real table component (typed columns, tag
# pills, paging) plus filled tab buttons -- the same shape as the Fulfillment
# screen in the client portal:
#
#   Shipment Tracker                                    [8 flagged]
#   24 open  ·  14 in transit  ·  2 not scanned  ·  8 flagged
#   [ All 24 ][ Hannah 13 ][ Lucy 1 ][ Other 5 ][ 7Brew Coffee 2 ] ...
#   ┌────────────┬──────────────┬────────────┬───────────┬────────┬───────┐
#   │ SHIPMENT   │ CLIENT       │ STATUS     │ DETAIL    │CARRIER │ ETA   │
#   ├────────────┼──────────────┼────────────┼───────────┼────────┼───────┤
#   │ HLT-SO6638 │ Craftworks   │ ● Flagged  │ Forwarded │ UPS    │ Today │
#   └────────────┴──────────────┴────────────┴───────────┴────────┴───────┘
#   [ All statuses v ]            [ ✓ Mark as delivered v ]
#
# Tabs are the books of business (All / Hannah / Lucy / Other / each client
# sheet), exactly the filter order asked for.

TAG_COLOR = {
    FLAGGED: "red",
    ARRIVING: "green",
    TRANSIT: "blue",
    UNSCANNED: "grey",
    DELIVERED: "green",
}

TAG_TEXT = {
    FLAGGED: "Flagged",
    ARRIVING: "Out for delivery",
    TRANSIT: "In transit",
    UNSCANNED: "Not scanned",
    DELIVERED: "Delivered",
}

STATUS_MENU = [
    ("all", "All statuses"),
    (FLAGGED, "Flagged only"),
    (TRANSIT, "In transit"),
    (UNSCANNED, "Not scanned"),
]

MAX_TABS = 14
MAX_DELIVER_OPTIONS = 20

# Filter order as asked for: All, Not scanned, In transit, Flagged
STATUS_TABS_V2 = [
    ("all", "All"),
    (UNSCANNED, "Ready to ship"),
    (TRANSIT, "In transit"),
    (FLAGGED, "On hold"),
]


def _kpi_tile(count, label, bucket, active, target):
    """One clickable stat tile.

    The whole tile is the control, not a button inside it -- a number you can
    read across the room and press with a thumb, instead of four code spans
    that looked like tabs and did nothing.
    """
    colour = TAG_COLOR.get(bucket, "grey")
    number = "**%d**" % count if count else "<font color='grey'>0</font>"
    if count and bucket == FLAGGED:
        number = "<font color='red'>**%d**</font>" % count
    return {
        "tag": "interactive_container",
        "width": "fill",
        "padding": "8px 10px 8px 10px",
        "corner_radius": "8px",
        "has_border": True,
        "border_color": colour if active else "grey",
        "background_style": "grey" if active else "default",
        "behaviors": [{"type": "callback", "value": target}],
        "elements": [
            {"tag": "markdown", "text_size": "heading", "content": number},
            {"tag": "markdown",
             "content": "<font color='grey'>%s %s</font>"
                        % (BUCKET_DOT[bucket], label)},
        ],
    }


def _tab_button(label, count, value, active):
    return {
        "tag": "button",
        "size": "small",
        "type": "primary_filled" if active else "text",
        "text": {"tag": "plain_text", "content": "%s  %d" % (label, count)},
        "behaviors": [{"type": "callback", "value": value}],
    }


def _table_row(r, scope=""):
    """One shipment as a table row.

    A table gives every shipment the same columns in the same places, so the
    eye runs down "how late" instead of re-parsing a sentence per line.
    """
    bucket = bucket_for(r)
    tracking = (r.get("tracking_num") or "").strip()
    carrier = (r.get("carrier") or "").strip().upper()
    url = _tracking_url(tracking, carrier)
    ref = _ref(r)
    late = days_overdue(r)

    # One column for "when", because a shipment is either late by so many days
    # or due on a date -- never both. Six columns did not fit the card and the
    # ETA was being cut off the right edge.
    eta = _short_date(r.get("delivery_date"))
    when = "%dd late" % late if late else (eta or "—")

    return {
        "shipment": "[%s](%s)" % (ref, url) if url else (ref or "—"),
        "client": (line_name(r, scope) or ("" if scope else section_for(r))
                   or "—"),
        "status": [{"text": TAG_TEXT[bucket], "color": TAG_COLOR[bucket]}],
        "when": when,
        "detail": _detail(r, bucket, terse=bool(late)) or "—",
    }


TABLE_COLUMNS = [
    {"name": "shipment", "display_name": "SHIPMENT", "data_type": "lark_md",
     "width": "auto"},
    {"name": "client", "display_name": "CLIENT", "data_type": "text",
     "width": "auto"},
    {"name": "status", "display_name": "STATUS", "data_type": "options",
     "width": "108px"},
    {"name": "when", "display_name": "WHEN", "data_type": "text",
     "width": "86px"},
    {"name": "detail", "display_name": "LAST SCAN", "data_type": "text",
     "width": "auto"},
]


def _columns_for(rows):
    """The columns worth spending width on for this particular list.

    A card is about 500px wide. When every row in a section carries the same
    status -- which is the whole point of a section called "Needs Attention"
    -- the status pill is 110px repeating one word, and it pushes the
    carrier's own words off the right edge. So it earns its place only on a
    mixed list.
    """
    mixed = len({bucket_for(r) for r in rows}) > 1
    return [c for c in TABLE_COLUMNS if mixed or c["name"] != "status"]


def _table(rows, scope="", limit=6):
    """A shipment table, or nothing at all when there is nothing to show."""
    if not rows:
        return []
    shown = rows[:limit]
    return [{
        "tag": "table",
        "page_size": max(1, len(shown)),
        "row_height": "low",
        "header_style": {"text_size": "small", "text_color": "grey",
                         "bold": True, "lines": 1},
        "freeze_first_column": True,
        "columns": _columns_for(shown),
        "rows": [_table_row(r, scope) for r in shown],
    }]


def _action_row(r, scope=""):
    """One shipment you can act on without leaving the chat.

    A table row can only be read. This carries the two things anyone actually
    does with a stuck parcel -- look at the carrier's page, or close it out
    because it arrived and the carrier never said so.
    """
    bucket = bucket_for(r)
    tracking = (r.get("tracking_num") or "").strip()
    carrier = (r.get("carrier") or "").strip().upper()
    url = _tracking_url(tracking, carrier)
    late = days_overdue(r)
    who = line_name(r, scope) or ("" if scope else section_for(r))

    # Headline: the reference someone quotes, and who it is for.
    head = "**%s**" % (_ref(r) or tracking or "No tracking #")
    if who:
        head += " %s %s" % (SEP, who)

    # How late, in red, at the end of the same line -- the number the eye is
    # hunting for on a card full of problems.
    if late:
        head += ("  <font color='red'>**%dd late**</font>" % late)
    else:
        eta = _short_date(r.get("delivery_date"))
        if eta:
            head += "  <font color='grey'>%s</font>" % eta

    meta = [_detail(r, bucket, terse=bool(late))]
    if carrier:
        meta.append(CARRIER_DISPLAY.get(
            carrier, carrier if len(carrier) <= 5 else carrier.title()))
    if tracking and _ref(r) != tracking:
        meta.append(tracking)
    boxes = str(r.get("num_boxes") or "").strip()
    packages = r.get("packages") or []
    n_boxes = len(packages) if len(packages) > 1 else (
        int(boxes) if boxes.isdigit() and int(boxes) > 1 else 0)
    if n_boxes:
        meta.append("%d boxes" % n_boxes)

    buttons = []
    if url:
        buttons.append({
            "tag": "button", "size": "tiny", "type": "default",
            "text": {"tag": "plain_text", "content": "Track"},
            "behaviors": [{"type": "open_url", "default_url": url}],
        })
    buttons.append({
        "tag": "button", "size": "tiny", "type": "default",
        "text": {"tag": "plain_text", "content": "Mark delivered"},
        "behaviors": [{"type": "callback",
                       "value": {"action": "mark_delivered",
                                 "handle": mark_delivered_value(r)}}],
    })

    return {
        "tag": "interactive_container",
        "width": "fill",
        "padding": "9px 11px 9px 11px",
        "corner_radius": "8px",
        "has_border": True,
        "border_color": TAG_COLOR.get(bucket, "grey"),
        "behaviors": [{"type": "open_url", "default_url": url}] if url else [],
        "elements": [
            {"tag": "markdown", "content": head},
            {"tag": "markdown",
             "content": "<font color='grey'>%s</font>"
                        % (" %s " % SEP).join(m for m in meta if m)},
            {"tag": "column_set", "flex_mode": "flow",
             "horizontal_spacing": "6px", "margin": "7px 0px 0px 0px",
             "columns": [{"tag": "column", "width": "auto",
                          "elements": [b]} for b in buttons]},
        ],
    }


def _person_block(name, rows, top, scope_note=""):
    """One person's pile: a heading, their worst few, the rest folded away."""
    if not rows:
        return []
    late = sum(1 for r in rows if days_overdue(r))
    bits = ["%d open" % len(rows)]
    if late:
        bits.append("%d late" % late)
    out = [
        {"tag": "markdown", "margin": "12px 0px 0px 0px",
         "content": "**%s**  <font color='grey'>%s</font>"
                    % (name, (" %s " % SEP).join(bits))},
    ]
    out += [_action_row(r, scope=name) for r in rows[:top]]
    rest = rows[top:]
    if rest:
        out += _overflow_panel(rest, "%d more for %s" % (len(rest), name),
                               scope=name)
    return out


def _overflow_panel(rows, title, scope=""):
    """The rest of the list, folded away until someone wants it."""
    if not rows:
        return []
    return [{
        "tag": "collapsible_panel",
        "expanded": False,
        "background_style": "default",
        "header": {
            "title": {"tag": "markdown",
                      "content": "<font color='grey'>%s</font>" % title},
            "vertical_align": "center",
            "icon_position": "right",
        },
        "elements": _table(rows, scope, limit=len(rows)),
    }]


def _arriving_today(r):
    """Out for delivery, or due to land today."""
    if bucket_for(r) == ARRIVING:
        return True
    eta = _parse_date(r.get("delivery_date"))
    return bool(eta) and eta == _now_et().date() and bucket_for(r) == TRANSIT


SUMMARY_ROWS = 2      # per person on the overview -- a nudge
TAB_ROWS = 4          # per person once a tile is pressed
CLIENT_ROWS = 6       # when one client is selected
DETAIL_ROWS = 16      # when a status filter is on


def _section(title, rows, limit, subtitle="", scope="", force_bucket=None):
    """A heading, up to `limit` shipment lines, and a '+N more' when cut.

    `scope` is the client the section is already titled with, so their name
    isn't repeated on every line. `force_bucket` colours the dot for a
    section whose meaning overrides the row's own bucket -- everything under
    "Arriving Today" is orange, even the rows the carrier still calls in
    transit.
    """
    out = [{"tag": "hr", "margin": "6px 0px"},
           {"tag": "markdown", "content": "**%s**" % title}]
    if subtitle:
        out.append({"tag": "markdown",
                    "content": "<font color='grey'>%s</font>" % subtitle})
    if not rows:
        out.append({"tag": "markdown",
                    "content": "<font color='grey'>Nothing here.</font>"})
        return out
    shown = rows[:limit]
    body = "\n".join(shipment_line(r, bucket=force_bucket, section=scope)
                     for r in shown)
    if len(rows) > limit:
        body += ("\n<font color='grey'>+%d more</font>"
                 % (len(rows) - limit))
    out.append({"tag": "markdown", "content": body})
    return out


def build_tracker_card_v2(results, client="all", status="all", sheet_count=None):
    """The chat card: a status board, then only what needs a person.

    The overview shows what's stuck and what lands today -- five or six lines
    that are worth reading in a chat. Everything else (all 30+ rows, search,
    sorting, history) is the dashboard's job, one button away. Picking a
    client re-renders the same card scoped to them.
    """
    rows = dedupe(results or [])
    totals = counts(rows)
    listable = [r for r in rows if bucket_for(r) != DELIVERED]
    sections = group_by_section(listable)

    today = [r for r in listable if _arriving_today(r)]
    flagged = [r for r in listable if bucket_for(r) == FLAGGED]
    n_today = len(today)
    n_transit = totals[TRANSIT] + totals[ARRIVING] - n_today

    def _order(rs):
        """Worst first.

        Alphabetical order by client put a three-day-old problem above a
        fourteen-day-old one, and the five lines that fit on the card are the
        only ones anybody reads. Days late now decides, and the client name is
        just the tie-breaker.
        """
        return sorted(rs, key=lambda r: (BUCKET_ORDER[bucket_for(r)],
                                         -days_overdue(r),
                                         section_for(r), _ref(r)))

    # --- stat tiles ---------------------------------------------------------
    #
    # The count IS the control. Press a tile to filter the card to it; press
    # the one you are already on to come back.
    tiles = [
        ("Attention", totals[FLAGGED], FLAGGED),
        ("Today", n_today, ARRIVING),
        ("Transit", max(0, n_transit), TRANSIT),
        ("Unscanned", totals[UNSCANNED], UNSCANNED),
    ]
    elements = [{
        "tag": "column_set", "flex_mode": "bisect", "horizontal_spacing": "6px",
        "columns": [
            {"tag": "column", "width": "weighted", "weight": 1,
             "elements": [_kpi_tile(
                 count, label, key, status == key,
                 {"action": "status_filter", "client": client,
                  "status": "all" if status == key else key})]}
            for label, count, key in tiles
        ],
    }]

    # --- body: everyone's pile, worst first ---------------------------------
    #
    # Grouped by person, because that is how the work is actually divided --
    # Hannah does not need to read past Lucy's parcels to find her own. Each
    # person shows only their worst few; the rest folds away. The card is a
    # nudge, the dashboard is the record.

    def by_person(rows, top):
        """Sections in their usual order, each already worst-first."""
        out, grouped = [], group_by_section(rows)
        for name in list(grouped)[:MAX_CLIENTS]:
            out += _person_block(name, _order(grouped[name]), top)
        return out

    if client and client != "all":
        name = next((n for n in sections if slugify(n) == client), client)
        mine = _order(sections.get(name, []))
        elements += _person_block(name, mine, CLIENT_ROWS)
        if not mine:
            elements.append({"tag": "markdown", "margin": "12px 0px 0px 0px",
                             "content": "<font color='grey'>Nothing open for "
                                        "%s.</font>" % name})
    elif status in (FLAGGED, ARRIVING, TRANSIT, UNSCANNED):
        title, rows = {
            FLAGGED: ("Needs attention", flagged),
            ARRIVING: ("Arriving today", today),
            TRANSIT: ("In transit",
                      [r for r in listable
                       if bucket_for(r) in (TRANSIT, ARRIVING)
                       and not _arriving_today(r)]),
            UNSCANNED: ("Not yet scanned",
                        [r for r in listable if bucket_for(r) == UNSCANNED]),
        }[status]
        elements.append({"tag": "markdown", "margin": "10px 0px 0px 0px",
                         "content": "**%s** <font color='grey'>%s %d "
                                    "shipment%s</font>"
                                    % (title, SEP, len(rows),
                                       "" if len(rows) == 1 else "s")})
        if rows:
            elements += by_person(rows, TAB_ROWS)
        else:
            elements.append({"tag": "markdown",
                             "content": "<font color='grey'>Nothing here "
                                        "right now.</font>"})
    else:
        urgent = flagged + [r for r in today if r not in flagged]
        if urgent:
            elements += by_person(urgent, SUMMARY_ROWS)
            quiet = len(listable) - len(urgent)
            if quiet > 0:
                elements.append({
                    "tag": "markdown", "margin": "12px 0px 0px 0px",
                    "content": "<font color='grey'>%d more moving normally "
                               "%s nothing needed</font>" % (quiet, SEP)})
        else:
            elements.append({
                "tag": "markdown", "margin": "12px 0px 0px 0px",
                "content": "<font color='green'>**All %d shipments moving.** "
                           "Nothing needs attention and nothing lands "
                           "today.</font>" % len(listable)})

    # --- actions -----------------------------------------------------------
    buttons = []
    try:
        import dashboard
        deep_link = dashboard.lark_link()
    except Exception:
        deep_link = ""
    if deep_link:
        buttons.append({
            "tag": "button", "size": "small", "type": "primary_filled",
            "text": {"tag": "plain_text", "content": "Open Full Dashboard"},
            "behaviors": [{"type": "open_url", "default_url": deep_link}],
        })
    # "View All Attention" used to live here. The Attention tab above now does
    # exactly that, and two controls for one action is how a card gets noisy.
    if status != "all" or client != "all":
        buttons.append({
            "tag": "button", "size": "small", "type": "default",
            "text": {"tag": "plain_text", "content": "Back to summary"},
            "behaviors": [{"type": "callback",
                           "value": {"action": "status_filter",
                                     "status": "all", "client": "all"}}],
        })
    if buttons:
        elements.append({"tag": "hr", "margin": "8px 0px 6px 0px"})
        elements.append({
            "tag": "column_set", "flex_mode": "flow",
            "horizontal_spacing": "8px",
            "columns": [{"tag": "column", "width": "auto",
                         "vertical_align": "center", "elements": [b]}
                        for b in buttons],
        })

    # --- client picker + mark delivered, side by side ----------------------
    #
    # Two selects in one column_set, because they are one thought: narrow to a
    # client, then close out one of their shipments. Stacked, they cost a row
    # of card height each; separate action blocks is what stacks them.
    pickers = [{
        "tag": "select_static",
        "initial_option": client,
        "placeholder": {"tag": "plain_text", "content": "Client: All Clients"},
        "behaviors": [{"type": "callback",
                       "value": {"action": "client_filter", "status": status}}],
        "options": [{"text": {"tag": "plain_text", "content": "All Clients"},
                     "value": "all"}] + [
            {"text": {"tag": "plain_text",
                      "content": "%s (%d)" % (n, len(sections[n]))},
             "value": slugify(n)}
            for n in list(sections)[:MAX_TABS]
        ],
    }]

    # The bulk mark-delivered dropdown used to live here. Every shipment now
    # carries its own button, so a second control listing the same shipments
    # is one more thing to read and one more way to pick the wrong row.

    elements.append({
        "tag": "column_set", "flex_mode": "flow", "horizontal_spacing": "8px",
        "margin": "6px 0px 0px 0px",
        "columns": [{"tag": "column", "width": "weighted", "weight": 1,
                     "vertical_align": "center", "elements": [p]}
                    for p in pickers],
    })

    # How many spreadsheets were read is our plumbing, not their news.
    note_bits = []
    if totals[DELIVERED]:
        note_bits.append("%d delivered today" % totals[DELIVERED])
    if note_bits:
        elements.append({"tag": "markdown", "margin": "6px 0px 0px 0px",
                         "content": "<font color='grey'>%s</font>"
                                    % (" %s " % SEP).join(note_bits)})

    # The header carries the state of the board: the count, the freshness, and
    # a tag that says whether anything is wrong. Those three were taking a
    # body line each.
    tags = []
    if totals[FLAGGED]:
        tags.append({"tag": "text_tag", "color": "red",
                     "text": {"tag": "plain_text",
                              "content": "%d need attention"
                                         % totals[FLAGGED]}})
    elif listable:
        tags.append({"tag": "text_tag", "color": "green",
                     "text": {"tag": "plain_text", "content": "All moving"}})

    return {
        "schema": "2.0",
        "config": {"update_multi": True, "width_mode": "fill"},
        "header": {
            "title": {"tag": "plain_text",
                      "content": "\U0001F4E6 Shipment Tracker"},
            "subtitle": {"tag": "plain_text",
                         "content": "%d open %s updated %s"
                                    % (len(listable), SEP,
                                       _now_et().strftime("%-I:%M %p"))},
            "text_tag_list": tags,
            "template": "red" if totals[FLAGGED] else "blue",
        },
        "body": {"direction": "vertical", "padding": "12px 12px 12px 12px",
                 "elements": elements},
    }


# =========================================================================
# Dashboard payload
# =========================================================================
# The web dashboard and the /api/shipments endpoint both consume this shape.
# Keeping it here means the dashboard classifies shipments exactly the way
# the chat card and the urgent alerts do -- one definition of "flagged".

def _status_text(r, bucket):
    return _detail(r, bucket)


def _int(v):
    v = str(v or "").strip().replace(",", "")
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return 0


def shipment_payload(r, client_map=None):
    """One shipment as the dashboard consumes it."""
    bucket = bucket_for(r)
    tracking = (r.get("tracking_num") or "").strip()
    carrier_raw = (r.get("carrier") or "").strip().upper()
    carrier = CARRIER_DISPLAY.get(
        carrier_raw, carrier_raw if len(carrier_raw) <= 5 else carrier_raw.title())

    boxes = str(r.get("num_boxes") or "").strip()
    packages = r.get("packages") or []
    n_boxes = len(packages) if len(packages) > 1 else (
        int(boxes) if boxes.isdigit() else 0)

    eta_date = _parse_date(r.get("delivery_date"))
    overdue = days_overdue(r)

    return {
        "id": _ref(r),
        "tracking": tracking,
        "track_url": _tracking_url(tracking, carrier_raw),
        "carrier": carrier or "—",
        "client": ((client_map or {}).get(line_name(r, ""), line_name(r, ""))
                   or "Unassigned"),
        "owner": section_for(r),
        "status": bucket,
        "status_label": TAG_TEXT[bucket],
        "detail": _status_text(r, bucket),
        "boxes": n_boxes,
        "eta": eta_date.isoformat() if eta_date else "",
        "eta_label": _short_date(r.get("delivery_date")) or "Unknown",
        "overdue_days": overdue if bucket == FLAGGED else 0,
        "location": (r.get("location") or "").strip(),
        "tab": r.get("tab") or "",
        "sheet_token": r.get("sheet_token") or "",
        "row_num": r.get("row_num"),
        "handle": mark_delivered_value(r),
        # Packing list: what's in the box, and whether it's the whole order.
        "product": (r.get("product_name") or "").strip(),
        "photo": (r.get("product_photo") or "").strip(),
        "qty_shipped": _int(r.get("qty_shipped")),
        "qty_ordered": _int(r.get("qty_expected")),
        "short": max(0, _int(r.get("qty_expected")) - _int(r.get("qty_shipped"))),
        "boxes": _int(r.get("num_boxes")),
    }


def dashboard_payload(results, sheet_count=None):
    """Everything the dashboard needs in one JSON-serializable dict."""
    rows = dedupe(results or [])
    totals = counts(rows)
    open_rows = [r for r in rows if bucket_for(r) != DELIVERED]

    # Merge spellings so the client filter has one "7Brew Coffee", not three.
    client_map = build_client_map(open_rows)
    shipments = [shipment_payload(r, client_map) for r in open_rows]

    groups = group_by_section(open_rows)
    clients = sorted({s["client"] for s in shipments if s["client"] != "Unassigned"})

    return {
        "updated": _now_et().isoformat(),
        "updated_label": _now_et().strftime("%b %-d, %Y") + " " + SEP + " "
                         + _now_et().strftime("%-I:%M %p"),
        "sheet_count": sheet_count or len({s["sheet_token"] for s in shipments}),
        "totals": {
            "flagged": totals[FLAGGED],
            "arriving": totals[ARRIVING],
            "transit": totals[TRANSIT],
            "unscanned": totals[UNSCANNED],
            "delivered": totals[DELIVERED],
            "open": len(open_rows),
        },
        "owners": list(groups.keys()),
        "clients": clients,
        "carriers": sorted({s["carrier"] for s in shipments if s["carrier"] != "—"}),
        "shipments": shipments,
    }
