"""
Configuration for Lark Tracking Bot
All settings are loaded from environment variables (GitHub Secrets)
"""
import os

# =============================================================================
# LARK APP CREDENTIALS (from Lark Developer Console)
# =============================================================================
LARK_APP_ID = os.environ.get("LARK_APP_ID", "")
LARK_APP_SECRET = os.environ.get("LARK_APP_SECRET", "")

# JP-region endpoint for Lark Suite
LARK_BASE_URL = os.environ.get("LARK_BASE_URL", "https://open.larksuite.com")

# =============================================================================
# LARK GROUP CHAT for notifications
# =============================================================================
LARK_CHAT_ID = os.environ.get("LARK_CHAT_ID", "")

# =============================================================================
# LARK SHEETS TO SCAN
# =============================================================================
# Comma-separated list of sheet tokens
# Extract from URL: https://xxx.jp.larksuite.com/sheets/<SHEET_TOKEN>
SHEET_TOKENS = [
    t.strip()
    for t in os.environ.get("LARK_SHEET_TOKENS", "").split(",")
    if t.strip()
]

# =============================================================================
# SHEET OWNERS -- maps each sheet token to a display section (Hannah/Lucy/Other)
# =============================================================================
# Format: "token1:Hannah,token2:Lucy" (any token not listed falls to "Other")
# Set via GitHub Secret: LARK_SHEET_OWNERS
SHEET_OWNERS = {}
for _entry in os.environ.get("LARK_SHEET_OWNERS", "").split(","):
    _entry = _entry.strip()
    if ":" in _entry:
        _tok, _owner = _entry.split(":", 1)
        SHEET_OWNERS[_tok.strip()] = _owner.strip()

# =============================================================================
# COLUMN MAPPING (letters A-Q)
# =============================================================================
COLUMNS = {
    "shipment_id": "A",
    "vendor": "B",
    "recipient": "C",
    "order_num": "D",
    "customer": "E",
    "product_photo": "F",
    "tracking_num": "H",
    "carrier": "I",
    "qty_shipped": "J",
    "qty_expected": "K",
    "discrepancy": "L",
    "balance_owed": "M",
    "status": "N",
    "tariff_charge": "O",
    "num_boxes": "P",
    "notes": "Q",
    "delivery_date": "R",
}

# Header row (1-indexed) -- data starts on the row after this
HEADER_ROW = 2

# =============================================================================
# CARRIER API CREDENTIALS
# =============================================================================
# FedEx -- https://developer.fedex.com
FEDEX_API_KEY = os.environ.get("FEDEX_API_KEY", "")
FEDEX_SECRET_KEY = os.environ.get("FEDEX_SECRET_KEY", "")

# UPS -- https://developer.ups.com
UPS_CLIENT_ID = os.environ.get("UPS_CLIENT_ID", "")
UPS_CLIENT_SECRET = os.environ.get("UPS_CLIENT_SECRET", "")

# DHL -- https://developer.dhl.com (free tier)
DHL_API_KEY = os.environ.get("DHL_API_KEY", "")
# 17Track universal aggregator -- DPD, UniUni, 1ST, 4PX (no native API)
SEVENTEENTRACK_API_KEY = os.environ.get("SEVENTEENTRACK_API_KEY", "")

# =============================================================================
# BOT SETTINGS
# =============================================================================
# Sheet tabs to skip
SKIP_TABS = {"TEMPLATE"}

# Carrier name normalization -- maps values in sheet column H to API client keys
CARRIER_ALIASES = {
    # FedEx
    "fedex": "fedex",
    "fed ex": "fedex",
    "federal express": "fedex",
    # UPS
    "ups": "ups",
    "united parcel": "ups",
    # USPS
    "usps": "usps",
    "us postal": "usps",
    "united states postal": "usps",
    # DHL
    "dhl": "dhl",
    "dhl express": "dhl",
    # Royal Mail
    "royal mail": "royalmail",
    "royalmail": "royalmail",
    "royal": "royalmail",
    "rm": "royalmail",
    # SF Express (Shun Feng)
    "sf express": "sfexpress",
    "sf-express": "sfexpress",
    "sfexpress": "sfexpress",
    "sf": "sfexpress",
    "shunfeng": "sfexpress",
    "shun feng": "sfexpress",
    "sf international": "sfexpress",
    # DPD / UniUni / 1ST / 4PX -- resolved via 17Track aggregator
    "dpd": "dpd",
    "uniuni": "uniuni",
    "uni uni": "uniuni",
    "1st": "first",
    "first": "first",
    "1st mile": "first",
    "4px": "fourpx",
    "fourpx": "fourpx",
}

# Status values the bot writes to the sheet (column M)
# MUST match the dropdown options in the Lark Sheet exactly (title-case).
# Writing values that don't match will overwrite the dropdown with plain text
# and break the color-coded formatting.
STATUS_MAP = {
    "delivered": "Delivered",
    "in_transit": "In Transit",
    "out_for_delivery": "In Transit",
    "exception": "Exception/Delay",
    "pending": "Label Created/Not Scanned",
    "label_created": "Label Created/Not Scanned",
    "unknown": "Label Created/Not Scanned",
    "not_found": "Label Created/Not Scanned",
}

# =============================================================================
# PER-SPREADSHEET COLUMN OVERRIDES
# =============================================================================
# Some spreadsheets use a different column layout than the default COLUMNS map.
# Map a sheet token to an override dict here. Any field not listed falls back
# to the default COLUMNS value. col_to_index() converts a letter to a 0-based
# index; an empty string means the column does not exist on that sheet.
#
# "2026 OTHER INBOUND SHIPMENTS" (token LxYSsmrrehIeRttb0UtjhtvBp7b) is shifted
# one column LEFT vs the main sheets: Tracking#=G, Carrier=H, Status=M.
SHEET_COLUMN_OVERRIDES = {
    "LxYSsmrrehIeRttb0UtjhtvBp7b": {
        "shipment_id": "A",
        "vendor": "B",
        "recipient": "C",
        "order_num": "D",
        "customer": "E",
        "product_photo": "F",
        "tracking_num": "G",
        "carrier": "H",
        "qty_shipped": "I",
        "qty_expected": "J",
        "discrepancy": "K",
        "balance_owed": "L",
        "status": "M",
        "num_boxes": "",
        "notes": "",
        "delivery_date": "",
    },
}

def columns_for(spreadsheet_token):
    """Return the column-letter map for a spreadsheet, merging any override."""
    merged = dict(COLUMNS)
    override = SHEET_COLUMN_OVERRIDES.get((spreadsheet_token or "").strip())
    if override:
        merged.update(override)
    return merged

def col_to_index(col_letter):
    """Convert a column letter (A, B, ... AA) to a 0-based index, or None."""
    if not col_letter:
        return None
    col_letter = col_letter.strip().upper()
    idx = 0
    for ch in col_letter:
        if not ("A" <= ch <= "Z"):
            return None
        idx = idx * 26 + (ord(ch) - ord("A") + 1)
    return idx - 1


# =============================================================================
# CLIENT SHEET LAYOUT ("2026 HANNAH <CLIENT> SHIPPING LIST" sheets)
# =============================================================================
# The per-client sheets in the HANNAH BULK SHIPPING LIST folder share one
# layout that differs from the main inbound sheets: the header is on row 1
# and there are no Shipment ID / Vendor / Recipient / Balance Owed / Tariff /
# Notes columns. Column D (Product Names) is not read by the bot.
CLIENT_SHEET_COLUMNS = {
    "shipment_id": "",
    "vendor": "",
    "recipient": "",
    "order_num": "A",
    "customer": "B",
    "product_photo": "C",
    "tracking_num": "E",
    "carrier": "F",
    "qty_shipped": "G",
    "qty_expected": "H",
    "discrepancy": "I",
    "balance_owed": "",
    "status": "J",
    "tariff_charge": "",
    "num_boxes": "K",
    "notes": "",
    "delivery_date": "L",
}

CLIENT_SHEET_TOKENS = [
    "KiHwsMaxnh5Qglt80L5jnB8Dpk6",  # 2026 HANNAH 7BREW COFFEE SHIPPING LIST
    "T0VlsL1AzhhUHJtjGshj3KOApDc",  # 2026 HANNAH CRAFTWORKS SHIPPING LIST
    "CNlmswxmjhwm7ttisngjbvM1pvd",  # 2026 HANNAH DENIM TEARS SHIPPING LIST
    "PG1hsm5ZihC0b9tWUgUjNulIp81",  # 2026 HANNAH LIQUID DEATH SHIPPING LIST
    "EkGxsuNkchw6hWtjFkxjcInBpaf",  # 2026 HANNAH LIVE FAST DIE YOUNG SHIPPING LIST
    "UbLasIUomhj3d4tsEwMjoOOupqd",  # 2026 HANNAH PALM TREE CREW SHIPPING LIST
    "J5EvspVVPhhvWdtFJz0jCAwRpJh",  # 2026 HANNAH STEADY HANDS SHIPPING LIST
    "Tk4vscdE3hVVO8txOUJjrUFppGc",  # 2026 HANNAH VEES SHIPPING LIST
    "TGXiss8HGhyJNBtdiLbjOr9appc",  # 2026 HANNAH WILDFANG SHIPPING LIST
]

for _t in CLIENT_SHEET_TOKENS:
    SHEET_COLUMN_OVERRIDES[_t] = dict(CLIENT_SHEET_COLUMNS)

# Per-spreadsheet header row overrides -- the client sheets have their header
# on row 1 (data starts on row 2) instead of the default row 2.
SHEET_HEADER_ROW_OVERRIDES = {_t: 1 for _t in CLIENT_SHEET_TOKENS}


def header_row_for(spreadsheet_token):
    """Return the header row (1-indexed) for a spreadsheet."""
    return SHEET_HEADER_ROW_OVERRIDES.get((spreadsheet_token or "").strip(), HEADER_ROW)


# =============================================================================
# FOLDER AUTO-DISCOVERY
# =============================================================================
# Comma-separated Lark Drive folder tokens (GitHub Secret: LARK_FOLDER_TOKENS).
# Every run the bot lists these folders and scans any spreadsheet it finds,
# in addition to LARK_SHEET_TOKENS. Subfolders are followed only when their
# name contains "SHIP" (e.g. "HANNAH BULK SHIPPING LIST") so quote/invoice
# folders are left alone. Newly discovered sheets are assumed to use the
# client sheet template layout above.
FOLDER_TOKENS = [
    t.strip()
    for t in os.environ.get("LARK_FOLDER_TOKENS", "").split(",")
    if t.strip()
]


def register_client_sheet(token):
    """Apply the client-sheet template layout to a discovered spreadsheet."""
    token = (token or "").strip()
    if not token:
        return
    if token not in SHEET_COLUMN_OVERRIDES:
        SHEET_COLUMN_OVERRIDES[token] = dict(CLIENT_SHEET_COLUMNS)
    if token not in SHEET_HEADER_ROW_OVERRIDES:
        SHEET_HEADER_ROW_OVERRIDES[token] = 1
