"""
Lark API Client

Per-spreadsheet column layout: read_tracking_data(), read_all_status_rows(),
update_tracking_row() and the status styling helpers resolve column positions
via config.columns_for(token) so spreadsheets with a shifted layout (e.g. the
Other Inbound sheet) are read and written using the correct columns.

Shipment groups span multiple rows; Shipment ID / Tracking # / Carrier / Num
Boxes are filled only on the first row of a group and carried forward.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime
import time
import requests

from config import (
    LARK_APP_ID,
    LARK_APP_SECRET,
    LARK_BASE_URL,
    LARK_CHAT_ID,
    COLUMNS,
    HEADER_ROW,
    SKIP_TABS,
    SHEET_OWNERS,
    columns_for,
    col_to_index, header_row_for,
)

logger = logging.getLogger(__name__)

PERMANENT_TABS = ["Hannah", "Lucy", "Other"]

SECTION_DISPLAY = {
    "Hannah": "HANNAH",
    "Lucy": "LUCY",
    "Other": "OTHER",
}


class LarkClient:
    def __init__(self):
        self.base_url = LARK_BASE_URL.rstrip("/")
        self.token = None
        self.token_expires = 0

    def _get_tenant_token(self):
        if self.token and time.time() < self.token_expires:
            return self.token
        url = f"{self.base_url}/open-apis/auth/v3/tenant_access_token/internal"
        resp = requests.post(url, json={
            "app_id": LARK_APP_ID,
            "app_secret": LARK_APP_SECRET,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Lark auth failed: {data}")
        self.token = data["tenant_access_token"]
        self.token_expires = time.time() + data.get("expire", 7200) - 300
        logger.info("Lark tenant token acquired")
        return self.token

    def _headers(self):
        return {
            "Authorization": f"Bearer {self._get_tenant_token()}",
            "Content-Type": "application/json",
        }

    def get_sheet_metadata(self, spreadsheet_token):
        url_v3 = f"{self.base_url}/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query"
        resp = requests.get(url_v3, headers=self._headers(), timeout=30)
        if resp.ok:
            data = resp.json()
            if data.get("code") == 0:
                return self._parse_sheets(data.get("data", {}).get("sheets", []), spreadsheet_token)
            logger.error("v3 code=%s msg=%s token=%s",
                         data.get("code"), data.get("msg"), spreadsheet_token)
        else:
            logger.error("v3 HTTP %s token=%s body=%s",
                         resp.status_code, spreadsheet_token, resp.text[:200])

        url_v2 = f"{self.base_url}/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/metainfo"
        resp2 = requests.get(url_v2, headers=self._headers(), timeout=30)
        if resp2.ok:
            data2 = resp2.json()
            if data2.get("code") == 0:
                sheets_raw = data2.get("data", {}).get("sheets", [])
                sheets = [{"title": s.get("title", ""), "sheet_id": s.get("sheetId", "")}
                          for s in sheets_raw]
                return self._parse_sheets(sheets, spreadsheet_token)
            raise Exception(
                f"Cannot read spreadsheet {spreadsheet_token}: "
                f"code={data2.get('code')} msg={data2.get('msg')}"
            )
        raise Exception(f"Cannot read spreadsheet {spreadsheet_token}: HTTP {resp2.status_code}")

    def _parse_sheets(self, sheets, spreadsheet_token):
        result = []
        for s in sheets:
            title = s.get("title", "")
            sheet_id = s.get("sheet_id", "")
            if title not in SKIP_TABS:
                result.append({"title": title, "sheet_id": sheet_id})
        logger.info("Found %d processable tabs in %s", len(result), spreadsheet_token)
        return result

    def list_folder_sheets(self, folder_token, _depth=0):
        """Recursively list spreadsheets in a Lark Drive folder.

        Returns [{"token": ..., "name": ...}]. Subfolders are followed only
        when their name contains "SHIP" so quote/invoice folders are skipped.
        Failures are logged and return what was found so the static
        LARK_SHEET_TOKENS list keeps working. Requires the Lark app to have
        a Drive read scope (drive:drive:readonly).
        """
        results = []
        if _depth > 3:
            return results
        page_token = ""
        while True:
            params = {"folder_token": folder_token, "page_size": 200}
            if page_token:
                params["page_token"] = page_token
            try:
                resp = requests.get(
                    f"{self.base_url}/open-apis/drive/v1/files",
                    headers=self._headers(), params=params, timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error("Folder listing failed for %s: %s", folder_token, e)
                return results
            if data.get("code") != 0:
                logger.error("Folder listing failed for %s: code=%s msg=%s",
                             folder_token, data.get("code"), data.get("msg"))
                return results
            payload = data.get("data", {})
            for f in payload.get("files", []):
                ftype = f.get("type", "")
                name = f.get("name", "")
                if ftype == "sheet":
                    results.append({"token": f.get("token", ""), "name": name})
                elif ftype == "folder" and "SHIP" in name.upper():
                    results.extend(self.list_folder_sheets(f.get("token", ""), _depth + 1))
            if payload.get("has_more") and payload.get("next_page_token"):
                page_token = payload["next_page_token"]
            else:
                break
        return results

    def read_sheet_range(self, spreadsheet_token, sheet_id, start_col, end_col,
                         start_row, end_row):
        range_str = f"{sheet_id}!{start_col}{start_row}:{end_col}{end_row}"
        url = (f"{self.base_url}/open-apis/sheets/v2/spreadsheets/"
               f"{spreadsheet_token}/values/{range_str}")
        resp = requests.get(url, headers=self._headers(),
                            params={"valueRenderOption": "ToString"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to read range {range_str}: {data}")
        rows = data.get("data", {}).get("valueRange", {}).get("values", [])
        logger.info("Read %d raw rows from %s", len(rows), range_str)
        return rows

    def read_tracking_data(self, spreadsheet_token, sheet_id):
        cols = columns_for(spreadsheet_token)
        i_shipment = col_to_index(cols.get("shipment_id", ""))
        i_vendor = col_to_index(cols.get("vendor", ""))
        i_recipient = col_to_index(cols.get("recipient", ""))
        i_order = col_to_index(cols.get("order_num", ""))
        i_customer = col_to_index(cols.get("customer", ""))
        i_tracking = col_to_index(cols.get("tracking_num", ""))
        i_carrier = col_to_index(cols.get("carrier", ""))
        i_status = col_to_index(cols.get("status", ""))
        i_num_boxes = col_to_index(cols.get("num_boxes", ""))
        i_delivery = col_to_index(cols.get("delivery_date", ""))

        def cell(row, idx):
            if idx is None or idx < 0 or idx >= len(row):
                return ""
            return str(row[idx] or "").strip()

        start_row = header_row_for(spreadsheet_token) + 1
        rows = self.read_sheet_range(
            spreadsheet_token, sheet_id,
            start_col="A", end_col="R",
            start_row=start_row, end_row=500,
        )

        MIN_COLS = 18
        results = []
        last_shipment_id = ""
        last_tracking = ""
        last_carrier = ""
        last_num_boxes = ""

        for i, row in enumerate(rows):
            if not isinstance(row, list):
                continue
            while len(row) < MIN_COLS:
                row.append("")

            shipment_id_raw = cell(row, i_shipment)
            tracking_tokens = cell(row, i_tracking).split(); tracking_raw = tracking_tokens[0] if tracking_tokens else ""
            carrier_raw = cell(row, i_carrier)
            num_boxes_raw = cell(row, i_num_boxes)

            shipment_id = shipment_id_raw or last_shipment_id
            tracking = tracking_raw or ("" if (shipment_id_raw or cell(row, i_order)) else last_tracking)
            carrier = carrier_raw or last_carrier
            num_boxes = num_boxes_raw or last_num_boxes

            if shipment_id_raw:
                last_shipment_id = shipment_id_raw
            if tracking_raw:
                last_tracking = tracking_raw
            if carrier_raw:
                last_carrier = carrier_raw
            if num_boxes_raw:
                last_num_boxes = num_boxes_raw

            if not any(str(c or "").strip() for c in row):
                last_shipment_id = ""
                last_tracking = ""
                last_carrier = ""
                last_num_boxes = ""
                continue

            if not tracking:
                continue

            if not carrier:
                logger.warning(
                    "  Row %d: tracking=%s but no carrier (even after carry-forward) - skipping",
                    start_row + i, tracking,
                )
                continue

            status_raw = cell(row, i_status)
            delivery_raw = cell(row, i_delivery)

            results.append({
                "row_num": start_row + i,
                "shipment_id": shipment_id,
                "vendor": cell(row, i_vendor),
                "recipient": cell(row, i_recipient),
                "customer": cell(row, i_customer),
                "order_num": cell(row, i_order),
                "tracking_num": tracking, "extra_tracking": max(0, len(tracking_tokens) - 1),
                "carrier": carrier,
                "num_boxes": num_boxes,
                "current_status": status_raw,
                "delivery_date": delivery_raw,
            })

        logger.info("  %d rows with tracking in sheet %s", len(results), sheet_id)
        return results

    def read_all_status_rows(self, spreadsheet_token, sheet_id):
        """Read every row with a non-empty status value in the status column.

        The status column letter is resolved per-spreadsheet via columns_for().
        Returns a list of dicts with keys: row_num, current_status.
        """
        cols = columns_for(spreadsheet_token)
        status_col = cols.get("status", "N") or "N"
        start_row = header_row_for(spreadsheet_token) + 1
        range_str = f"{sheet_id}!{status_col}{start_row}:{status_col}500"
        url = (
            f"{self.base_url}/open-apis/sheets/v2/spreadsheets/"
            f"{spreadsheet_token}/values/{range_str}"
        )
        resp = requests.get(
            url,
            headers=self._headers(),
            params={"valueRenderOption": "ToString"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to read status column {range_str}: {data}")
        raw_rows = data.get("data", {}).get("valueRange", {}).get("values", [])
        results = []
        for i, row in enumerate(raw_rows):
            status_val = ""
            if isinstance(row, list) and len(row) > 0:
                status_val = str(row[0] or "").strip()
            if status_val:
                results.append({
                    "row_num": start_row + i,
                    "current_status": status_val,
                })
        logger.info("  %d rows with status in sheet %s", len(results), sheet_id)
        return results

    def write_cells(self, spreadsheet_token, sheet_id, updates):
        if not updates:
            return
        value_ranges = []
        for u in updates:
            range_str = f"{sheet_id}!{u['col']}{u['row']}:{u['col']}{u['row']}"
            value_ranges.append({"range": range_str, "values": [[u["value"]]]})
        url = (f"{self.base_url}/open-apis/sheets/v2/spreadsheets/"
               f"{spreadsheet_token}/values_batch_update")
        resp = requests.post(url, headers=self._headers(),
                             json={"valueRanges": value_ranges}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception(f"Failed to write cells: {data}")
        logger.info("Updated %d cells in sheet %s", len(updates), sheet_id)

    def update_tracking_row(self, spreadsheet_token, sheet_id, row_num,
                            delivery_date="", num_boxes=""):
        """Update delivery date and num_boxes for a tracking row.

        NOTE: We intentionally do NOT write to the status column to preserve
        the Lark dropdown widget and its color formatting. Column positions
        are resolved per-spreadsheet via columns_for().
        """
        cols = columns_for(spreadsheet_token)
        delivery_col = cols.get("delivery_date", "")
        num_boxes_col = cols.get("num_boxes", "")
        updates = []
        if delivery_date and delivery_col:
            updates.append({"row": row_num, "col": delivery_col, "value": delivery_date})
        if num_boxes and num_boxes_col:
            updates.append({"row": row_num, "col": num_boxes_col, "value": str(num_boxes)})
        if updates:
            self.write_cells(spreadsheet_token, sheet_id, updates)

    STATUS_CELL_COLORS = {
        "Label Created/Not Scanned": "#FF6B6B",
        "In Transit": "#1677FF",
        "Delivered": "#00B96B",
        "Exception/Delay": "#FA8C16",
    }

    def set_status_cell_style(self, spreadsheet_token, sheet_id, row_num, status_value):
        """Apply a background color to the Status cell for a given row."""
        cols = columns_for(spreadsheet_token)
        status_col = cols.get("status", "N") or "N"
        color_hex = self.STATUS_CELL_COLORS.get(status_value)
        if not color_hex:
            logger.warning("set_status_cell_style: unknown status %r, skipping", status_value)
            return
        range_str = f"{sheet_id}!{status_col}{row_num}:{status_col}{row_num}"
        self._apply_cell_background(spreadsheet_token, [range_str], color_hex)

    def set_status_styles_batch(self, spreadsheet_token, sheet_id, row_status_pairs):
        """Apply background colors to multiple Status cells in a single API call."""
        if not row_status_pairs:
            return
        cols = columns_for(spreadsheet_token)
        status_col = cols.get("status", "N") or "N"
        by_color = {}
        for row_num, status_value in row_status_pairs:
            color_hex = self.STATUS_CELL_COLORS.get(status_value)
            if not color_hex:
                logger.warning(
                    "set_status_styles_batch: unknown status %r for row %d, skipping",
                    status_value, row_num,
                )
                continue
            by_color.setdefault(color_hex, []).append(f"{sheet_id}!{status_col}{row_num}:{status_col}{row_num}")

        for color_hex, ranges in by_color.items():
            self._apply_cell_background(spreadsheet_token, ranges, color_hex)

    def _apply_cell_background(self, spreadsheet_token, ranges, color_hex):
        """Call styles_batch_update to set background color on the given ranges."""
        url = (
            f"{self.base_url}/open-apis/sheets/v2/spreadsheets/"
            f"{spreadsheet_token}/styles_batch_update"
        )
        payload = {
            "data": [
                {
                    "ranges": ranges,
                    "style": {"backColor": color_hex},
                }
            ]
        }
        try:
            resp = requests.put(
                url, headers=self._headers(), json=payload, timeout=30
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 0:
                logger.error(
                    "styles_batch_update failed for ranges %s color %s: code=%s msg=%s",
                    ranges, color_hex, data.get("code"), data.get("msg"),
                )
            else:
                logger.info("Applied background %s to %d range(s)", color_hex, len(ranges))
        except Exception as e:
            logger.error("_apply_cell_background failed: %s", e)

    def send_group_message(self, message, chat_id=None, message_id=None):
        """Send message to Lark group. Falls back to plain text if card fails."""
        target_chat = chat_id or LARK_CHAT_ID
        if not target_chat:
            logger.warning("No chat_id configured, skipping message")
            return
        try:
            self._send_card(message, target_chat, message_id)
            return
        except Exception as e:
            logger.warning("Interactive card failed (%s), retrying as plain text", e)
        try:
            self._send_text(message, target_chat, message_id)
        except Exception as e:
            logger.error("Plain text message also failed: %s", e)
            raise

    def _send_card(self, message, chat_id, message_id=None, card_json=None):
        url = f"{self.base_url}/open-apis/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        content = card_json if card_json else self._build_card_message(message)
        body = {
            "receive_id": chat_id,
            "msg_type": "interactive",
            "content": content,
        }
        if message_id:
            url = f"{self.base_url}/open-apis/im/v1/messages/{message_id}/reply"
            params = {}
            body = {"msg_type": "interactive", "content": content}
        resp = requests.post(url, headers=self._headers(), params=params, json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception("Card send failed: code=%s msg=%s" % (data.get("code"), data.get("msg")))
        logger.info("Interactive card sent to group chat")

    def _send_text(self, message, chat_id, message_id=None):
        url = f"{self.base_url}/open-apis/im/v1/messages"
        params = {"receive_id_type": "chat_id"}
        body = {
            "receive_id": chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": message}),
        }
        if message_id:
            url = f"{self.base_url}/open-apis/im/v1/messages/{message_id}/reply"
            params = {}
            body = {"msg_type": "text", "content": json.dumps({"text": message})}
        resp = requests.post(url, headers=self._headers(), params=params, json=body, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("code") != 0:
            raise Exception("Text send failed: code=%s msg=%s" % (data.get("code"), data.get("msg")))
        logger.info("Plain text message sent to group chat")

    def _build_card_message(self, text_content):
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "HLT Shipment Update"},
                "template": "blue",
            },
            "elements": [{"tag": "markdown", "content": text_content}],
        }
        return json.dumps(card)

    def _build_alert_card(self, text_content):
        """Red-banner card used for exception alerts."""
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"tag": "plain_text", "content": "Shipment Alert"},
                "template": "red",
            },
            "elements": [{"tag": "markdown", "content": text_content}],
        }
        return json.dumps(card)

    @staticmethod
    def _format_delivery_date_long(raw_date):
        """Format a date as Thursday, March 26th 2026 style for the chat message."""
        if not raw_date:
            return ""
        clean = raw_date.strip()[:10]
        for fmt in ("%Y-%m-%d", "%m-%d-%Y", "%m/%d/%Y"):
            try:
                dt = datetime.strptime(clean, fmt)
                day = dt.day
                if 11 <= day <= 13:
                    suffix = "th"
                elif day % 10 == 1:
                    suffix = "st"
                elif day % 10 == 2:
                    suffix = "nd"
                elif day % 10 == 3:
                    suffix = "rd"
                else:
                    suffix = "th"
                return dt.strftime(f"%A, %B {day}{suffix} %Y")
            except (ValueError, TypeError):
                continue
        return raw_date

    @staticmethod
    def _format_delivery_date(raw_date):
        if not raw_date:
            return ""
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%m-%d-%Y"):
            try:
                dt = datetime.strptime(raw_date.strip()[:10], fmt)
                return "expected delivery on " + dt.strftime("%A, %B %d, %Y").replace(" 0", " ")
            except (ValueError, TypeError):
                continue
        return raw_date

    @staticmethod
    def _format_date_short(raw_date):
        if not raw_date:
            return ""
        try:
            dt = datetime.strptime(raw_date[:10], "%Y-%m-%d")
            return dt.strftime("%b %-d")
        except Exception:
            return raw_date

    @staticmethod
    def _section_for(r):
        token = r.get("sheet_token", "").strip()
        return SHEET_OWNERS.get(token, "Other")

    @staticmethod
    def _is_fully_delivered(r):
        """Return True only when ALL boxes in a shipment are confirmed delivered."""
        packages = r.get("packages", [])
        status = r.get("new_status", "").upper()
        if packages:
            statuses = [p.get("status", "").upper() for p in packages]
            if all("DELIVERED" in s for s in statuses):
                return True
            # Carriers often never scan every box of a multi-piece shipment.
            # If at least one box arrived and nothing is still moving, treat
            # the shipment as delivered instead of reporting it forever.
            delivered_any = any("DELIVERED" in s for s in statuses)
            still_moving = any(
                p.get("scanned") and "DELIVERED" not in p.get("status", "").upper()
                for p in packages
            )
            return delivered_any and not still_moving
        return status == "DELIVERED" or (r.get("raw_status", "") or "").strip().upper().startswith("DELIVERED")

    @staticmethod
    def _tracking_url(tracking, carrier):
        """Return a clickable tracking URL for the given carrier."""
        c = (carrier or "").strip().upper()
        if c == "FEDEX":
            return f"https://www.fedex.com/fedextrack/?trknbr={tracking}"
        if c == "UPS":
            return f"https://www.ups.com/track?tracknum={tracking}"
        if c == "USPS":
            return f"https://tools.usps.com/go/TrackConfirmAction?tLabels={tracking}"
        if c == "DHL":
            return f"https://www.dhl.com/en/express/tracking.html?AWB={tracking}"
        return ""

    @staticmethod
    def _shipment_line(r):
        """Format one shipment line for the daily summary."""
        tracking = r.get("tracking_num", "N/A")
        order = r.get("order_num", "").strip()
        customer = r.get("customer", "").strip()
        recipient = r.get("recipient", "").strip()
        carrier = r.get("carrier", "").strip()

        if recipient.upper() == "BRENDAN":
            name = "Brendan"
        elif recipient.upper() == "CUSTOMER DIRECT":
            name = customer or "Unknown"
        else:
            name = customer or recipient or "Unknown"

        status = r.get("new_status", "").upper()
        raw_status = r.get("raw_status", "").strip()
        location = r.get("location", "").strip()
        delivery = r.get("delivery_date", "").strip()
        packages = r.get("packages", [])

        group_count = int(r.get("group_count", 1) or 1)
        grouped = group_count > 1
        # For shipments where one tracking number covers many sheet rows, show a
        # single grouped line representing all the items (no per-order/customer).
        if grouped:
            name = ""
            order = ""

        url = LarkClient._tracking_url(tracking, carrier)
        tracking_display = (f"[**{tracking}**]({url})" if url else f"**{tracking}**") + (f" ({group_count} orders)" if grouped else "") + (f" ({r.get('extra_tracking', 0) + 1} boxes)" if r.get("extra_tracking") and not (packages and len(packages) > 1) else "") + (f" ({str(r.get('num_boxes') or '').strip()} boxes)" if grouped and str(r.get('num_boxes') or '').strip() and not packages and not r.get("extra_tracking") else "")

        if packages and len(packages) > 1:
            total = len(packages)
            delivered = [p for p in packages if "DELIVERED" in p.get("status", "").upper()]
            in_transit = [p for p in packages if p.get("scanned")
                          and "DELIVERED" not in p.get("status", "").upper()]
            unscanned = [p for p in packages if not p.get("scanned")]
            n_del = len(delivered)
            n_it = len(in_transit)
            n_uns = len(unscanned)
            parts = []
            if n_del:
                parts.append(f"{n_del} of {total} delivered")
            if n_it:
                date_groups = defaultdict(int)
                for p in in_transit:
                    d = p.get("delivery_date", "") or ""
                    date_groups[d] += 1
                for date_str in sorted(date_groups):
                    count = date_groups[date_str]
                    label = LarkClient._format_date_short(date_str) if date_str else "no date"
                    parts.append(f"{count} arriving {label}")
            if n_uns:
                parts.append(f"{n_uns} not yet scanned")
            if not parts:
                parts.append("in transit")
            box_summary = ", ".join(parts)
            return f"- {tracking_display} ({total} boxes) -- {box_summary}" if grouped else f"- {tracking_display} ({total} boxes) -- {name} -- {box_summary}"

        if status == "DELIVERED":
            if delivery:
                date_str = LarkClient._format_delivery_date_long(delivery)
                status_desc = f"delivered on {date_str}" if date_str else "delivered"
            else:
                status_desc = "delivered"
        elif status == "EXCEPTION/DELAY":
            if raw_status:
                status_desc = f"exception - {raw_status}"
            else:
                status_desc = "exception/delay"
        elif status == "LABEL CREATED/NOT SCANNED":
            status_desc = "label created - not yet scanned"
        else:
            if raw_status and location:
                status_desc = raw_status.lower() if location.split("-")[0].strip().lower() in raw_status.lower() else f"{raw_status.lower()} in {location}"
            elif location:
                status_desc = f"in transit in {location}"
            elif raw_status:
                status_desc = raw_status.lower()
            else:
                status_desc = "in transit"

        if status == "DELIVERED":
            date_desc = ""
        elif delivery:
            date_long = LarkClient._format_delivery_date_long(delivery)
            date_desc = f" - estimated delivery date of {date_long}" if date_long else ""
        else:
            if status not in ("DELIVERED", "LABEL CREATED/NOT SCANNED"):
                date_desc = " - no estimated delivery date yet"
            else:
                date_desc = ""

        city = ""
        if location:
            city = location.replace(" - ", ",").split(",")[0].strip()
        loc_desc = f" in {city}" if (status == "DELIVERED" and city) else ""

        if grouped:
            return f"- {tracking_display} -- {status_desc}{date_desc}{loc_desc}"
        return f"- {tracking_display} -- {order} -- {name} -- {status_desc}{date_desc}{loc_desc}"

    def send_daily_summary(self, all_results, chat_id=None, message_id=None):
        """Send the shipment summary card to the Lark group chat."""
        active = [r for r in all_results if not LarkClient._is_fully_delivered(r)]
        if not active:
            self.send_group_message(
                "All shipments delivered. Nothing to track.",
                chat_id=chat_id, message_id=message_id,
            )
            return

        # Count how many active rows share each tracking number so we can
        # collapse them into a single grouped line ("N items").
        tracking_counts = {}
        for r in active:
            tn = r.get("tracking_num", "").strip()
            if tn:
                tracking_counts[tn] = tracking_counts.get(tn, 0) + 1

        seen, unique = set(), []
        for r in active:
            tn = r.get("tracking_num", "").strip()
            if tn and tn not in seen:
                seen.add(tn)
                r = {**r, "group_count": tracking_counts.get(tn, 1)}
                unique.append(r)

        buckets = {tab: [] for tab in PERMANENT_TABS}
        for r in unique:
            section = self._section_for(r)
            buckets.setdefault(section, []).append(r)

        NL = chr(10)
        lines = ["**HLT Shipment Tracker**"]

        def render_section(label, items):
            display = SECTION_DISPLAY.get(label, label.upper())
            lines.append(NL + NL + "<font color='orange'>**" + "".join(c + "\u0332" for c in display) + "**</font>")
            if not items:
                lines.append("No active shipments")
                return
            by_carrier = {}
            for r in items:
                c = r.get("carrier", "").strip().upper() or "UNKNOWN"
                by_carrier.setdefault(c, []).append(r)
            for carrier in sorted(by_carrier):
                lines.append(NL + "".join(c + "\u0332" for c in carrier))
                for r in sorted(by_carrier[carrier], key=lambda rr: 0 if ("EXCEPTION" in (rr.get("new_status", "") or "").upper() or "DELAY" in (rr.get("new_status", "") or "").upper()) else (1 if "TRANSIT" in (rr.get("new_status", "") or "").upper() else 2)):
                    lines.append(LarkClient._shipment_line(r))

        for tab_name in PERMANENT_TABS:
            render_section(tab_name, buckets[tab_name])

        # Client sheet sections (owners beyond Hannah/Lucy/Other) render
        # below the permanent sections, only when they have active shipments.
        for tab_name in sorted(s for s in buckets if s not in PERMANENT_TABS):
            if buckets[tab_name]:
                render_section(tab_name, buckets[tab_name])

        self.send_group_message(
            NL.join(lines),
            chat_id=chat_id, message_id=message_id,
        )

    def send_exception_alerts(self, alerts, chat_id=None):
        """Send a red-banner alert card for newly detected shipping exceptions."""
        target_chat = chat_id or LARK_CHAT_ID
        if not target_chat:
            logger.warning("No chat_id configured, skipping exception alerts")
            return

        MONTH_NAMES = [
            "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
            "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
        ]
        now = datetime.now()
        current_month = MONTH_NAMES[now.month - 1]

        NL = chr(10)
        lines = ["**HLT Shipment Alert**",
                 NL + "The following shipments need attention:"]

        for a in alerts:
            tracking = a.get("tracking_num", "N/A")
            carrier = a.get("carrier", "")
            name = a.get("name", "")
            tab = a.get("tab", "")
            raw = a.get("raw_status", "").strip()
            num_boxes = a.get("num_boxes", "").strip()

            if raw:
                detail = raw
            else:
                detail = a.get("new_status", "").title()

            if num_boxes and num_boxes != "1":
                box_tag = f" ({num_boxes} boxes)"
            elif num_boxes == "1":
                box_tag = " (1 box)"
            else:
                box_tag = ""

            tab_upper = tab.upper().strip()
            if tab_upper in MONTH_NAMES and tab_upper != current_month:
                month_tag = f" [{tab_upper}]"
            else:
                month_tag = ""

            line = f"- **{carrier}** {tracking}{box_tag} -- {name}{month_tag}: {detail}"
            lines.append(line)

        message = NL.join(lines)
        alert_card = self._build_alert_card(message)

        try:
            self._send_card("", target_chat, card_json=alert_card)
            logger.info("Exception alert card sent (%d alerts)", len(alerts))
        except Exception as e:
            logger.warning("Alert card failed (%s), sending as plain text", e)
            try:
                self._send_text(message, target_chat)
            except Exception as e2:
                logger.error("Exception alert plain text also failed: %s", e2)
