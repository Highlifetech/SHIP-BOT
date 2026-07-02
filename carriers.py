"""
Carrier Tracking Clients

Uses the official FedEx Track API (requires credentials).
Uses free public tracking endpoints for USPS/Royal Mail.
UPS and DHL use their respective APIs (existing credentials).
SF Express uses its public international tracking endpoint (no key).

UPS multi-piece shipments: when a tracking number is part of a multi-box
shipment, the API returns all sibling packages. We capture total boxes,
how many are scanned/unscanned, and delivery date breakdown.
"""
import logging
import time
import re
import json
import requests
from collections import defaultdict
from datetime import datetime

from config import (
    FEDEX_API_KEY,
    FEDEX_SECRET_KEY,
    UPS_CLIENT_ID,
    UPS_CLIENT_SECRET,
    DHL_API_KEY,
    SEVENTEENTRACK_API_KEY,
    STATUS_MAP,
)

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

def _fmt_date(date_str):
    """Convert YYYY-MM-DD to MM-DD-YYYY for the Lark Sheet."""
    if not date_str:
        return ""
    raw = date_str.strip()[:10]
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%m/%d/%Y", "%m-%d-%Y"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%m-%d-%Y")
        except (ValueError, TypeError):
            continue
    return date_str

def normalize_result(status, delivery_date="", location="", raw_status="",
                     error="", packages=None):
    """Return a standardized tracking result.

    packages: list of dicts for multi-piece UPS shipments, each:
        {"tracking_num": str, "status": str, "delivery_date": str, "scanned": bool}
    """
    return {
        "status": STATUS_MAP.get(status, STATUS_MAP["unknown"]),
        "status_key": status,
        "delivery_date": _fmt_date(delivery_date),
        "location": location,
        "raw_status": raw_status,
        "error": error,
        "packages": packages or [],
    }

def _safe_expires(data, key="expires_in", default=3600):
    val = data.get(key, default)
    try:
        return int(float(str(val)))
    except (ValueError, TypeError):
        return default

def _parse_ups_date(date_str):
    """Convert UPS date string YYYYMMDD to YYYY-MM-DD."""
    if date_str and len(str(date_str)) == 8:
        s = str(date_str)
        return "%s-%s-%s" % (s[:4], s[4:6], s[6:])
    return ""

def _format_date_short(date_str):
    """Convert YYYY-MM-DD to 'Mar 5' style."""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return dt.strftime("%b %-d")
    except Exception:
        return date_str

# =============================================================================
# FedEx Track API v1
# =============================================================================

class FedExTracker:
    TOKEN_URL = "https://apis.fedex.com/oauth/token"
    TRACK_URL = "https://apis.fedex.com/track/v1/trackingnumbers"

    def __init__(self):
        self.token = None
        self.token_expires = 0

    def _authenticate(self):
        if self.token and time.time() < self.token_expires:
            return self.token
        if not FEDEX_API_KEY or not FEDEX_SECRET_KEY:
            raise Exception("FedEx API credentials not configured")
        resp = requests.post(self.TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": FEDEX_API_KEY,
            "client_secret": FEDEX_SECRET_KEY,
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        self.token = data["access_token"]
        self.token_expires = time.time() + _safe_expires(data) - 300
        return self.token

    def track(self, tracking_number):
        try:
            token = self._authenticate()
            headers = {
                "Authorization": "Bearer %s" % token,
                "Content-Type": "application/json",
            }
            body = {
                "trackingInfo": [
                    {"trackingNumberInfo": {"trackingNumber": tracking_number}}
                ],
                "includeDetailedScans": False,
            }
            resp = requests.post(self.TRACK_URL, headers=headers, json=body, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            results = (data.get("output", {})
                       .get("completeTrackResults", [{}])[0]
                       .get("trackResults", [{}])[0])

            if results.get("error"):
                return normalize_result("not_found",
                                        error=results["error"].get("message", ""))

            latest = results.get("latestStatusDetail", {})
            status_code = latest.get("code", "").upper()
            raw_status = latest.get("description", "")

            location_info = latest.get("scanLocation", {})
            location = ", ".join(filter(None, [
                location_info.get("city"),
                location_info.get("stateOrProvinceCode"),
                location_info.get("countryCode"),
            ]))

            status_map = {
                "DL": "delivered", "IT": "in_transit",
                "OD": "out_for_delivery", "DE": "exception",
                "PU": "in_transit", "PL": "label_created",
            }
            status = status_map.get(status_code, "in_transit")

            delivery_date = ""
            for d in results.get("dateAndTimes", []):
                if d.get("type") in ("ACTUAL_DELIVERY", "ESTIMATED_DELIVERY"):
                    delivery_date = d.get("dateTime", "")[:10]
                    break

            # ---- Multi-piece: check all trackResults ----
            all_track_results = (data.get("output", {})
                                 .get("completeTrackResults", [{}])[0]
                                 .get("trackResults", []))
            packages_info = []
            if len(all_track_results) > 1:
                for tr in all_track_results:
                    tr_tracking = tr.get("trackingNumberInfo", {}).get("trackingNumber", "")
                    tr_latest = tr.get("latestStatusDetail", {})
                    tr_code = tr_latest.get("code", "").upper()
                    tr_status = status_map.get(tr_code, "in_transit")
                    tr_date = ""
                    for dd in tr.get("dateAndTimes", []):
                        if dd.get("type") in ("ACTUAL_DELIVERY", "ESTIMATED_DELIVERY"):
                            tr_date = dd.get("dateTime", "")[:10]
                            break
                    tr_scanned = tr_code not in ("", "PL")
                    packages_info.append({
                        "tracking_num": tr_tracking,
                        "status": STATUS_MAP.get(tr_status, STATUS_MAP["unknown"]),
                        "delivery_date": tr_date,
                        "scanned": tr_scanned,
                    })
            else:
                piece_count = results.get("pieceCount", "")
                if not piece_count:
                    piece_count = results.get("packageDetails", {}).get("count", "")
                if piece_count and str(piece_count) not in ("", "0", "1"):
                    for i in range(int(piece_count)):
                        packages_info.append({
                            "tracking_num": tracking_number,
                            "status": STATUS_MAP.get(status, STATUS_MAP["unknown"]),
                            "delivery_date": delivery_date,
                            "scanned": status_code not in ("", "PL"),
                        })

            return normalize_result(status, delivery_date, location, raw_status,
                                    packages=packages_info)

        except Exception as e:
            logger.error("FedEx tracking error for %s: %s", tracking_number, e)
            return normalize_result("unknown", error=str(e))

# =============================================================================
# UPS Tracking API -- with multi-piece shipment support
# =============================================================================

class UPSTracker:
    TOKEN_URL = "https://onlinetools.ups.com/security/v1/oauth/token"
    TRACK_URL = "https://onlinetools.ups.com/api/track/v1/details"

    def __init__(self):
        self.token = None
        self.token_expires = 0

    def _authenticate(self):
        if self.token and time.time() < self.token_expires:
            return self.token
        if not UPS_CLIENT_ID or not UPS_CLIENT_SECRET:
            raise Exception("UPS API credentials not configured")
        resp = requests.post(
            self.TOKEN_URL,
            data={"grant_type": "client_credentials"},
            auth=(UPS_CLIENT_ID, UPS_CLIENT_SECRET),
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self.token = data["access_token"]
        self.token_expires = time.time() + _safe_expires(data, "expires_in", 14400) - 300
        return self.token

    def _get_package_delivery_date(self, package):
        """Extract the best delivery date from a UPS package object."""
        del_date = package.get("deliveryDate", [])
        if del_date:
            d = del_date[0] if isinstance(del_date, list) else del_date
            return _parse_ups_date(d.get("date", ""))
        activity = package.get("activity", [])
        if activity:
            date_str = str(activity[0].get("date", ""))
            return _parse_ups_date(date_str)
        return ""

    def _is_scanned(self, package):
        """Return True if this package has had any real carrier scan."""
        activity = package.get("activity", [])
        if not activity:
            return False
        for act in activity:
            status = act.get("status", {})
            status_type = status.get("type", "").upper()
            if status_type not in ("M", ""):
                return True
        return False

    def _get_package_status(self, package):
        """Return normalized status string for a package."""
        activity = package.get("activity", [])
        if not activity:
            return "label_created"
        latest = activity[0]
        status_type = latest.get("status", {}).get("type", "").upper()
        status_map = {
            "D": "delivered", "I": "in_transit",
            "P": "in_transit", "M": "label_created",
            "X": "exception", "O": "out_for_delivery",
        }
        return status_map.get(status_type, "in_transit")

    def track(self, tracking_number):
        try:
            token = self._authenticate()
            headers = {
                "Authorization": "Bearer %s" % token,
                "Content-Type": "application/json",
                "transId": "track-%s" % tracking_number[:20],
                "transactionSrc": "lark-tracking-bot",
            }
            url = "%s/%s" % (self.TRACK_URL, tracking_number)
            resp = requests.get(
                url, headers=headers,
                params={"locale": "en_US", "returnSignature": "false"},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()

            track_resp = data.get("trackResponse", {})
            shipment = track_resp.get("shipment", [{}])[0]
            all_packages = shipment.get("package", [])
            if not all_packages:
                return normalize_result("not_found")

            primary = all_packages[0]
            activity = primary.get("activity", [])
            if not activity:
                return normalize_result("not_found")

            latest = activity[0]
            status_type = latest.get("status", {}).get("type", "").upper()
            raw_status = latest.get("status", {}).get("description", "")

            location_obj = latest.get("location", {}).get("address", {})
            location = ", ".join(filter(None, [
                location_obj.get("city"),
                location_obj.get("stateProvince"),
                location_obj.get("country"),
            ]))

            status_map = {
                "D": "delivered", "I": "in_transit",
                "P": "in_transit", "M": "label_created",
                "X": "exception", "O": "out_for_delivery",
            }
            status = status_map.get(status_type, "in_transit")
            delivery_date = self._get_package_delivery_date(primary)

            if status == "delivered" and not delivery_date:
                date_str = str(latest.get("date", ""))
                delivery_date = _parse_ups_date(date_str)

            packages_info = []
            if len(all_packages) > 1:
                for pkg in all_packages:
                    pkg_tracking = pkg.get("trackingNumber", "")
                    pkg_status = self._get_package_status(pkg)
                    pkg_date = self._get_package_delivery_date(pkg)
                    pkg_scanned = self._is_scanned(pkg)
                    packages_info.append({
                        "tracking_num": pkg_tracking,
                        "status": STATUS_MAP.get(pkg_status, STATUS_MAP["unknown"]),
                        "delivery_date": pkg_date,
                        "scanned": pkg_scanned,
                    })

            return normalize_result(status, delivery_date, location, raw_status,
                                    packages=packages_info)

        except Exception as e:
            logger.error("UPS tracking error for %s: %s", tracking_number, e)
            return normalize_result("unknown", error=str(e))

# =============================================================================
# USPS - Scrapes the public USPS tracking page (no API key needed)
# =============================================================================

class USPSTracker:
    TRACK_URL = "https://tools.usps.com/go/TrackConfirmAction"

    def track(self, tracking_number):
        try:
            headers = {**HEADERS, "Referer": "https://tools.usps.com/go/TrackConfirmAction"}
            resp = requests.get(
                self.TRACK_URL,
                params={"tLabels": tracking_number},
                headers=headers,
                timeout=30,
            )
            resp.raise_for_status()
            html = resp.text

            raw_status = ""
            status = "in_transit"
            delivery_date = ""

            if re.search(r"Delivered", html, re.IGNORECASE):
                status = "delivered"
                raw_status = "Delivered"
                date_match = re.search(
                    r"(January|February|March|April|May|June|July|August|"
                    r"September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
                    html, re.IGNORECASE
                )
                if date_match:
                    try:
                        delivery_date = datetime.strptime(
                            date_match.group(0), "%B %d, %Y"
                        ).strftime("%Y-%m-%d")
                    except Exception:
                        pass
            elif re.search(r"Out for Delivery", html, re.IGNORECASE):
                status = "out_for_delivery"
                raw_status = "Out for Delivery"
            elif re.search(r"In Transit", html, re.IGNORECASE):
                status = "in_transit"
                raw_status = "In Transit"
            elif re.search(r"Alert", html, re.IGNORECASE):
                status = "exception"
                raw_status = "Alert"
            elif re.search(r"Pre-Shipment|Label Created", html, re.IGNORECASE):
                status = "label_created"
                raw_status = "Pre-Shipment Info Sent"
            else:
                if "not found" in html.lower() or "not available" in html.lower():
                    return normalize_result("not_found")
                raw_status = "In Transit"

            return normalize_result(status, delivery_date, "", raw_status)

        except Exception as e:
            logger.error("USPS tracking error for %s: %s", tracking_number, e)
            return normalize_result("unknown", error=str(e))

# =============================================================================
# DHL Tracking API -- with throttling + 429 backoff
# =============================================================================

class DHLTracker:
    TRACK_URL = "https://api-eu.dhl.com/track/shipments"

    # DHL free tier is rate limited (roughly 1 request/sec, 250/day). Keep a
    # class-level timestamp so calls are spaced out across the run. We do NOT
    # retry on HTTP 429: a 429 from this key almost always means the daily
    # quota is exhausted, so retrying just burns runtime. Instead we remember
    # numbers that failed and, after several consecutive 429s, stop calling
    # DHL for the rest of the run (treat the quota as exhausted).
    _last_call = 0.0
    _MIN_INTERVAL = 0.6  # seconds between DHL API calls
    _failed_numbers = {}          # tracking_number -> cached result
    _consecutive_429 = 0
    _quota_exhausted = False
    _QUOTA_429_THRESHOLD = 5      # stop calling DHL after this many 429s in a row

    def _throttle(self):
        """Sleep so consecutive DHL calls are at least _MIN_INTERVAL apart."""
        now = time.time()
        elapsed = now - DHLTracker._last_call
        if elapsed < DHLTracker._MIN_INTERVAL:
            time.sleep(DHLTracker._MIN_INTERVAL - elapsed)
        DHLTracker._last_call = time.time()

    def track(self, tracking_number):
        if not DHL_API_KEY:
            return normalize_result("unknown", error="DHL API key not configured")

        # Skip numbers we already failed on this run (avoids duplicate retries).
        if tracking_number in DHLTracker._failed_numbers:
            return DHLTracker._failed_numbers[tracking_number]

        # If the quota looks exhausted, stop hammering DHL for the rest of the run.
        if DHLTracker._quota_exhausted:
            return normalize_result("unknown", error="DHL quota exhausted (429)")

        self._throttle()
        try:
            resp = requests.get(
                self.TRACK_URL,
                headers={"DHL-API-Key": DHL_API_KEY},
                params={"trackingNumber": tracking_number},
                timeout=20,
            )

            if resp.status_code == 429:
                DHLTracker._consecutive_429 += 1
                logger.warning(
                    "DHL 429 (rate limit/quota) for %s; not retrying (%d in a row)",
                    tracking_number, DHLTracker._consecutive_429,
                )
                result = normalize_result("unknown", error="429 Too Many Requests")
                DHLTracker._failed_numbers[tracking_number] = result
                if DHLTracker._consecutive_429 >= DHLTracker._QUOTA_429_THRESHOLD:
                    DHLTracker._quota_exhausted = True
                    logger.warning(
                        "DHL quota appears exhausted after %d consecutive 429s; "
                        "skipping remaining DHL lookups this run.",
                        DHLTracker._consecutive_429,
                    )
                return result

            if resp.status_code == 404:
                DHLTracker._consecutive_429 = 0
                return normalize_result("not_found")

            resp.raise_for_status()
            DHLTracker._consecutive_429 = 0
            data = resp.json()

            shipments = data.get("shipments", [])
            if not shipments:
                return normalize_result("not_found")

            shipment = shipments[0]
            status_obj = shipment.get("status", {})
            status_code = status_obj.get("statusCode", "").lower()
            raw_status = status_obj.get("description", "")
            location = (status_obj.get("location", {})
                        .get("address", {})
                        .get("addressLocality", ""))

            status_map = {
                "delivered": "delivered",
                "transit": "in_transit",
                "failure": "exception",
                "pre-transit": "label_created",
                "unknown": "unknown",
            }
            status = status_map.get(status_code, "in_transit")

            delivery_date = ""
            if status == "delivered":
                ts = status_obj.get("timestamp", "")
                if ts:
                    delivery_date = ts[:10]
            elif shipment.get("estimatedTimeOfDelivery"):
                etd = shipment["estimatedTimeOfDelivery"]
                delivery_date = etd[:10] if isinstance(etd, str) else ""

            return normalize_result(status, delivery_date, location, raw_status)

        except requests.exceptions.HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if code == 404:
                return normalize_result("not_found")
            logger.error("DHL tracking error for %s: %s", tracking_number, e)
            return normalize_result("unknown", error=str(e))
        except Exception as e:
            logger.error("DHL tracking error for %s: %s", tracking_number, e)
            return normalize_result("unknown", error=str(e))

# =============================================================================
# SF Express (順豐) -- public international tracking endpoint (no API key)
# =============================================================================

class SFExpressTracker:
    """Best-effort tracking via SF Express's public international route API.

    SF Express does not offer a free keyless API with guaranteed uptime, so
    this is best-effort: on any failure we return 'unknown' (NOT 'not_found')
    so the shipment still appears in the summary instead of being dropped.
    """
    ROUTE_URL = ("https://www.sf-international.com/sf-service-owf-web/"
                 "service/integration/track/route")

    def track(self, tracking_number):
        try:
            payload = {
                "trackingType": "SFNUMBER",
                "trackingNumber": [tracking_number],
                "language": "en",
                "translate": "en",
            }
            headers = {
                **HEADERS,
                "Content-Type": "application/json",
                "Referer": "https://www.sf-international.com/",
            }
            resp = requests.post(
                self.ROUTE_URL, headers=headers, json=payload, timeout=12
            )
            if resp.status_code != 200:
                return normalize_result("unknown",
                                        error="SF Express HTTP %s" % resp.status_code)
            data = resp.json()

            routes = (data.get("result", {})
                          .get("waybillRouteResp", []))
            if not routes:
                return normalize_result("unknown", raw_status="No SF route data")

            route = routes[0]
            events = route.get("waybillRoute", []) or []
            if not events:
                return normalize_result("label_created", raw_status="Accepted")

            # Events are ordered oldest->newest; take the most recent.
            latest = events[-1]
            remark = (latest.get("remark", "") or "").strip()
            op_code = str(latest.get("opCode", "")).strip()
            accept_time = latest.get("acceptTime", "") or ""
            location = latest.get("acceptAddress", "") or ""

            text = remark.lower()
            if "delivered" in text or "signed" in text or op_code == "8000":
                status = "delivered"
            elif "out for delivery" in text or op_code == "44":
                status = "out_for_delivery"
            elif "exception" in text or "failed" in text or "返" in remark:
                status = "exception"
            elif "picked up" in text or "collected" in text or op_code == "50":
                status = "in_transit"
            else:
                status = "in_transit"

            delivery_date = ""
            if status == "delivered" and accept_time:
                delivery_date = accept_time[:10]

            return normalize_result(status, delivery_date, location, remark or "In Transit")

        except Exception as e:
            logger.error("SF Express tracking error for %s: %s", tracking_number, e)
            # Keep the row visible rather than dropping it.
            return normalize_result("unknown", error=str(e))

# =============================================================================
# Royal Mail
# =============================================================================

class RoyalMailTracker:
    def track(self, tracking_number):
        try:
            url = "https://api.royalmail.com/mailpieces/v2/%s/events" % tracking_number
            headers = {
                **HEADERS,
                "Accept": "application/json",
                "Referer": (
                    "https://www.royalmail.com/track-your-item"
                    "#/tracking-results/%s" % tracking_number
                ),
            }
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 404:
                return normalize_result("not_found")
            if resp.status_code == 200:
                data = resp.json()
                mail_pieces = data.get("mailPieces", [])
                if not mail_pieces:
                    return normalize_result("not_found")
                piece = mail_pieces[0]
                events = piece.get("events", [])
                summary = piece.get("summary", {})
                status_desc = summary.get("statusDescription", "").lower()
                raw_status = summary.get("statusDescription", "")

                if "delivered" in status_desc:
                    status = "delivered"
                elif "out for delivery" in status_desc or "with delivery" in status_desc:
                    status = "out_for_delivery"
                elif "exception" in status_desc or "returned" in status_desc:
                    status = "exception"
                elif "posted" in status_desc or "dispatched" in status_desc:
                    status = "label_created"
                elif status_desc:
                    status = "in_transit"
                else:
                    status = "in_transit"

                delivery_date = ""
                if status == "delivered" and events:
                    ts = events[0].get("eventDateTime", "")
                    if ts:
                        delivery_date = ts[:10]
                estimated = summary.get("estimatedDeliveryDate", {})
                if estimated and not delivery_date:
                    start = estimated.get("startOfEstimatedWindow", "")
                    if start:
                        delivery_date = start[:10]

                location = events[0].get("locationName", "") if events else ""
                return normalize_result(status, delivery_date, location, raw_status)

            resp2 = requests.get(
                "https://www.royalmail.com/track-your-item",
                params={"trackNumber": tracking_number},
                headers=HEADERS,
                timeout=30,
            )
            html = resp2.text
            if "delivered" in html.lower():
                return normalize_result("delivered", "", "", "Delivered")
            elif "out for delivery" in html.lower():
                return normalize_result("out_for_delivery", "", "", "Out for Delivery")
            elif "exception" in html.lower() or "returned" in html.lower():
                return normalize_result("exception", "", "", "Exception")
            elif "not found" in html.lower():
                return normalize_result("not_found")
            else:
                return normalize_result("in_transit", "", "", "In Transit")

        except Exception as e:
            logger.error("Royal Mail tracking error for %s: %s", tracking_number, e)
            return normalize_result("unknown", error=str(e))

# =============================================================================
# 17Track (universal aggregator: DPD, UniUni, 1ST, 4PX, and others)
# =============================================================================
class SeventeenTrackTracker:
    REGISTER_URL = "https://api.17track.net/track/v2.2/register"
    INFO_URL = "https://api.17track.net/track/v2.2/gettrackinfo"
    STATUS_MAP_17 = {
        "Delivered": "delivered", "OutForDelivery": "out_for_delivery",
        "InTransit": "in_transit", "AvailableForPickup": "in_transit",
        "InfoReceived": "label_created", "DeliveryFailure": "exception",
        "Exception": "exception", "NotFound": "not_found", "Expired": "unknown",
    }

    def _headers(self):
        return {"17token": SEVENTEENTRACK_API_KEY, "Content-Type": "application/json"}

    def track(self, tracking_number):
        if not SEVENTEENTRACK_API_KEY:
            return normalize_result("unknown", error="17track API key not configured")
        try:
            body = [{"number": tracking_number}]
            try:
                requests.post(self.REGISTER_URL, headers=self._headers(), json=body, timeout=20)
            except Exception as e:
                logger.warning("17track register failed for %s: %s", tracking_number, e)
            resp = requests.post(self.INFO_URL, headers=self._headers(), json=body, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            accepted = data.get("data", {}).get("accepted", [])
            if not accepted:
                return normalize_result("unknown", raw_status="17track: no data yet")
            info = accepted[0].get("track_info", {}) or {}
            latest_status = info.get("latest_status", {}) or {}
            raw_code = latest_status.get("status", "")
            status = self.STATUS_MAP_17.get(raw_code, "in_transit")
            latest_event = info.get("latest_event", {}) or {}
            raw_status = latest_event.get("description", "") or raw_code
            location = latest_event.get("location", "") or ""
            metrics = info.get("time_metrics", {}) or {}
            edd = metrics.get("estimated_delivery_date", {}) or {}
            delivery_date = (edd.get("to") or edd.get("from") or "")[:10]
            if status == "delivered" and latest_event.get("time_iso"):
                delivery_date = latest_event["time_iso"][:10]
            return normalize_result(status, delivery_date, location, raw_status)
        except Exception as e:
            logger.error("17track tracking error for %s: %s", tracking_number, e)
            return normalize_result("unknown", error=str(e))


# =============================================================================
# Unified Tracker
# =============================================================================

class CarrierTracker:
    def __init__(self):
        self.fedex = FedExTracker()
        self.ups = UPSTracker()
        self.usps = USPSTracker()
        self.dhl = DHLTracker()
        self.royalmail = RoyalMailTracker()
        self.sfexpress = SFExpressTracker()
        self.seventeentrack = SeventeenTrackTracker()
        self._clients = {
            "fedex": self.fedex,
            "ups": self.ups,
            "usps": self.usps,
            "dhl": self.dhl,
            "royalmail": self.royalmail,
            "sfexpress": self.sfexpress,
            "dpd": self.seventeentrack,
            "uniuni": self.seventeentrack,
            "first": self.seventeentrack,
            "fourpx": self.seventeentrack,
        }

    def track(self, tracking_number, carrier):
        client = self._clients.get(carrier)
        if not client:
            logger.warning("Unknown carrier '%s' for tracking %s",
                           carrier, tracking_number)
            return normalize_result("unknown",
                                    error="Unsupported carrier: %s" % carrier)
        logger.info("Tracking %s via %s", tracking_number, carrier.upper())
        return client.track(tracking_number)
