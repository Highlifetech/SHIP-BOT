"""
Ship-Bot web dashboard.

The Lark chat card stays a summary -- counts, what needs attention, per-client
totals -- but a message card can't hold a search box, per-row buttons or
collapsible groups. Those live here: a single self-contained page served off
the same Railway service, reading the same scan snapshot the card reads.

Routes (all mounted on the existing Flask app):

    GET  /dashboard            the page (token-gated)
    GET  /api/shipments        JSON payload; ?refresh=1 forces a live scan
    POST /api/mark-delivered   writes Delivered + today's date to the sheet
    GET  /dashboard/health     unauthenticated liveness check

Auth is a shared token (DASHBOARD_TOKEN) carried in ?t=. That is the right
weight for an internal tool behind a link the team pastes in Lark: no login to
maintain, nothing sensitive beyond shipment status, and the token can be
rotated by changing one Railway variable. If DASHBOARD_TOKEN is unset the
dashboard refuses to serve rather than defaulting to open.
"""

import os
import json
import time
import hmac
import logging
from urllib.parse import quote

from flask import request, jsonify, Response, abort

import card_builder

logger = logging.getLogger(__name__)

DASHBOARD_TOKEN = os.environ.get("DASHBOARD_TOKEN", "").strip()
PUBLIC_URL = os.environ.get("DASHBOARD_URL", "").strip()
SNAPSHOT_STALE_SECONDS = int(os.environ.get("DASHBOARD_STALE_SECONDS", "1800"))

_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "dashboard.html")
_html_cache = {"mtime": 0, "body": ""}


def dashboard_url():
    """Public link to the dashboard, or '' when it isn't configured."""
    if not PUBLIC_URL or not DASHBOARD_TOKEN:
        return ""
    base = PUBLIC_URL.rstrip("/")
    if not base.endswith("/dashboard"):
        base += "/dashboard"
    return "%s?t=%s" % (base, DASHBOARD_TOKEN)


# Where the card's button lands.
#
# "browser" is the default because it is the only one that behaves the same
# everywhere. Lark's AppLink wrapper sounds better than it is: per Lark's own
# docs, "when AppLink is clicked on a PC, it will always first open a browser
# page and then attempt to open Lark" -- so on desktop you get a browser tab
# and a hand-off prompt, which reads as broken. The clean in-Lark experience
# needs the dashboard registered as a web app in the Developer Console, not
# an AppLink.
#
#   browser       plain URL, opens in the default browser (default)
#   sidebar-semi  AppLink: beside the chat -- reliable on mobile, not on PC
#   window        AppLink: its own Lark window
#   appCenter     AppLink: a tab in the Lark nav bar (Lark 7.5+)
OPEN_MODE = os.environ.get("DASHBOARD_OPEN_MODE", "browser").strip()
APPLINK = "https://applink.larksuite.com/client/web_url/open"


def lark_link(mode=None):
    """The URL the card button should open.

    Wrapped in Lark's AppLink so the dashboard opens inside Lark -- in the
    chat sidebar by default -- instead of kicking the user out to a browser.
    Requires Lark 3.41+ (appCenter mode needs 7.5+); older clients and the
    'browser' mode get the plain URL.
    """
    target = dashboard_url()
    if not target:
        return ""
    mode = (mode or OPEN_MODE).strip()
    if mode == "browser":
        return target
    link = "%s?mode=%s&url=%s" % (APPLINK, quote(mode, safe=""),
                                  quote(target, safe=""))
    if mode == "window":
        link += "&width=1280&height=860"
    return link


def _authorized():
    if not DASHBOARD_TOKEN:
        return False
    given = (request.args.get("t") or
             request.headers.get("X-Dashboard-Token") or "")
    return hmac.compare_digest(given, DASHBOARD_TOKEN)


def _page():
    try:
        mtime = os.path.getmtime(_HTML_PATH)
    except OSError:
        return "<h1>dashboard.html is missing</h1>"
    if mtime != _html_cache["mtime"]:
        with open(_HTML_PATH, "r", encoding="utf-8") as f:
            _html_cache["body"] = f.read()
        _html_cache["mtime"] = mtime
    return _html_cache["body"]


def register(app, chat, run_tracker, lark):
    """Attach the dashboard routes to the Flask app.

    Takes its dependencies as arguments rather than importing webhook_server,
    so this module stays importable on its own and easy to test.
    """

    def _snapshot(force=False):
        results = chat._SNAPSHOT.get("results") or []
        age = time.time() - (chat._SNAPSHOT.get("ts") or 0)
        if force or not results or age > SNAPSHOT_STALE_SECONDS:
            try:
                results = run_tracker(dry_run=True)   # read-only: no writes
                chat.update_snapshot(results)
            except Exception as e:
                logger.error("Dashboard scan failed: %s", e)
                results = chat._SNAPSHOT.get("results") or []
        return results

    @app.route("/dashboard", methods=["GET"])
    def dashboard_page():
        if not DASHBOARD_TOKEN:
            return Response(
                "Dashboard is not configured. Set DASHBOARD_TOKEN on the "
                "Railway service to switch it on.", status=503,
                mimetype="text/plain")
        if not _authorized():
            abort(403)
        return Response(_page(), mimetype="text/html")

    @app.route("/api/shipments", methods=["GET"])
    def api_shipments():
        if not _authorized():
            abort(403)
        force = request.args.get("refresh") == "1"
        results = _snapshot(force=force)
        sheets = len({(r.get("sheet_token") or "") for r in results})
        return jsonify(card_builder.dashboard_payload(results,
                                                      sheet_count=sheets))

    @app.route("/api/mark-delivered", methods=["POST"])
    def api_mark_delivered():
        if not _authorized():
            abort(403)
        body = request.get_json(silent=True) or {}
        target = card_builder.parse_delivered_value(body.get("handle", ""))
        if not target:
            return jsonify({"ok": False, "error": "Unrecognized shipment"}), 400
        try:
            when = lark.mark_delivered(target["sheet_token"], target["tab"],
                                       target["row_num"])
        except Exception as e:
            logger.error("Dashboard mark_delivered failed for %s: %s", target, e)
            return jsonify({"ok": False, "error": str(e)[:160]}), 502

        for r in (chat._SNAPSHOT.get("results") or []):
            if ((r.get("sheet_token") or "") == target["sheet_token"]
                    and r.get("row_num") == target["row_num"]):
                r["new_status"] = "Delivered"
                r["raw_status"] = "Marked delivered from the dashboard"
                r["delivery_date"] = when
        return jsonify({"ok": True, "delivered_on": when})

    @app.route("/dashboard/health", methods=["GET"])
    def dashboard_health():
        return jsonify({
            "configured": bool(DASHBOARD_TOKEN),
            "public_url_set": bool(PUBLIC_URL),
            "snapshot_rows": len(chat._SNAPSHOT.get("results") or []),
        })

    logger.info("Dashboard routes registered (configured=%s, url=%s)",
                bool(DASHBOARD_TOKEN), PUBLIC_URL or "(unset)")
