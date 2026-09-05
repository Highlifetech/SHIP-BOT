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

from flask import request, jsonify, Response, abort, redirect

import card_builder
import config
import lark_auth
import packing_list
import shipment_create
import fulfillment_web
from background_sync import BackgroundSnapshot

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
# There are two AppLink protocols and they behave nothing alike. The one we
# tried first, client/web_url/open, wraps an arbitrary URL, and per Lark's own
# docs "when AppLink is clicked on a PC, it will always first open a browser
# page and then attempt to open Lark" -- a browser tab plus a hand-off prompt,
# which reads as broken. The right one is client/web_app/open, which opens a
# web app registered in the Developer Console *inside* Lark. It takes the
# app's own appId, and Lark rejects a link whose appId isn't this app -- which
# is also why pasting a plain https URL into a shortcut field fails with
# "make sure the URL format and the app connected are correct".
#
#   web_app   AppLink into the registered web app, in Lark (default when
#             LARK_APP_ID is set)
#   browser   plain URL, opens in the default browser (the fallback)
#
# DASHBOARD_OPEN_TARGET picks the container: sidebar (beside the chat),
# window (its own Lark window), or appCenter (a Workplace tab).
OPEN_MODE = os.environ.get("DASHBOARD_OPEN_MODE", "web_app").strip()
OPEN_TARGET = os.environ.get("DASHBOARD_OPEN_TARGET", "sidebar").strip()
APP_ID = os.environ.get("LARK_APP_ID", "").strip()
APPLINK_WEB_APP = "https://applink.larksuite.com/client/web_app/open"


def lark_link(mode=None, status=""):
    """The URL a card button should open.

    An AppLink into the registered web app when we have an app id, so the
    dashboard opens inside Lark rather than kicking the user to a browser;
    the plain link otherwise.

    `status` carries the card's own bucket through to the board, so a chip
    that says "8 Attention" in Lark opens those same eight shipments here --
    the card and the dashboard stay one click apart instead of being two
    views of the same data that you have to re-filter by hand.
    """
    target = dashboard_url()
    if not target:
        return ""
    if status:
        target += ("&" if "?" in target else "?") + "status=" + quote(status)
    mode = (mode or OPEN_MODE).strip()
    if mode != "web_app" or not APP_ID:
        return target

    link = "%s?appId=%s&mode=%s" % (APPLINK_WEB_APP, quote(APP_ID, safe=""),
                                    quote(OPEN_TARGET, safe=""))
    # `path` can't carry ? or & -- Lark says so explicitly -- so anything with
    # a query string has to go through lk_target_url instead.
    if lark_auth.configured() and not status:
        link += "&path=dashboard"
    else:
        link += "&lk_target_url=" + quote(target, safe="")
    return link


def current_user():
    """The signed-in Lark user, or None."""
    if not lark_auth.configured():
        return None
    return lark_auth.read_session(request.cookies.get(lark_auth.SESSION_COOKIE))


def _token_ok():
    if not DASHBOARD_TOKEN:
        return False
    given = (request.args.get("t") or
             request.headers.get("X-Dashboard-Token") or "")
    return hmac.compare_digest(given, DASHBOARD_TOKEN)


def _authorized():
    """Either a Lark session or the shared link token.

    Both are accepted on purpose: the token link keeps working while the web
    app is being registered and published, and for anyone outside the tenant.
    """
    return bool(current_user()) or _token_ok()


def _callback_url():
    base = (PUBLIC_URL or request.host_url).rstrip("/")
    if base.endswith("/dashboard"):
        base = base[:-len("/dashboard")]
    return base + "/auth/lark/callback"


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


def register(app, chat, run_tracker, lark, fulfillment_service=None):
    """Attach the dashboard routes to the Flask app.

    Takes its dependencies as arguments rather than importing webhook_server,
    so this module stays importable on its own and easy to test.
    """

    fulfillment_web.register(app, lark, _authorized, current_user, service=fulfillment_service)

    def read_tracking_dashboard():
        results = run_tracker(dry_run=True)
        chat.update_snapshot(results)
        return card_builder.dashboard_payload(results, sheet_count=len({r.get('sheet_token', '') for r in results}))

    tracking_sync = BackgroundSnapshot(read_tracking_dashboard, 300)
    if fulfillment_service is None and os.environ.get('FULFILLMENT_CONFIG'):
        tracking_sync.start()
    app.extensions['shipping_tracking_sync'] = tracking_sync

    @app.get('/api/shipping-workspace/tracking')
    def workspace_tracking():
        if not _authorized():
            abort(403)
        tracking_sync.start()
        snapshot = tracking_sync.snapshot()
        data = snapshot.pop('data')
        if data is None:
            data = card_builder.dashboard_payload(chat._SNAPSHOT.get('results') or [], sheet_count=0)
        return jsonify(data=data, sync=snapshot)

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
    @app.route("/dashboard/legacy", methods=["GET"])
    def dashboard_page():
        if not DASHBOARD_TOKEN and not lark_auth.configured():
            return Response(
                "Dashboard is not configured. Set DASHBOARD_TOKEN (or "
                "LARK_SSO=1) on the Railway service to switch it on.",
                status=503, mimetype="text/plain")
        if not _authorized():
            # Inside Lark this round-trip is silent -- the client is already
            # signed in, so Lark redirects straight back with a code.
            if lark_auth.configured():
                return redirect(lark_auth.login_url(_callback_url()))
            abort(403)
        # Preserve the previous tracker at an explicit route. Old notification
        # links carrying tracker filters still land on the screen they describe.
        legacy = (request.path.endswith("/legacy") or request.args.get("status")
                  or os.environ.get("SHIPPING_LAYOUT") == "legacy")
        if legacy:
            return Response(_page(), mimetype="text/html")
        return Response(fulfillment_web.page_html(), mimetype="text/html",
                        headers={"Cache-Control": "no-store", "Referrer-Policy": "same-origin"})

    @app.route("/auth/lark/callback", methods=["GET"])
    def lark_callback():
        if not lark_auth.configured():
            abort(404)
        code = request.args.get("code", "")
        if not code:
            return Response("Lark did not return an authorization code.",
                            status=400, mimetype="text/plain")
        try:
            user = lark_auth.sign_in(code, _callback_url())
        except Exception as e:
            logger.error("Lark sign-in failed: %s", e)
            return Response("Sign-in failed: %s" % str(e)[:200], status=502,
                            mimetype="text/plain")
        logger.info("Lark sign-in: %s (%s)", user.get("name"),
                    user.get("open_id", "")[:12])
        resp = redirect((PUBLIC_URL.rstrip("/") if PUBLIC_URL else "")
                        + "/dashboard")
        resp.set_cookie(lark_auth.SESSION_COOKIE,
                        lark_auth.make_session(user),
                        max_age=lark_auth.SESSION_TTL, httponly=True,
                        secure=True, samesite="Lax")
        return resp

    @app.route("/packing-list", methods=["GET"])
    def packing_list_page():
        """A shipment's packing lists, in Off Menu's own template.

        Generates the whole set by default -- an outside-box label and an
        inside-box packing list for every box -- so one Print runs the
        shipment. ?box=2 narrows it to one; ?format=xlsx returns the workbook
        instead of the page.
        """
        if not _authorized():
            if lark_auth.configured():
                return redirect(lark_auth.login_url(_callback_url()))
            abort(403)
        tracking = request.args.get("tracking", "").strip()
        if not tracking:
            return Response("No tracking number given.", status=400,
                            mimetype="text/plain")
        doc = packing_list.gather(_snapshot(), tracking)
        if not doc:
            return Response("No shipment found for %s. It may have been "
                            "delivered and closed out." % tracking,
                            status=404, mimetype="text/plain")

        try:
            box = int(request.args.get("box", "") or 0) or None
        except ValueError:
            box = None
        if box and box not in [b["num"] for b in doc["boxes"]]:
            return Response("This shipment has %d box(es); there is no box %d."
                            % (doc["box_count"], box), status=404,
                            mimetype="text/plain")

        if request.args.get("format") == "xlsx":
            try:
                blob = packing_list.workbook(doc)
            except Exception as e:
                logger.error("Packing-list xlsx failed for %s: %s", tracking, e)
                return Response("Could not build the workbook: %s" % str(e)[:200],
                                status=500, mimetype="text/plain")
            return Response(blob, mimetype=(
                "application/vnd.openxmlformats-officedocument."
                "spreadsheetml.sheet"), headers={
                "Content-Disposition": 'attachment; filename="%s"'
                                       % packing_list.filename(doc)})

        who = (current_user() or {}).get("name", "")
        token = request.args.get("t", "") or DASHBOARD_TOKEN
        return Response(packing_list.render(doc, viewer=who, token=token,
                                            box=box),
                        mimetype="text/html")

    # ------------------------------------------------- creating a shipment
    #
    # The dashboard stops being a tracker here. Open order lines come from the
    # same sheets the tracker reads -- anything ordered with quantity still
    # outstanding -- because the client portal's sales orders have no API yet.

    _orders = {"ts": 0, "rows": []}

    def _order_rows(force=False):
        age = time.time() - _orders["ts"]
        if not force and _orders["rows"] and age < SNAPSHOT_STALE_SECONDS:
            return _orders["rows"]
        rows = []
        seen = set()
        for r in _snapshot():
            token, tab = r.get("sheet_token") or "", r.get("tab") or ""
            if not token or (token, tab) in seen:
                continue
            seen.add((token, tab))
            try:
                sheet_id = shipment_create._resolve_sheet_id(lark, token, tab)
                for row in lark.read_order_rows(token, sheet_id):
                    row["tab"] = tab
                    row["section"] = r.get("section") or tab
                    rows.append(row)
            except Exception as e:
                logger.error("Order scan failed for %s/%s: %s", token[:10], tab, e)
        if rows:
            _orders["rows"], _orders["ts"] = rows, time.time()
        return rows or _orders["rows"]

    @app.route("/api/open-orders", methods=["GET"])
    def api_open_orders():
        if not _authorized():
            abort(403)
        rows = _order_rows(force=request.args.get("refresh") == "1")
        return jsonify({
            "orders": shipment_create.open_orders(rows),
            "boards": shipment_create.boards(rows),
            "clients": shipment_create.clients(rows),
        })

    @app.route("/api/shipments", methods=["POST"])
    def api_create_shipment():
        if not _authorized():
            abort(403)
        payload = request.get_json(silent=True) or {}
        rows = _order_rows()
        columns = config.columns_for(payload.get("sheet_token", ""))

        if payload.get("dry_run"):
            problems = shipment_create.validate(
                payload, available={o["key"]: o["remaining"]
                                    for o in shipment_create.open_orders(rows)})
            work = shipment_create.plan(payload, rows, columns)
            unmapped = [f for f in ("box_num", "ship_to", "invoice_num",
                                    "ship_date", "ddp", "product_name")
                        if not str(columns.get(f) or "").strip()]
            return jsonify({"ok": not problems, "problems": problems,
                            "preview": work["preview"],
                            "start_row": work["start_row"],
                            "unmapped_columns": unmapped})

        try:
            made = shipment_create.create(lark, rows, payload, columns)
        except ValueError as e:
            return jsonify({"ok": False, "error": str(e)}), 400
        except Exception as e:
            logger.error("Create shipment failed: %s", e)
            return jsonify({"ok": False, "error": str(e)[:200]}), 502

        who = (current_user() or {}).get("name", "")
        logger.info("Shipment %s created by %s", made["tracking"], who or "link user")
        _orders["ts"] = 0          # the sheet moved; re-read before the next one
        chat._SNAPSHOT["ts"] = 0
        made["ok"] = True
        made["packing_list"] = "/packing-list?tracking=%s" % quote(made["tracking"])
        return jsonify(made)

    @app.route("/api/me", methods=["GET"])
    def api_me():
        if not _authorized():
            abort(403)
        user = current_user()
        return jsonify({"signed_in": bool(user),
                        "name": (user or {}).get("name", ""),
                        "open_id": (user or {}).get("open_id", "")})

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
                who = (current_user() or {}).get("name", "")
                r["raw_status"] = ("Marked delivered by %s" % who if who
                                   else "Marked delivered from the dashboard")
                r["delivery_date"] = when
        return jsonify({"ok": True, "delivered_on": when})

    @app.route("/dashboard/health", methods=["GET"])
    def dashboard_health():
        try:
            import webhook_server
            build = getattr(webhook_server, "BUILD_ID", "unknown")
        except Exception:
            build = "unknown"
        try:
            import card_lint
            lint_present = True
        except Exception:
            lint_present = False
        return jsonify({
            "build": build,
            "card_lint_deployed": lint_present,
            "lark_sso": lark_auth.configured(),
            "configured": bool(DASHBOARD_TOKEN),
            "public_url_set": bool(PUBLIC_URL),
            "snapshot_rows": len(chat._SNAPSHOT.get("results") or []),
        })

    logger.info("Dashboard routes registered (configured=%s, url=%s)",
                bool(DASHBOARD_TOKEN), PUBLIC_URL or "(unset)")
