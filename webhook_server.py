— @mention trigger for the shipment tracker bot.

When someone @mentions the bot in the HLT INBOUND DELIVERIES group chat,
Lark sends a POST to this server's /webhook endpoint.  The server:
  1. Responds to the URL-verification challenge (one-time setup).
  2. On a real message event, runs the full tracker and sends the summary
                                                      back to the same chat, threaded under the triggering message.

Deploy this on any server/cloud that can receive HTTPS traffic and set
the Lark bot's "Event Subscription" URL to:
    https://<your-server>/webhook

Required env vars (same as the GitHub Actions secrets):
    LARK_APP_ID, LARK_APP_SECRET, LARK_BASE_URL,
    LARK_CHAT_ID, LARK_SHEET_TOKENS, LARK_SHEET_OWNERS,
    LARK_VERIFICATION_TOKEN   (from Lark bot Event Subscription settings)
    FEDEX_API_KEY, FEDEX_SECRET_KEY,
    UPS_CLIENT_ID, UPS_CLIENT_SECRET,
    DHL_API_KEY

Run locally for testing:
    pip install flask
    python webhook_server.py
           """

import hashlib
import hmac
import json
import logging
import os
import threading

from flask import Flask, jsonify, request

from main import run_tracker

logging.basicConfig(
    level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        )
        logger = logging.getLogger(__name__)

        app = Flask(__name__)

        # ── Lark event verification token (from the Lark developer console) ───────────
        LARK_VERIFICATION_TOKEN = os.environ.get("LARK_VERIFICATION_TOKEN", "")
        # Optional: encrypt key for v2 event signature verification
        LARK_ENCRYPT_KEY = os.environ.get("LARK_ENCRYPT_KEY", "")


        # ─────────────────────────────────────────────────────────────────────────────
        # Helpers
        # ─────────────────────────────────────────────────────────────────────────────

        def _is_mention_event(body: dict) -> bool:
            """Return True if this is a message that @mentions the bot."""
                event = body.get("event", {})
                    msg   = event.get("message", {})
                        mentions = msg.get("mentions", [])
                            # Lark puts @mentioned users in the 'mentions' list; at least one must be present.
                                # We also accept any message sent directly to the bot (direct message).
                                    if mentions:
                                            return True
                                                # Fallback: message type is "text" and contains "@"
                                                    try:
                                                            content = json.loads(msg.get("content", "{}"))
                                                                    text = content.get("text", "")
                                                                            return "@" in text
                                                                                except Exception:
                                                                                        return False


                                                                                        def _run_and_reply(chat_id: str, message_id: str):
                                                                                            """Run the tracker in a background thread and send summary back to the chat."""
                                                                                                try:
                                                                                                        logger.info(f"@mention trigger: chat={chat_id} message={message_id}")
                                                                                                                run_tracker(
                                                                                                                            dry_run=False,
                                                                                                                                        chat_id=chat_id,
                                                                                                                                                    message_id=message_id,
                                                                                                                                                            )
                                                                                                                                                                except Exception as e:
                                                                                                                                                                        logger.error(f"Error during @mention-triggered run: {e}")
                                                                                                                                                                        
                                                                                                                                                                        
                                                                                                                                                                        # ─────────────────────────────────────────────────────────────────────────────
                                                                                                                                                                        # Webhook endpoint
                                                                                                                                                                        # ─────────────────────────────────────────────────────────────────────────────
                                                                                                                                                                        
                                                                                                                                                                        @app.route("/webhook", methods=["POST"])
                                                                                                                                                                        def webhook():
                                                                                                                                                                            body = request.get_json(force=True, silent=True) or {}
                                                                                                                                                                                logger.info(f"Received webhook: type={body.get('type')} header={body.get('header', {}).get('event_type')}")
                                                                                                                                                                                
                                                                                                                                                                                    # ── 1. URL verification challenge (one-time during bot setup) ────────────
                                                                                                                                                                                        if body.get("type") == "url_verification":
                                                                                                                                                                                                challenge = body.get("challenge", "")
                                                                                                                                                                                                        token     = body.get("token", "")
                                                                                                                                                                                                                if LARK_VERIFICATION_TOKEN and token != LARK_VERIFICATION_TOKEN:
                                                                                                                                                                                                                            logger.warning("Verification token mismatch — ignoring")
                                                                                                                                                                                                                                        return jsonify({"error": "invalid token"}), 403
                                                                                                                                                                                                                                                logger.info("URL verification challenge answered")
                                                                                                                                                                                                                                                        return jsonify({"challenge": challenge})
                                                                                                                                                                                                                                                        
                                                                                                                                                                                                                                                            # ── 2. Token / signature check for real events ───────────────────────────
                                                                                                                                                                                                                                                                header = body.get("header", {})
                                                                                                                                                                                                                                                                    token  = header.get("token", "") or body.get("token", "")
                                                                                                                                                                                                                                                                        if LARK_VERIFICATION_TOKEN and token != LARK_VERIFICATION_TOKEN:
                                                                                                                                                                                                                                                                                logger.warning("Event token mismatch — ignoring")
                                                                                                                                                                                                                                                                                        return jsonify({"error": "invalid token"}), 403
                                                                                                                                                                                                                                                                                        
                                                                                                                                                                                                                                                                                            # ── 3. Handle message events ─────────────────────────────────────────────
                                                                                                                                                                                                                                                                                                event_type = header.get("event_type", "") or body.get("event", {}).get("type", "")
                                                                                                                                                                                                                                                                                                
                                                                                                                                                                                                                                                                                                    if event_type == "im.message.receive_v1" or event_type == "message":
                                                                                                                                                                                                                                                                                                            if _is_mention_event(body):
                                                                                                                                                                                                                                                                                                                        event      = body.get("event", {})
                                                                                                                                                                                                                                                                                                                                    msg        = event.get("message", {})
                                                                                                                                                                                                                                                                                                                                                message_id = msg.get("message_id", "")
                                                                                                                                                                                                                                                                                                                                                            chat       = event.get("message", {}).get("chat_id", "") \
                                    or event.get("chat_id", "")

                                                if not chat:
                                                                # Try sender context
                                                                                chat = event.get("sender", {}).get("sender_id", {}).get("chat_id", "")

                                                                                            logger.info(f"@mention detected in chat={chat} msg={message_id} — triggering tracker")

                                                                                                        # Run tracker in background so we can return 200 immediately
                                                                                                                    # (Lark will retry if it doesn't get a 200 within ~3 s)
                                                                                                                                t = threading.Thread(
                                                                                                                                                target=_run_and_reply,
                                                                                                                                                                args=(chat, message_id),
                                                                                                                                                                                daemon=True,
                                                                                                                                                                                            )
                                                                                                                                                                                                        t.start()
                                                                                                                                                                                                        
                                                                                                                                                                                                                return jsonify({"code": 0}), 200
                                                                                                                                                                                                                
                                                                                                                                                                                                                    # ── 4. All other event types — acknowledge and ignore ────────────────────
                                                                                                                                                                                                                        return jsonify({"code": 0}), 200
                                                                                                                                                                                                                        
                                                                                                                                                                                                                        
                                                                                                                                                                                                                        # ─────────────────────────────────────────────────────────────────────────────
                                                                                                                                                                                                                        # Health check
                                                                                                                                                                                                                        # ─────────────────────────────────────────────────────────────────────────────
                                                                                                                                                                                                                        
                                                                                                                                                                                                                        @app.route("/health", methods=["GET"])
                                                                                                                                                                                                                        def health():
                                                                                                                                                                                                                            return jsonify({"status": "ok"}), 200
                                                                                                                                                                                                                            
                                                                                                                                                                                                                            
                                                                                                                                                                                                                            # ─────────────────────────────────────────────────────────────────────────────
                                                                                                                                                                                                                            # Entry point
                                                                                                                                                                                                                            # ─────────────────────────────────────────────────────────────────────────────
                                                                                                                                                                                                                            
                                                                                                                                                                                                                            if __name__ == "__main__":
                                                                                                                                                                                                                                port = int(os.environ.get("PORT", 8080))
                                                                                                                                                                                                                                    logger.info(f"Starting webhook server on port {port}")
                                                                                                                                                                                                                                        app.run(host="0.0.0.0", port=port, debug=False)
