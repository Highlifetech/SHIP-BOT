"""Local UI test harness. Never mounted on the production webhook server.

python fulfillment_demo.py -> http://127.0.0.1:8787/fulfillment
Synthetic records, simulated Lark saves, no credentials and no external writes.
"""
import tempfile
from flask import Flask
from fulfillment import Coordinator, FulfillmentService
import dashboard
from types import SimpleNamespace
from test_fulfillment import MemoryStore

app = Flask(__name__)
store = MemoryStore()
scratch = tempfile.TemporaryDirectory(prefix="shipbot-pick-pack-test-")
settings = {"warehouse_address": "OFF MENU — TEST RECEIVING\n100 Example Street\nTest City, NY 10001\nUnited States (sample only)", "demo": True}
store.rows.append(dict(store.rows[0], key="tblOne:recC", record_id="recC", order="SO-1050",
                       product="120 pins — quantity not verified", opening_china=None, ready=False))
service = FulfillmentService(store, settings, Coordinator(scratch.name))
dashboard.DASHBOARD_TOKEN = "local-demo"
dashboard._authorized = lambda: True
dashboard.current_user = lambda: {"name": "Local test", "open_id": "not-a-live-user"}
chat = SimpleNamespace(_SNAPSHOT={"results": [], "ts": 0}, update_snapshot=lambda results: None)
dashboard.register(app, chat, lambda **kwargs: [], None, fulfillment_service=service)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8787, debug=False)
