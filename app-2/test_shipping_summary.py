import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import card_lint
import shipping_summary
from lark_client import LarkClient


class SummaryTests(unittest.TestCase):
    def test_catalog_setup_cannot_enable_writes_or_invent_history(self):
        import json
        from fulfillment_web import configured_service
        config = {"catalog_only": True, "base_token": "testbase",
                  "sources": [{"table_id": "orders"}]}
        with patch.dict(os.environ, {"FULFILLMENT_CONFIG": json.dumps(config),
                        "FULFILLMENT_STATE_DIR": "/must-not-create", "FULFILLMENT_SINGLE_REPLICA": "1"}):
            service = configured_service(Mock())
            self.assertIsNone(service.coordinator)
            self.assertEqual([], service.store.shipments())
            # Setup states say nothing on the team card -- but the invariant
            # that matters is unchanged: never report a count it doesn't have.
            self.assertEqual("", shipping_summary.snapshot(Mock()))

    def test_unconfigured(self):
        with patch.dict(os.environ, {"FULFILLMENT_CONFIG": ""}):
            self.assertEqual("", shipping_summary.snapshot(Mock()))

    def test_counts_and_failure(self):
        store = Mock()
        store.shipments.return_value = [{"status": s} for s in
                                       ("Packed", "Packed", "Shipped", "Received", "Cancelled")]
        with patch("fulfillment_web.configured_service", return_value=SimpleNamespace(store=store, settings={})):
            line = shipping_summary.snapshot(Mock())
            self.assertIn("2 packed", line)
            self.assertIn("1 shipped", line)
            self.assertIn("1 received", line)
            self.assertNotIn("\n", line)      # one line, not a paragraph
            # Nothing to report says nothing, and never invents a zero.
            store.shipments.return_value = []
            self.assertEqual("", shipping_summary.snapshot(Mock()))
            store.shipments.side_effect = RuntimeError("unavailable")
            self.assertEqual("", shipping_summary.snapshot(Mock()))

    def test_card_links_and_lint(self):
        with patch("dashboard.lark_link", side_effect=lambda **kw: "https://example.com/dashboard" +
                   ("?status=all" if kw else "")):
            card = shipping_summary.build_card([], 0, "Packed: 2")
        self.assertEqual([], card_lint.lint_v2(card))
        self.assertIn("Open shipping workspace", str(card))
        # One app, so one button and no "legacy vs new" caveat to explain.
        self.assertNotIn("Legacy", str(card))

        # With nothing from pick & pack, the card is just the tracker: no
        # preamble, no divider, no apology about overlapping totals.
        with patch("dashboard.lark_link", return_value="https://example.com/d"):
            bare = shipping_summary.build_card([], 0, "")
        self.assertEqual([], card_lint.lint_v2(bare))
        self.assertNotIn("Pick & pack", str(bare))
        self.assertNotIn("Legacy", str(bare))

    def test_empty_legacy_still_sends_new_summary_and_text_fallback(self):
        client = LarkClient.__new__(LarkClient)
        client._send_card = Mock()
        client._send_text = Mock()
        with patch.dict(os.environ, {"SHIPPING_SUMMARY_LAYOUT": "new"}), \
             patch("shipping_summary.snapshot", return_value="Packed: 2"):
            client.send_daily_summary([], chat_id="existing-chat", message_id="existing-message")
            self.assertEqual("existing-chat", client._send_card.call_args.args[1])
            self.assertIn("Packed: 2", client._send_card.call_args.kwargs["card_json"])
            client._send_card.side_effect = RuntimeError("rejected")
            client.send_daily_summary([], chat_id="existing-chat", message_id="existing-message")
            self.assertTrue(client._send_text.call_args.args[0].startswith("Packed: 2"))
            self.assertEqual(("existing-chat", "existing-message"), client._send_text.call_args.args[1:])


if __name__ == "__main__":
    unittest.main()
