import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import card_lint
import shipping_summary
from lark_client import LarkClient


class SummaryTests(unittest.TestCase):
    def test_unconfigured(self):
        with patch.dict(os.environ, {"FULFILLMENT_CONFIG": ""}):
            self.assertIn("awaiting setup", shipping_summary.snapshot(Mock()))

    def test_counts_and_failure(self):
        store = Mock()
        store.shipments.return_value = [{"status": s} for s in
                                       ("Packed", "Packed", "Shipped", "Received", "Cancelled")]
        with patch("fulfillment_web.configured_service", return_value=SimpleNamespace(store=store)):
            self.assertIn("Packed: 2 · Shipped: 1 · Received: 1", shipping_summary.snapshot(Mock()))
            store.shipments.side_effect = RuntimeError("unavailable")
            self.assertIn("not reported as zero", shipping_summary.snapshot(Mock()))

    def test_card_links_and_lint(self):
        with patch("dashboard.lark_link", side_effect=lambda **kw: "https://example.com/dashboard" +
                   ("?status=all" if kw else "")):
            card = shipping_summary.build_card([], 0, "Packed: 2")
        self.assertEqual([], card_lint.lint_v2(card))
        self.assertIn("Legacy tracker", str(card))
        self.assertIn("?status=all", str(card))

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
