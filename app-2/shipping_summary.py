"""Read-only pick/pack snapshot for the existing scheduled Lark report.

The card opens with whatever this returns, which makes it expensive space: a
line here pushes the first real shipment further down, and the top of the card
is the part people actually read in a chat.

So the rule is that this says nothing unless it has something to say. Three
zeroes plus a paragraph explaining what the three zeroes would have meant is
not something to say -- it was spending four lines and a divider to tell
nobody anything, before a single shipment appeared.
"""
import logging


def snapshot(lark):
    """A one-line pick/pack status, or "" when there is nothing to report."""
    from fulfillment_web import configured_service
    try:
        service = configured_service(lark)
        if service is None or service.settings.get("catalog_only"):
            # Setup states concern whoever is doing the setup. They do not
            # belong at the top of the whole team's twice-daily card.
            return ""
        # Only read saved manifests; never create reservations or query carriers.
        shipments = service.store.shipments()
        counts = {status: sum(s["status"] == status for s in shipments)
                  for status in ("Packed", "Shipped", "Received")}
        if not any(counts.values()):
            return ""
        bits = ["%d %s" % (n, name.lower()) for name, n in counts.items() if n]
        return "**Pick & pack** · " + " · ".join(bits)
    except Exception:
        logging.getLogger(__name__).warning("Pick/pack summary data unavailable")
        return ""


def build_card(results, sheet_count, summary):
    """The tracker card, with the pick/pack line on top when there is one."""
    import card_builder
    import dashboard

    card = card_builder.build_tracker_card_v2(results, sheet_count=sheet_count)
    card["header"]["title"]["content"] = "\U0001F4E6 Shipping Tracker"

    if not summary:
        # Nothing from pick & pack, so no preamble and no "legacy" caveat.
        # With one system reporting there is nothing to disambiguate, and the
        # card is simply the tracker.
        return card

    head = [{"tag": "markdown", "content": summary}]
    url = dashboard.lark_link()
    if url:
        head.append({
            "tag": "column_set", "flex_mode": "flow",
            "columns": [{"tag": "column", "width": "auto", "elements": [{
                "tag": "button", "type": "default", "size": "small",
                "text": {"tag": "plain_text",
                         "content": "Open shipping workspace"},
                "behaviors": [{"type": "open_url", "default_url": url}]}]}],
        })
    head.append({"tag": "hr"})
    card["body"]["elements"][0:0] = head
    return card
