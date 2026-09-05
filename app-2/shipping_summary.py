"""Read-only pick/pack snapshot for the existing scheduled Lark report."""
import logging


def snapshot(lark):
    from fulfillment_web import configured_service
    try:
        service = configured_service(lark)
        if service is None:
            return "Pick & pack: Production Base connection awaiting setup."
        # Only read saved manifests; never create reservations or query carriers.
        shipments = service.store.shipments()
        counts = {status: sum(s["status"] == status for s in shipments)
                  for status in ("Packed", "Shipped", "Received")}
        return ("Pick & pack — current saved batches\n"
                "Packed: {Packed} · Shipped: {Shipped} · Received: {Received}\n"
                "Packed reserves items; Shipped is dispatch status, not a carrier scan. "
                "Received inbound stock is not customer fulfillment.").format(**counts)
    except Exception:
        logging.getLogger(__name__).warning("Pick/pack summary data unavailable")
        return "Pick & pack: shipment data unavailable. Counts are not reported as zero."


def build_card(results, sheet_count, summary):
    import card_builder
    import dashboard
    card = card_builder.build_tracker_card_v2(results, sheet_count=sheet_count)
    card["header"]["title"]["content"] = "Shipping operations summary"
    buttons = []
    for label, url in (("Open shipping workspace", dashboard.lark_link()),
                       ("Legacy tracker", dashboard.lark_link(status="all"))):
        if url:
            buttons.append({"tag": "column", "width": "auto", "elements": [{
                "tag": "button", "type": "default", "size": "small",
                "text": {"tag": "plain_text", "content": label},
                "behaviors": [{"type": "open_url", "default_url": url}]}]})
    card["body"]["elements"][0:0] = [
        {"tag": "markdown", "content": summary},
        {"tag": "column_set", "flex_mode": "flow", "columns": buttons},
        {"tag": "hr"},
        {"tag": "markdown", "content": "**Legacy sheet tracker** — separate from pick & pack; totals may overlap."},
    ] if buttons else [
        {"tag": "markdown", "content": summary}, {"tag": "hr"},
        {"tag": "markdown", "content": "**Legacy sheet tracker** — separate totals; may overlap."}]
    return card
