"""Explicit operator commands; never runs automatically on app startup.

Uses existing LARK_APP_ID/SECRET environment variables. Does not print secrets.
inspect is read-only. init-* add schema only; never infer/backfill quantities.
"""
import argparse
import json
from fulfillment_base import BaseStore, SHIPMENT_FIELDS, BaseError, ident
from lark_client import LarkClient


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inspect", "init-shipments", "init-source"])
    parser.add_argument("--base", required=True)
    parser.add_argument("--table")
    parser.add_argument("--apply", action="store_true", help="Required for schema additions")
    args = parser.parse_args()
    store = BaseStore(LarkClient(), {"base_token": args.base, "shipment_table": "unused"})
    tables = store.pages(store.root + "/tables")
    if args.action == "inspect":
        result = [{"name": t["name"], "table_id": t["table_id"],
                   "fields": [{k: f.get(k) for k in ("field_id", "field_name", "type")}
                              for f in store.fields(t["table_id"])]}
                  for t in tables if not args.table or t["table_id"] == args.table]
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return
    if not args.apply:
        parser.error("Schema commands require --apply. They add fields/tables; they do not populate quantities.")
    if args.action == "init-shipments":
        name = "SHIPMENTS - PICK PACK"
        table = next((t for t in tables if t["name"] == name), None)
        if table:
            existing = {f["field_name"] for f in store.fields(table["table_id"])}
            if not set(SHIPMENT_FIELDS).issubset(existing):
                raise BaseError("Existing PICK PACK table has a different schema; inspect manually")
            print("Existing shipment_table:", table["table_id"])
        else:
            result = store.api("POST", store.root + "/tables", json={"table": {
                "name": name, "default_view_name": "All shipments",
                "fields": [{"field_name": f, "type": 1} for f in SHIPMENT_FIELDS]}})
            print("Created shipment_table:", result["table_id"])
    else:
        if not args.table or args.table not in {t["table_id"] for t in tables}:
            parser.error("Select an existing original order/item table with --table")
        fields = {f["field_name"]: f for f in store.fields(args.table)}
        for name, kind in [("Fulfillment Opening China", 2), ("Fulfillment Opening US", 2), ("Ready to Pack", 7)]:
            if name in fields:
                if fields[name]["type"] != kind:
                    raise BaseError("Existing field has an unexpected type: " + name)
                continue
            store.api("POST", store.root + "/tables/" + ident(args.table) + "/fields",
                      json={"field_name": name, "type": kind})
            print("Added:", name)
        print("No order data changed. Verify opening balances and per-item identity before checking Ready to Pack.")


if __name__ == "__main__":
    main()
