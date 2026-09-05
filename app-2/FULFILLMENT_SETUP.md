# Order-first pick & pack — deployment handoff

## What changed

### Layout replacement (non-destructive)

- `/dashboard`: new Shipping workspace, with Overview, Create shipment,
  Shipments & packing lists, and How it works navigation.
- `/dashboard/legacy`: preserved previous tracker, including the old sheet-entry
  modal. The legacy HTML, APIs, sheet data and carrier routines are retained.
- `/fulfillment`: alias for the new workspace. `#create`, `#shipments`, and `#flow`
  open the matching screen without changing shipment records.
- Old `/dashboard?status=...` notification links still open the legacy tracker.
- Set `SHIPPING_LAYOUT=legacy` to restore the old default page without reverting
  files or deleting new functionality. Authentication still applies to both.
- New Overview counts only app-created shipments and approved source items;
  it does not imply old shipping sheets were migrated. Failed connections show
  an explicit setup message and keep Legacy accessible.

This is a source-code change, not a production deployment. It must be pushed,
deployed and checked in the real tenant before the live URL changes.

Railway service `shipment-tracker` currently deploys **`app-2`**, not `app`, the
repository root, or the two other uploaded app folders. This change adds
`/fulfillment` to that same Flask application and links it from `/dashboard`.
It does not replace the existing tracking dashboard, notify a chat, buy labels,
or write anything to the seven legacy inbound sheets.

The previous `/api/open-orders` used `read_order_rows` on the *shipping sheets*.
The new `/api/fulfillment/catalog` uses the **Base records API** on an explicit
allowlist of original production/order-item tables. All pages are read. A source
failure stops the operation rather than calculating inventory from partial data.

```
Original Lark order-item records (one row per SKU/variant)
   ↓ select items, quantities and destination
Carton allocation → preview summary + per-carton packing pages
   ↓ save (revalidate current balances under write lock)
SHIPMENTS - PICK PACK in the same Lark Base
   ↓ mark shipped / receive inbound / reship from US
Saved packing lists + on-demand UPS/FedEx/DHL tracking
```

NetSuite is **not connected** in this version. An existing Sales Order value is
retained as the reference. A later NetSuite connector must supply stable order
**line** IDs, SKU/variant, quantities and addresses; order numbers alone are not
enough to pack an order.

## Current status and honest limits

- Implemented and locally tested; **not deployed and not live-connected**.
- Live Lark credentials remain in Railway. The connector reveals variable names,
  not values, so they were not copied locally. No Lark records/schema were changed.
- A local UI harness uses synthetic orders and a simulated Lark store. The real
  production routes do not fall back to that harness or to sample data.
- Existing original source table names and one actual record were inspected in
  Lark. The opened record had a quantity in prose but a blank Quantity field.
  The code correctly requires a reviewed numerical opening balance instead.
- Project photos use Lark attachment download with Base permission metadata and
  fall back visually to the source link when unavailable. Real attachment API
  permissions/downloads still require a live test, including the China team.
- Printing generates a summary plus one page per carton. Browser Print → Save PDF
  works from the preview. This does **not** create/upload a permanent PDF file;
  the immutable packing data is in Lark and can regenerate the document.
- One master tracking number per shipment. The existing carrier clients can
  return sibling-package details. Separate per-carton labels/tracking, carton
  weights/dimensions, partial receiving, inventory adjustments, and custom
  customs/commercial invoices are not implemented.
- Carrier checks are on demand with a 5-minute cache, not background polling or
  webhooks. They never automatically mark stock received or notify a customer.
- New shipments are listed in the new screen. The old tracker remains historical;
  the new records are not duplicated into its inbound sheets.
- Trusted internal pilot only. Uses the existing dashboard token/Lark SSO; no
  new role-based permission system or tenant isolation was added.

## Data model: keep the existing Base

Use `OM_Production_26` (`VcAlbwImaab1KlsFLBVjunTNp1c`). Do **not** make another
production Base or reuse the old demonstration SHIPMENTS table.

### Source items

Start with one original table (the example maps WORKSHOP OTHERS), then add others
after validating the column mapping and record identity. Do **not** ingest MASTER,
URGENT, quote, archived or mirrored copies alongside their original records.
Rows are identified by `(table ID, record ID)`, never title/order number/row index.
If an existing project row includes multiple items/sizes, use a linked
`FULFILLMENT ITEMS` table with one row per actual SKU/variant. Map that table in
the same configuration. Do not treat an entire mixed-item project as one item.

Required source mappings:

| App key | Example Base field | Meaning |
|---|---|---|
| order | Sales Order | Existing NetSuite/Lark SO reference |
| customer | Client Name | Owning customer |
| product | Description | A single reviewed item/variant, not a multi-item project |
| address | Address | Actual final customer delivery address; validate actual field name |
| photos | Production Photos | Attachment field; source-record links also retained |
| opening_china | Fulfillment Opening China | Verified China units available at cutover |
| opening_us | Fulfillment Opening US | Verified US units available at cutover; blank = 0 |
| ready | Ready to Pack | Checkbox, enabled only after the row and stock are reviewed |

Opening quantities are **frozen cutover balances**, not a live Quantity Shipped
column that might later include these same app-created shipments. Deduct historical
shipments once when establishing balances. Otherwise stock will be double-counted
or double-subtracted. Do not change these balances while the app is in use without
a coordinated inventory reconciliation. Current production workflows may continue
updating their existing fields; the app does not modify them.

### Saved shipments

Create a dedicated `SHIPMENTS - PICK PACK` table. One row is one shipment. It has
readable ID, route, customer/order references, address, tracking, status and a
line-by-line Packing Summary. `Manifest JSON` stores an entire versioned snapshot:
items, photos/source IDs, box quantities, address, origin/destination, history and
submission ID. Writing a whole manifest in one record avoids half-saved box/line
records across multiple Base tables. Packing pages read this saved snapshot, not
today's potentially changed order descriptions.

Keep this table app-writable and human-read-only in Base permissions. Do not let
workflows change `Manifest JSON`, IDs or status. The manifest is authoritative;
summary columns are generated views. Normalized linked Cartons/Carton Lines tables
can be added later as derived projections, not a second stock ledger.

### Inventory rules

- **Packed** reserves origin stock, but does not mean the order shipped.
- **Shipped**, China → US: still an inbound transfer, not customer fulfillment.
- **Received**, China → US: all cartons physically received; adds US availability.
- **Shipped**, China/US → customer: outbound movement against the customer's items.
- **Cancelled**: allowed only before shipment, releases the packed reservation.
- Different customer addresses require separate direct/customer shipments.
  Mixed customers are allowed on a China → US warehouse shipment.

## Safe launch procedure

1. Review/merge this branch into the existing repository only after approving the
   change. Deploy `app-2`. Do not change Railway's root folder or replace secrets.
2. Confirm the existing Lark app has Base record read/create/update, schema
   permissions for setup, and attachment download permissions. Grant it access
   to this Base and the intended tables. Schema permission can be removed after
   setup. Scope IDs depend on the app's permission-console version; use the
   official API pages below and test, do not assume existing Sheets access is Base access.
3. From an administrator shell with the existing Lark environment, inspect schema:

   ```sh
   python fulfillment_setup.py inspect --base VcAlbwImaab1KlsFLBVjunTNp1c --table tblFKKlCAbQa2l4h
   ```

4. Explicitly add the new schema (commands only add, never backfill or delete):

   ```sh
   python fulfillment_setup.py init-shipments --base VcAlbwImaab1KlsFLBVjunTNp1c --apply
   python fulfillment_setup.py init-source --base VcAlbwImaab1KlsFLBVjunTNp1c --table tblFKKlCAbQa2l4h --apply
   ```

   Use the returned shipment table ID in the configuration. These API commands
   have not yet been executed against the live Base.
5. Verify a few real item lines and opening quantities. Populate Ready to Pack.
   Confirm the actual US receiving address and the direct-shipping address field.
   Do not infer the receiver from the old packing-list sender letterhead.
6. Set Railway `FULFILLMENT_CONFIG` to the JSON based on
   `fulfillment-config.example.json`, with the new table ID, verified warehouse
   address, actual field mappings, and only the approved source table(s).
   No credentials belong inside this JSON.
7. First deploy without write-coordinator variables for read/preview-only checking.
   Verify real orders, pictures, addresses, both shipment routes and print layout.
8. Before enabling saves: attach a persistent Railway volume at `/data/fulfillment`,
   confirm **one replica only**, and set:

   ```text
   FULFILLMENT_STATE_DIR=/data/fulfillment
   FULFILLMENT_SINGLE_REPLICA=1
   ```

   Existing one-worker gunicorn is suitable. No ephemeral `/tmp` state in production.
   The app cannot detect whether a given directory actually is a mounted volume;
   the administrator must verify this. Back up the volume with the Lark data.
9. Test one small, explicitly approved real shipment: split an item into two
   cartons, save once, verify one Lark row and reduced available units. Open the
   saved packing list after refreshing. Test a second user's concurrent packing.
   Cancel the test Packed record if it is not a real shipment; do not delete it.
10. Open `/fulfillment` in the same registered Lark web app/Workplace entry as the
    existing dashboard. Confirm China users' Lark access, image loading, mobile/
    desktop usability and print behavior before rollout.

## Persistence and recovery

The persistent SQLite file contains a **write journal**, not the primary orders
or shipment database. An OS file lock serializes confirmations across workers in
the single container. A full expected Lark manifest is committed to the journal
*before* the remote write. Duplicate submission IDs return the existing record;
changed contents with the same ID are rejected. Available units are reread under
the lock at confirmation, not trusted from the browser preview.

After an uncertain API response, the next write reconciles the expected document
against Lark. If it is there, the operation completes without another create. If
it is missing/different, further writes pause. **Do not clear the journal or retry
with a new ID to get past this.** An administrator must inspect Lark, API outcome
and the pending journal entry, establish whether the remote write happened, and
repair/reconcile the exact record before resuming. Externally edited/deleted app
manifests likewise pause writes. Recovery UI is not yet implemented.

This lock is not suitable for multiple replicas, regions, independently writing
apps or direct manual modifications. Move the coordinator to a transactional
shared database/advisory lock before any such deployment. Read consistency across
external production edits is not transactional; freeze opening fields and control
Ready to Pack changes during the pilot.

## Validation and local test

```sh
cd app-2
python -m unittest -v test_fulfillment
python test_shipment_create.py
python test_packing_list.py
python test_lark_auth.py
python fulfillment_demo.py
```

Local UI: `http://127.0.0.1:8787/fulfillment`. Sample quantities reset when the
harness stops; it is not a deployment target. It imports only the test-double
store, not real Lark credentials. Carrier lookups are disabled in this mode.

Legacy `test_dashboard.py` cannot run in the current repo because its `real_rows`
fixture module was not committed. This is an existing fixture gap, not a new
passing test. Pillow was missing from requirements for legacy Excel photos and
is now explicit.

## API references

- [List Base records](https://open.larksuite.com/document/server-docs/docs/bitable-v1/app-table-record/list)
- [Create a Base record](https://open.larksuite.com/document/server-docs/docs/bitable-v1/app-table-record/create)
- [Base attachment fields](https://open.larksuite.com/document/server-docs/docs/bitable-v1/app-table-field/attachment)

Confirm these and your tenant permissions during live setup. The local tests
verify the app's contracts and safety behavior; they do not certify tenant access
or substitute for an end-to-end live API test.
