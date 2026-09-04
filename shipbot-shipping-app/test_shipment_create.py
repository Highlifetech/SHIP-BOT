"""Creating a shipment: open lines, validation, and the rows that get written."""

import shipment_create as sc


PASS = []
FAIL = []


def check(label, cond):
    (PASS if cond else FAIL).append(label)


COLUMNS = {"customer": "E", "recipient": "C", "order_num": "D",
           "product_name": "S", "product_photo": "F", "qty_shipped": "J",
           "qty_expected": "K", "box_num": "T", "tracking_num": "H",
           "carrier": "I", "num_boxes": "P", "notes": "Q", "status": "N"}

ROWS = [
    # 300 of 600 shipped -- 300 still open.
    {"sheet_token": "tokA", "tab": "Hannah", "row_num": 5, "customer": "7Brew",
     "order_num": "PO-1", "product_name": "Dripper", "qty_expected": "600",
     "qty_shipped": "300", "tracking_num": "1Z999"},
    # Ordered, nothing shipped, no tracking -- fully open.
    {"sheet_token": "tokA", "tab": "Hannah", "row_num": 6, "customer": "7Brew",
     "order_num": "PO-1", "product_name": "Trucker Cap", "qty_expected": "200",
     "qty_shipped": "", "tracking_num": ""},
    # Fully shipped and moving -- not open.
    {"sheet_token": "tokA", "tab": "Hannah", "row_num": 7, "customer": "7Brew",
     "order_num": "PO-2", "product_name": "Tote", "qty_expected": "50",
     "qty_shipped": "50", "tracking_num": "1Z888"},
    # A different board.
    {"sheet_token": "tokB", "tab": "Lucy", "row_num": 4, "customer": "Craftworks",
     "order_num": "PO-9", "product_name": "Sticker", "qty_expected": "500",
     "qty_shipped": "", "tracking_num": ""},
    # Noise: no quantity, no product.
    {"sheet_token": "tokA", "tab": "Hannah", "row_num": 8, "customer": "7Brew"},
]

opens = sc.open_orders(ROWS)
by_product = {o["product"]: o for o in opens}

# ------------------------------------------------------------ open orders
check("partially shipped lines stay open", "Dripper" in by_product)
check("remaining is what's left", by_product["Dripper"]["remaining"] == 300)
check("untracked lines are open", "Trucker Cap" in by_product)
check("fully shipped and tracked lines drop out", "Tote" not in by_product)
check("rows with no quantity and no product drop out", len(opens) == 3)
check("lines carry their board", by_product["Sticker"]["board"] == "Lucy")
check("lines carry their sheet and row",
      by_product["Sticker"]["sheet_token"] == "tokB"
      and by_product["Sticker"]["row_num"] == 4)
check("keys are unique", len({o["key"] for o in opens}) == 3)
check("empty input is empty output", sc.open_orders([]) == [])

bs = {b["board"]: b for b in sc.boards(ROWS)}
check("boards are counted", bs["Hannah"]["lines"] == 2 and bs["Lucy"]["lines"] == 1)
check("board units add up", bs["Hannah"]["units"] == 500)
check("clients are listed", sc.clients(ROWS) == ["7Brew", "Craftworks"])

# ------------------------------------------------------------- validation
GOOD = {
    "sheet_token": "tokA", "tab": "Hannah", "client": "7Brew", "carrier": "UPS",
    "tracking": "1ZC228W90429665565", "boxes": 2,
    "lines": [
        {"key": by_product["Dripper"]["key"], "product": "Dripper",
         "order_num": "PO-1", "ordered": 600, "qty": 300, "box": 1},
        {"key": by_product["Trucker Cap"]["key"], "product": "Trucker Cap",
         "order_num": "PO-1", "ordered": 200, "qty": 200, "box": 2},
    ],
}
avail = {o["key"]: o["remaining"] for o in opens}
check("a good shipment validates", sc.validate(GOOD, avail) == [])


def bad(change, needle):
    p = dict(GOOD)
    p.update(change)
    return any(needle.lower() in x.lower() for x in sc.validate(p, avail))


check("no lines is caught", bad({"lines": []}, "at least one order line"))
check("no client is caught", bad({"client": ""}, "Client is required"))
check("no carrier is caught", bad({"carrier": ""}, "Carrier is required"))
check("no tracking is caught", bad({"tracking": ""}, "Tracking number is required"))
check("junk tracking is caught",
      bad({"tracking": "no idea"}, "doesn't look right"))
check("short tracking is caught", bad({"tracking": "1Z9"}, "doesn't look right"))
check("zero boxes is caught", bad({"boxes": 0}, "at least one box"))
check("absurd box counts are caught", bad({"boxes": 500}, "more than this form"))
check("an empty box is caught", bad({"boxes": 3}, "Box 3 is empty"))
check("two empty boxes read naturally", bad({"boxes": 4}, "Boxes 3, 4 are empty"))

over = dict(GOOD)
over["lines"] = [dict(GOOD["lines"][0], **{"qty": 999}), GOOD["lines"][1]]
check("overshipping is caught",
      any("only 300 are still open" in p for p in sc.validate(over, avail)))

noqty = dict(GOOD)
noqty["lines"] = [dict(GOOD["lines"][0], **{"qty": 0}), GOOD["lines"][1]]
check("a zero quantity is caught",
      any("has no quantity" in p for p in sc.validate(noqty, avail)))

badbox = dict(GOOD)
badbox["lines"] = [dict(GOOD["lines"][0], **{"box": 7}), GOOD["lines"][1]]
check("a nonexistent box is caught",
      any("box 7, which doesn't exist" in p for p in sc.validate(badbox, avail)))

dupe = dict(GOOD)
dupe["lines"] = [GOOD["lines"][0], dict(GOOD["lines"][0])]
check("a duplicated line is caught",
      any("appears twice" in p for p in sc.validate(dupe, avail)))

# ---------------------------------------------------------------- writing
check("next row follows the last one seen", sc.next_row(ROWS, "tokA", "Hannah") == 9)
check("an unseen tab starts after the header",
      sc.next_row(ROWS, "tokZ", "New") == 3)

work = sc.plan(GOOD, ROWS, COLUMNS)
cells = {(u["col"], u["row"]): u["value"] for u in work["updates"]}
check("writing starts on the next free row", work["start_row"] == 9)
check("one row per line", work["rows"] == 2)
check("lines are ordered by box",
      [p["box"] for p in work["preview"]] == [1, 2])
check("tracking is written once, on the head row",
      cells.get(("H", 9)) == "1ZC228W90429665565" and ("H", 10) not in cells)
check("carrier is written once", cells.get(("I", 9)) == "UPS"
      and ("I", 10) not in cells)
check("box count is written once", cells.get(("P", 9)) == "2"
      and ("P", 10) not in cells)
check("status is set on the head row",
      cells.get(("N", 9)) == "Label Created/Not Scanned")
check("every line gets its product", cells.get(("S", 9)) == "Dripper"
      and cells.get(("S", 10)) == "Trucker Cap")
check("every line gets its quantity shipped",
      cells.get(("J", 9)) == "300" and cells.get(("J", 10)) == "200")
check("quantity ordered is preserved", cells.get(("K", 9)) == "600")
check("every line gets its box", cells.get(("T", 9)) == "1"
      and cells.get(("T", 10)) == "2")
check("the client lands on every line", cells.get(("E", 9)) == "7Brew"
      and cells.get(("E", 10)) == "7Brew")
check("the preview matches the writes",
      work["preview"][0]["row"] == 9 and work["preview"][0]["qty"] == "300")

# An unmapped column is skipped rather than written to the wrong place.
thin = sc.plan(GOOD, ROWS, {k: v for k, v in COLUMNS.items() if k != "box_num"})
check("unmapped columns are skipped, not guessed",
      not any(u["col"] == "T" for u in thin["updates"]))
check("the rest still writes", any(u["col"] == "S" for u in thin["updates"]))


class FakeLark(object):
    def __init__(self):
        self.written = None

    def get_sheet_metadata(self, token):
        return [{"title": "Hannah", "sheet_id": "sheet123"}]

    def write_cells(self, token, sheet_id, updates):
        self.written = (token, sheet_id, updates)


lark = FakeLark()
made = sc.create(lark, ROWS, GOOD, COLUMNS)
check("create writes to the resolved sheet id", lark.written[1] == "sheet123")
check("create writes every cell", len(lark.written[2]) == len(work["updates"]))
check("create reports the tracking", made["tracking"] == "1ZC228W90429665565")
check("create reports the rows and boxes",
      made["rows"] == 2 and made["boxes"] == 2)

blocked = FakeLark()
try:
    sc.create(blocked, ROWS, dict(GOOD, **{"tracking": ""}), COLUMNS)
    check("an invalid shipment is refused", False)
except ValueError as e:
    check("an invalid shipment is refused", "Tracking number is required" in str(e))
check("a refused shipment writes nothing", blocked.written is None)

nocols = FakeLark()
try:
    sc.create(nocols, ROWS, GOOD, {})
    check("no mapped columns is refused", False)
except ValueError as e:
    check("no mapped columns is refused", "no mapped columns" in str(e))
check("that writes nothing either", nocols.written is None)

try:
    sc.create(FakeLark(), ROWS, dict(GOOD, **{"tab": "Nowhere"}), COLUMNS)
    check("an unknown tab is refused", False)
except ValueError as e:
    check("an unknown tab is refused", "Can't find the tab" in str(e))

print("\n".join("  ok   %s" % p for p in PASS))
if FAIL:
    print("\n".join("  FAIL %s" % f for f in FAIL))
print("\n%d passed, %d failed" % (len(PASS), len(FAIL)))
raise SystemExit(1 if FAIL else 0)
