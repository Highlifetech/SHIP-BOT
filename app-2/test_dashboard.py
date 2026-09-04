import os, sys, types
os.environ["DASHBOARD_TOKEN"]="testtoken123"
os.environ["DASHBOARD_URL"]="https://shipment-tracker-production-7096.up.railway.app"
os.environ["LARK_SHEET_OWNERS"]=("tokH:Hannah,tokL:Lucy,tokO:Other,tok7:Hannah 7Brew Coffee,"
 "tokC:Hannah Craftworks,tokLF:Hannah Live Fast Die Young,tokM:Hannah MFused 副本")
sys.path.insert(0,"/root/shipbot"); sys.path.insert(0,"/tmp")
from real_rows import ROWS
for i,r in enumerate(ROWS): r.setdefault("row_num",10+i); r.setdefault("tab","SEP")

from flask import Flask
import dashboard, card_builder as cb

chat = types.SimpleNamespace(_SNAPSHOT={"results": ROWS, "ts": 9e18}, update_snapshot=lambda r: None)
calls=[]
class FakeLark:
    def mark_delivered(self, token, tab, row, when=None):
        calls.append((token, tab, row)); return "2026-09-04"
app = Flask(__name__)
dashboard.register(app, chat, lambda dry_run=False: ROWS, FakeLark())
c = app.test_client()

fails=[]
def check(label, cond, extra=""):
    if not cond: fails.append(label)
    print(("PASS  " if cond else "FAIL  ") + label + (("   " + str(extra)[:90]) if extra else ""))

r=c.get("/dashboard");                  check("GET /dashboard with no token is refused", r.status_code==403)
r=c.get("/dashboard?t=wrong");          check("wrong token is refused", r.status_code==403)
r=c.get("/dashboard?t=testtoken123");   check("valid token serves the page", r.status_code==200 and b"Shipment Tracker" in r.data)
r=c.get("/api/shipments?t=testtoken123");check("GET /api/shipments 200", r.status_code==200, r.status_code)
d=r.get_json()
check("24 open shipments", len(d["shipments"])==24, len(d["shipments"]))
# Date-independent: an unscanned label crosses into "flagged" once its ETA
# passes, so assert the pair, not today's split between them.
check("10 need attention or are unscanned",
      d["totals"]["flagged"] + d["totals"]["unscanned"] == 10, d["totals"])
check("owners lead with Hannah/Lucy/Other", d["owners"][:3]==["Hannah","Lucy","Other"], d["owners"][:4])
check("7Brew spellings merged to one client", len([x for x in d["clients"] if "7" in x])==1, [x for x in d["clients"] if "7" in x])
check("Craftworks merged", "Craftworks Design" not in d["clients"])
check("no junk tracking row", not any(s["tracking"]=="HANNAH" for s in d["shipments"]))
check("/api/shipments needs a token", c.get("/api/shipments").status_code==403)

h=d["shipments"][0]["handle"]
r=c.post("/api/mark-delivered?t=testtoken123", json={"handle":h})
check("POST /api/mark-delivered succeeds", r.status_code==200 and r.get_json().get("ok") is True, r.get_json())
check("it wrote to the sheet", len(calls)==1, calls)
check("bad handle rejected", c.post("/api/mark-delivered?t=testtoken123", json={"handle":"garbage"}).status_code==400)
check("write needs a token", c.post("/api/mark-delivered", json={"handle":h}).status_code==403)
check("/dashboard/health is open", c.get("/dashboard/health").status_code==200)

card = cb.build_tracker_card_v2(ROWS, sheet_count=8)

def _find_open_url(node):
    """The dashboard button, wherever it sits in the card."""
    if isinstance(node, dict):
        if node.get("tag") == "button":
            for b in node.get("behaviors", []):
                if b.get("type") == "open_url":
                    return b.get("default_url", "")
        for v in node.values():
            got = _find_open_url(v)
            if got:
                return got
    elif isinstance(node, list):
        for v in node:
            got = _find_open_url(v)
            if got:
                return got
    return ""

link = _find_open_url(card)
check("card carries an Open dashboard link", bool(link))

# the card opens with the open count and the four status chips
head = card["body"]["elements"][0]["content"]
chips = card["body"]["elements"][1]["content"]
check("card opens with the open count and updated time",
      "open" in head and "Updated" in head, head[:60])
check("chips read Attention / Today / Transit / Unscanned",
      all(w in chips for w in ("Attention", "Today", "Transit", "Unscanned")),
      chips[:80])

# the overview shows only what needs a person
def _headings(c):
    return [e["content"] for e in c["body"]["elements"]
            if e["tag"] == "markdown"
            and e["content"].startswith("**") and e["content"].endswith("**")]

titles = _headings(card)
check("overview leads with Needs Attention and Arriving Today",
      titles[:2] == ["**Needs Attention**", "**Arriving Today**"], titles[:3])

# picking a client re-renders the card scoped to them
one = cb.build_tracker_card_v2(ROWS, client="hannah", sheet_count=8)
t2 = _headings(one)
check("choosing a client scopes the card to them",
      t2 and t2[0] == "**Hannah**", t2[:2])
sub = [e["content"] for e in one["body"]["elements"]
       if e["tag"] == "markdown" and "open" in e["content"] and "grey" in e["content"]]
check("client view carries an open / attention / arriving line",
      any("open" in x for x in sub), sub[:2])

# the line reads tracking -> order -> customer -> details
sample = [r for r in ROWS if r.get("shipment_id") and r.get("customer")][0]
line = cb.shipment_line(sample)
tn = sample["tracking_num"]
check("line leads with the linked tracking number",
      ("[**%s**]" % tn) in line, line[:80])
check("order number comes after the tracking number",
      line.index(sample["shipment_id"]) > line.index(tn), line[:110])

# --- every card layout, every filter state, linted against Lark's schema ---
import card_lint
states = [("all","all"), ("all","flagged"), ("all","transit"),
          ("all","unscanned"), ("hannah","all"), ("lucy","all"),
          ("other","all"), ("7brew_coffee","flagged")]
bad = []
for cl, st in states:
    v2 = cb.build_tracker_card_v2(ROWS, client=cl, status=st, sheet_count=8)
    v1 = cb.build_tracker_card(ROWS, client=cl, status=st, sheet_count=8)
    for name, card, probs in (("2.0", v2, card_lint.lint_v2(v2)),
                              ("1.0", v1, card_lint.lint_v1(v1))):
        if probs:
            bad.append("%s %s/%s: %s" % (name, cl, st, probs[0]))
check("all %d card states pass the Lark schema linter" % (len(states)*2),
      not bad, bad[:3])

# empty and single-shipment cards are the states that break layout code
for label, rows in (("no shipments", []), ("one shipment", ROWS[:1])):
    v2 = cb.build_tracker_card_v2(rows, sheet_count=0)
    check("%s -> valid 2.0 card" % label, not card_lint.lint_v2(v2),
          card_lint.lint_v2(v2)[:2])

# the linter must actually catch the bug that shipped
regression = {"schema":"2.0","config":{},"header":{"title":{"tag":"plain_text","content":"x"}},
              "body":{"elements":[{"tag":"note","elements":[]}]}}
check("linter catches the 'note' tag that Lark 400'd on",
      any("note" in p for p in card_lint.lint_v2(regression)))

TOTAL = 28
print("\nbutton link:", link)
print("\n%d/%d passed" % (TOTAL-len(fails), TOTAL))
sys.exit(1 if fails else 0)
