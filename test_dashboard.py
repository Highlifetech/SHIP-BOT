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
check("8 flagged", d["totals"]["flagged"]==8, d["totals"])
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
btn = [e for e in card["body"]["elements"] if e["tag"]=="column_set"]
link = ""
for cs in btn:
    for col in cs["columns"]:
        x=col["elements"][0]
        if x["tag"]=="button" and x["behaviors"][0]["type"]=="open_url":
            link = x["behaviors"][0]["default_url"]
check("card carries an Open dashboard link", bool(link))
print("\nbutton link:", link)
print("\n%d/%d passed" % (17-len(fails), 17))
sys.exit(1 if fails else 0)
