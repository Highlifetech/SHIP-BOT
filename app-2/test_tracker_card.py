"""What a line on the tracker card has to tell you.

Every check here started as a complaint about the card in the chat, so each
one names the thing that was wrong rather than the function it lives in.
"""
import datetime as _dt
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ.setdefault("LARK_SHEET_OWNERS", "tokH:Hannah,tokL:Lucy,tokO:Other")
# The Open-dashboard button only exists when there is a dashboard to open,
# so give the card one; without it the button is correctly absent.
os.environ.setdefault("DASHBOARD_URL", "https://example.invalid")
os.environ.setdefault("DASHBOARD_TOKEN", "t")

import card_builder as cb
import card_lint

PASS, FAIL = [], []


def check(label, cond, extra=""):
    (PASS if cond else FAIL).append(label)
    print(("ok   " if cond else "FAIL ") + label +
          (("   " + str(extra)[:90]) if extra else ""))


TODAY = cb._now_et().date()


def row(days_late, **kw):
    """A shipment whose own ETA was `days_late` days ago."""
    r = {"tracking_num": "1Z%013d" % abs(days_late), "carrier": "UPS",
         "customer": "Test Client", "sheet_token": "tokH", "tab": "SEP",
         "row_num": 100 + abs(days_late), "new_status": "LABEL CREATED",
         "delivery_date": str(TODAY - _dt.timedelta(days=days_late))}
    r.update(kw)
    return r


# 1. Carrier sentences were being chopped mid-word: "...released by a…"
released = row(2, new_status="IN TRANSIT",
               raw_status="Your package has been released by a government agency")
detail = cb._detail(released, cb.bucket_for(released))
check("a carrier sentence is not cut off mid-phrase",
      not detail.rstrip().endswith(("by a", "by a…", "…")), detail)
check("the customs phrase reads as the news it is",
      "customs" in detail.lower(), detail)

# 2. How late it is leads the line, and is said once, not twice
late = row(16)
line = cb.shipment_line(late)
check("a late line leads with how late it is",
      line.split("·")[0].strip().endswith("16d late**"), line[:70])
check("the day count isn't repeated in the status text",
      line.count("16d") == 1 and "16 days ago" not in line, line[:110])

# 3. A past ETA must not read like a future promise
check("an ETA that has passed says so", "ETA was" in line, line[-40:])
future = cb.shipment_line(row(-3, new_status="IN TRANSIT",
                              raw_status="In transit"))
check("a future ETA stays a plain date", "ETA was" not in future, future[-40:])

# 4. Worst first -- alphabetical order buried the oldest problem
def card_text(card):
    """All the markdown on a card, in the order it is drawn."""
    out = []
    def walk(n):
        if isinstance(n, dict):
            if n.get("tag") == "markdown":
                out.append(n.get("content", ""))
            for v in n.values():
                walk(v)
        elif isinstance(n, list):
            for v in n:
                walk(v)
    walk(card["body"]["elements"])
    return out


card = cb.build_tracker_card_v2([row(3), row(16), row(9)], status="flagged")
order = [int(x) for x in re.findall(r"\*\*(\d+)d late\*\*",
                                    "\n".join(card_text(card)))]
check("the most overdue shipment is listed first",
      order == sorted(order, reverse=True) and order, order)
check("every late shipment says how late it is", len(order) == 3, order)

# 5. Every line names someone; several sheet rows carry no client at all
anon = cb.shipment_line(row(4, customer=""))
check("a line with no client still names whose desk it is",
      "Hannah" in anon, anon[:90])

# 6. An empty section shouldn't spend two lines saying nothing
quiet = cb.build_tracker_card_v2([row(5)])
text = "\n".join(e.get("content", "") for e in quiet["body"]["elements"])
check("no empty 'Arriving Today' when nothing arrives today",
      "Arriving Today" not in text)
check("sheet plumbing is off the card", "sheets" not in text)

# 7. Whatever else changes, Lark still has to accept it
bad = []
for cl, st in (("all", "all"), ("all", "flagged"), ("all", "transit"),
               ("all", "unscanned"), ("test_client", "all")):
    c = cb.build_tracker_card_v2([row(3), row(16)], client=cl, status=st)
    bad += ["%s/%s: %s" % (cl, st, p) for p in card_lint.lint_v2(c)]
check("every card state still passes the Lark schema linter", not bad, bad[:2])


# --- the card has to actually do something when you press it --------------
#
# The four status chips were markdown code spans: they looked like tabs and
# were inert. Everything below checks a real control exists AND that the
# value it sends is one the webhook already knows how to handle.

def controls(node, out=None):
    """Every button and select on the card, with the value it sends."""
    out = [] if out is None else out
    if isinstance(node, dict):
        if node.get("tag") in ("button", "select_static",
                              "interactive_container"):
            val = node.get("value") or {}
            for b in node.get("behaviors") or []:
                val = b.get("value") or val
                if b.get("type") == "open_url":
                    val = {"action": "open_url"}
            out.append((node, val))
        for v in node.values():
            controls(v, out)
    elif isinstance(node, list):
        for v in node:
            controls(v, out)
    return out


live = cb.build_tracker_card_v2([row(3), row(16), row(-2, new_status="IN TRANSIT")])
ctrls = controls(live)
actions = {v.get("action") for _n, v in ctrls}

check("all four stat tiles are real controls, not code text",
      sum(1 for n, v in ctrls
          if n["tag"] == "interactive_container"
          and v.get("action") == "status_filter") == 4,
      sorted(actions))
check("the client filter is on the card", "client_filter" in actions, sorted(actions))
check("mark as delivered is on the card", "mark_delivered" in actions, sorted(actions))
check("the dashboard button is still there", "open_url" in actions, sorted(actions))

# Whatever a control sends, the webhook must recognise it.
KNOWN = {"status_filter", "client_filter", "mark_delivered", "open_url"}
unknown = sorted(a for a in actions if a not in KNOWN)
check("no control sends an action the webhook can't handle", not unknown, unknown)

# Each tab has to lead somewhere different, and pressing the active one returns.
tab_targets = [v["status"] for n, v in ctrls
               if v.get("action") == "status_filter"]
check("the four tiles filter to four different things",
      len(set(tab_targets)) == 4, tab_targets)

on_flagged = cb.build_tracker_card_v2([row(3)], status=cb.FLAGGED)
active = [(n, v) for n, v in controls(on_flagged)
          if n["tag"] == "interactive_container"
          and n.get("background_style") == "grey"]
check("the tile you are on is the highlighted one", len(active) == 1, len(active))
check("pressing the active tile goes back to the overview",
      active and active[0][1]["status"] == "all",
      active[0][1] if active else None)

# Every tab must render a view of its own -- "Today" quietly returned the
# overview because it had no branch.
for key, want in ((cb.FLAGGED, "Needs attention"), (cb.ARRIVING, "Arriving today"),
                  (cb.TRANSIT, "In transit"), (cb.UNSCANNED, "Not yet scanned")):
    c = cb.build_tracker_card_v2([row(3), row(-2, new_status="IN TRANSIT")],
                                 status=key)
    body = "\n".join(card_text(c))
    check("the %s tab shows its own list" % want.lower(), want in body, body[:70])

# Every shipment carries its own Mark-delivered button, and each names the
# shipment it would close -- a bulk dropdown listing the same rows was a
# second way to pick the wrong one.
per_row = [(n, v) for n, v in ctrls
           if n["tag"] == "button" and v.get("action") == "mark_delivered"]
# Two per person on the overview -- it is a nudge, not the whole list.
check("every listed shipment has its own mark-delivered button",
      len(per_row) == 2, len(per_row))
check("each button names the shipment it closes",
      all(v.get("handle") for _n, v in per_row),
      [str(v.get("handle"))[:20] for _n, v in per_row][:2])
check("the buttons name different shipments",
      len({v["handle"] for _n, v in per_row}) == len(per_row))

# Grouped by person, so everyone finds their own pile.
mixed = [row(6, sheet_token="tokH"), row(11, sheet_token="tokL")]
heads = "\n".join(card_text(cb.build_tracker_card_v2(mixed)))
check("the card groups shipments by person",
      "Hannah" in heads and "Lucy" in heads, heads[:90])

print("\n%d passed, %d failed" % (len(PASS), len(FAIL)))
sys.exit(1 if FAIL else 0)
