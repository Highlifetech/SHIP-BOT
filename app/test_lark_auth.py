"""Round-trip tests for the Lark sign-in module (no network)."""
import os, sys, importlib
os.environ.update(LARK_APP_ID="cli_test123", LARK_APP_SECRET="secret_abc", LARK_SSO="1")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import lark_auth as la

fails = []
def check(label, cond, extra=""):
    if not cond: fails.append(label)
    print(("PASS  " if cond else "FAIL  ") + label + (("   " + str(extra)[:70]) if extra else ""))

check("SSO reports configured when the vars are set", la.configured())
user = {"name": "Hannah", "open_id": "ou_abc123"}
cookie = la.make_session(user)
check("session round-trips the user", (la.read_session(cookie) or {}).get("name") == "Hannah")
body, sig = cookie.rsplit(".", 1)
check("a tampered payload is rejected",
      la.read_session(la._b64(b'{"name":"Admin","exp":9999999999}') + "." + sig) is None)
check("a tampered signature is rejected", la.read_session(body + ".AAAA") is None)
check("garbage is rejected", la.read_session("not-a-cookie") is None)
check("no cookie is rejected", la.read_session("") is None)
la.SESSION_TTL = -1
check("an expired session is rejected", la.read_session(la.make_session(user)) is None)
la.SESSION_TTL = 3600
url = la.login_url("https://x.up.railway.app/auth/lark/callback")
check("login URL hits Lark's authorize page with the app id",
      url.startswith("https://accounts.larksuite.com/open-apis/authen/v1/authorize")
      and "client_id=cli_test123" in url)
check("redirect_uri is url-encoded",
      "redirect_uri=https%3A%2F%2Fx.up.railway.app%2Fauth%2Flark%2Fcallback" in url)
check("scope is requested", "contact%3Acontact.base%3Areadonly" in url)
os.environ["LARK_SSO"] = ""
importlib.reload(la)
check("SSO stays off until LARK_SSO is set", not la.configured())
print("\n%d/%d passed" % (11 - len(fails), 11))
sys.exit(1 if fails else 0)
