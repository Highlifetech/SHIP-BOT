"""One-off: list every Lark chat the bot app belongs to, with its chat_id.

Run via GitHub Actions (has the bot's LARK_APP_ID/SECRET). The output shows the
oc_... chat_id for each channel the bot is a member of -- use it to configure
LARK_CHAT_ID_FOUNDERS for the customs/stuck alerts.
"""
import logging
import requests
from lark_client import LarkClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("list_chats")


def main():
    lark = LarkClient()
    base = lark.base_url.rstrip("/")
    url = base + "/open-apis/im/v1/chats"
    page = ""
    total = 0
    while True:
        params = {"page_size": 100}
        if page:
            params["page_token"] = page
        r = requests.get(url, headers=lark._headers(), params=params, timeout=30)
        try:
            data = r.json()
        except Exception:
            logger.error("Non-JSON response (HTTP %s): %s", r.status_code, r.text[:200])
            return
        if data.get("code") != 0:
            logger.error("chats list error: %s", data)
            return
        d = data.get("data", {})
        for it in d.get("items", []):
            total += 1
            logger.info("CHATLINE :: %-45s :: %s", it.get("name", ""), it.get("chat_id", ""))
        page = d.get("page_token", "") if d.get("has_more") else ""
        if not page:
            break
    logger.info("=== %d chats the bot is a member of ===", total)


if __name__ == "__main__":
    main()
