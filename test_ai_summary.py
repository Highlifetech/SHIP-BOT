"""One-off: verify the Claude customs-summary path end to end."""
import logging
from stuck_detector import _ai_customs_summary

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

sample = [
    {"name": "Brendan", "carrier": "DHL", "tracking_num": "8387502222",
     "raw_status": "Clearance event", "location": "NEW YORK CITY GATEWAY",
     "days_unchanged": 3.0, "reason": "CUSTOMS_HOLD"},
    {"name": "Hannah 7Brew", "carrier": "4PX", "tracking_num": "000571273",
     "raw_status": "Held by customs", "location": "LOS ANGELES, CA",
     "days_unchanged": 2.5, "reason": "CUSTOMS_HOLD"},
]

text = _ai_customs_summary(sample)
logging.info("AI_SUMMARY_START")
for line in (text or "(empty -- key missing or call failed)").splitlines() or ["(empty)"]:
    logging.info("SUMMARY:: %s", line)
logging.info("AI_SUMMARY_END")
