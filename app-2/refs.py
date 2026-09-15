"""One spelling for an order / shipment reference.

A reference like HLT-S06601 is a prefix, a section letter, then a number. The
leading zero of that number gets typed as the letter O -- often enough that it
is the normal spelling. Of the HLT-S references on the shipping sheets, 92 rows
carry the letter and 5 carry the zero, and the Production Base carries the
letter too (#HLT-SO6065), so this is not a sheet problem that pulling directly
from the Base would solve. Both sources spell it the same wrong way.

The two look identical in a cell and are different strings everywhere else.
That is why somebody reads an ID off a row, types it into the dashboard search,
and gets nothing back.

So both readers -- the sheet reader in lark_client and the Base reader in
fulfillment_base -- fold it here, at the point of ingest, and everything
downstream sees one spelling: dashboard, card, Iron Bot, mark-as-delivered.
The alternative was correcting 92 cells by hand and then correcting the next
92, because nothing stops the next one being typed the same way.

This module deliberately imports nothing, so the Base adapter can use it
without pulling in the app's configuration.
"""
import re

# Non-greedy prefix, so the section letter stays in the prefix and the O that
# follows it is read as part of the number. The number must already contain a
# digit, which keeps this away from words: ORDER-ONE and SO-NOTES are text.
_REF_RE = re.compile(r"^(.*?-[A-Z]*?)([0-9O]*[0-9][0-9O]*)$")


def canonical_ref(raw):
    """Normalize a reference: a letter O inside its number is a zero.

    'HLT-SO6601' -> 'HLT-S06601'. '#HLT-SO6065' -> '#HLT-S06065'.
    Anything that is not a hyphenated reference ending in a number comes back
    exactly as it went in, minus surrounding whitespace.
    """
    value = str(raw or "").strip()
    match = _REF_RE.match(value.upper())
    if not match:
        return value
    prefix, number = match.groups()
    if "O" not in number:
        return value
    # Slice the original rather than the upper-cased copy: this changes one
    # character, it is not a general upcasing pass that would alter how an ID
    # displays on a card.
    return value[:len(prefix)] + number.replace("O", "0")
