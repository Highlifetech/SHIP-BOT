"""The order/shipment reference spelling that made the dashboard unsearchable.

Somebody reads HLT-S06601 off a row, types it into the dashboard search, and
gets nothing -- because the cell actually holds HLT-SO6601 with a letter O
where the leading zero belongs. The two render identically in the sheet.

canonical_ref() folds that at ingest. These cases are the ones that have to
keep holding: the typo folds, the correct spelling is untouched, and nothing
that merely contains an O gets mangled on the way through.
"""
import os
import unittest

os.environ.setdefault("LARK_APP_ID", "test")
os.environ.setdefault("LARK_APP_SECRET", "test")

from refs import canonical_ref


class CanonicalRefTests(unittest.TestCase):
    def test_letter_o_in_the_number_becomes_zero(self):
        # The four IDs that were spelled correctly in the sheet, and the way
        # the other sixty were spelled.
        self.assertEqual("HLT-S06601", canonical_ref("HLT-SO6601"))
        self.assertEqual("HLT-S06638", canonical_ref("HLT-SO6638"))
        self.assertEqual("HLT-S06666", canonical_ref("HLT-SO6666"))
        self.assertEqual("HLT-S06730", canonical_ref("HLT-SO6730"))

    def test_more_than_one_o(self):
        self.assertEqual("HLT-S06730", canonical_ref("HLT-SO673O"))

    def test_correct_spelling_is_left_exactly_alone(self):
        for ref in ("HLT-S06601", "HLT-S06638", "HLT-J1234", "PO-2024-11"):
            self.assertEqual(ref, canonical_ref(ref))

    def test_case_and_whitespace(self):
        # Lower case stays lower case -- this normalizes one character, it is
        # not a general upcasing pass that would change how IDs display.
        self.assertEqual("hlt-s06601", canonical_ref("hlt-so6601"))
        self.assertEqual("HLT-S06601", canonical_ref("  HLT-SO6601  "))

    def test_the_base_spells_it_the_same_way(self):
        # Pulling directly from the Production Base does not avoid this: the
        # Base carries '#HLT-SO6065' too. The leading '#' must survive.
        self.assertEqual("#HLT-S06065", canonical_ref("#HLT-SO6065"))

    def test_never_touches_things_that_are_not_numbered_references(self):
        # A reference with no digits, free text, a customer name, and a
        # tracking number: an O in any of these is a real letter O.
        for value in ("HLT-SO", "ORDER-ONE", "7Brew Coffee", "Off Menu - OMS",
                      "1ZG010566734150379", "SO-NOTES", ""):
            self.assertEqual(value.strip(), canonical_ref(value))

    def test_blank_cells(self):
        self.assertEqual("", canonical_ref(None))
        self.assertEqual("", canonical_ref("   "))


if __name__ == "__main__":
    unittest.main()
