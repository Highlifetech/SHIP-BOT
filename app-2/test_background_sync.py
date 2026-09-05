import threading
import unittest
from background_sync import BackgroundSnapshot


class SyncTests(unittest.TestCase):
    def test_retains_last_success_and_timestamp_on_failure(self):
        cache = BackgroundSnapshot(lambda: {'items': [1]})
        cache.refresh()
        first = cache.snapshot()
        cache.reader = lambda: (_ for _ in ()).throw(RuntimeError('private error'))
        cache.refresh()
        after = cache.snapshot()
        self.assertEqual(after['data'], first['data'])
        self.assertEqual(after['last_synced'], first['last_synced'])
        self.assertNotIn('private error', after['error'])

    def test_nonblocking_read_and_single_flight(self):
        entered, release = threading.Event(), threading.Event()
        calls = []
        def reader():
            calls.append(1); entered.set(); release.wait(2); return {'items': []}
        cache = BackgroundSnapshot(reader)
        worker = threading.Thread(target=cache.refresh); worker.start(); entered.wait(1)
        self.assertTrue(cache.snapshot()['syncing'])
        cache.refresh(); self.assertEqual(len(calls), 1)
        release.set(); worker.join()
        self.assertFalse(cache.snapshot()['syncing'])


if __name__ == '__main__':
    unittest.main()
