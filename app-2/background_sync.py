"""Read-only, single-flight snapshot polling independent of browser sessions."""
import copy
import threading
from datetime import datetime, timezone


class BackgroundSnapshot:
    def __init__(self, reader, interval=60):
        self.reader, self.interval = reader, interval
        self.lock = threading.Lock()
        self.wake = threading.Event()
        self.data = None
        self.synced_at = None
        self.error = None
        self.running = False
        self.thread = None

    def refresh(self):
        with self.lock:
            if self.running:
                return
            self.running = True
        try:
            result = self.reader()
            with self.lock:
                self.data = result
                self.synced_at = datetime.now(timezone.utc).isoformat()
                self.error = None
        except Exception:
            # Keep the previous complete snapshot; never replace it with partial data.
            with self.lock:
                self.error = 'Sync failed; last successful data retained. Check source connection.'
        finally:
            with self.lock:
                self.running = False

    def start(self):
        with self.lock:
            if self.thread:
                return
            self.thread = threading.Thread(target=self._loop, daemon=True, name='shipping-sync')
            self.thread.start()

    def _loop(self):
        while True:
            self.refresh()
            self.wake.wait(self.interval)
            self.wake.clear()

    def snapshot(self):
        with self.lock:
            return {'data': copy.deepcopy(self.data), 'last_synced': self.synced_at,
                    'syncing': self.running, 'error': self.error,
                    'interval_seconds': self.interval}

    def request_refresh(self):
        self.wake.set()
