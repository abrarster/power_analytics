from datetime import datetime
import time
import threading
from dagster import ConfigurableResource, InitResourceContext
from typing import Optional


class RateLimiterClient:
    def __init__(self, calls_per_minute: int):
        self.calls_per_minute = calls_per_minute
        self._lock = threading.Lock()
        self._calls = []

    def wait_for_capacity(self):
        with self._lock:
            now = datetime.now()
            # Remove calls older than 1 minute
            self._calls = [t for t in self._calls if (now - t).total_seconds() < 60]

            if len(self._calls) >= self.calls_per_minute:
                # Wait until the oldest call is more than 1 minute old
                sleep_time = 60 - (now - self._calls[0]).total_seconds()
                if sleep_time > 0:
                    time.sleep(sleep_time)
                # Clear old calls after waiting
                self._calls = [t for t in self._calls if (now - t).total_seconds() < 60]

            self._calls.append(now)


class RateLimiter(ConfigurableResource):
    calls_per_minute: int = 200
    _client: Optional[RateLimiterClient] = None

    def setup_for_execution(self, context: InitResourceContext) -> None:
        """This method is called by Dagster during resource initialization"""
        self._client = RateLimiterClient(calls_per_minute=self.calls_per_minute)

    def wait_for_capacity(self):
        """Delegate to the client's wait_for_capacity method"""
        if self._client is None:
            raise RuntimeError("RateLimiter client not initialized")
        return self._client.wait_for_capacity()
