"""Rate-limited, caching HTTP client for EDGAR. Throwaway smoke-test quality."""
import os
import time
import threading
import requests

# SEC requires every request to declare a real contact:
# "FirstName LastName email@domain.com". Deliberately not defaulted to anyone's
# address — set SEC_UA in the environment, or put it in a local .env (gitignored).
UA_ENV = "SEC_UA"


def get_user_agent():
    ua = os.environ.get(UA_ENV, "").strip()
    if not ua:
        raise RuntimeError(
            f"{UA_ENV} is not set. SEC requires a contact in the User-Agent.\n"
            f'Run:  export {UA_ENV}="Your Name you@example.com"\n'
            f"or put that line in edgar-smoketest/.env"
        )
    return ua


class TokenBucket:
    """Thread-safe token bucket. Global cap across all workers."""

    def __init__(self, rate, capacity=None):
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else rate)
        self.tokens = self.capacity
        self.last = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.monotonic()
                self.tokens = min(self.capacity,
                                  self.tokens + (now - self.last) * self.rate)
                self.last = now
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                wait = (1.0 - self.tokens) / self.rate
            time.sleep(wait)


class EdgarClient:
    def __init__(self, bucket, cache_dir="cache", ua=None):
        self.bucket = bucket
        self.cache_dir = cache_dir
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": ua or get_user_agent(),
            "Accept-Encoding": "gzip, deflate",
        })  # NB: no hardcoded Host — we also hit data.sec.gov
        os.makedirs(cache_dir, exist_ok=True)
        # counters (accessed under lock for honest stats)
        self._c_lock = threading.Lock()
        self.net_hits = 0
        self.cache_hits = 0
        self.not_found = 0
        self.retries = 0

    def _bump(self, attr):
        with self._c_lock:
            setattr(self, attr, getattr(self, attr) + 1)

    # SEC/S3 returns 403 AccessDenied (not 404) for missing daily-index files
    # and not-yet-posted dates; treat both as "not there, skip silently".
    NOT_FOUND = (403, 404)

    def fetch(self, url, cache_path, force=False, retries=3):
        """Return (text, from_cache). text is None on 403/404."""
        if not force and os.path.exists(cache_path):
            with open(cache_path, "r", encoding="utf-8", errors="replace") as f:
                self._bump("cache_hits")
                return f.read(), True

        last_err = None
        for attempt in range(retries):
            self.bucket.acquire()
            try:
                resp = self.session.get(url, timeout=45)
            except requests.RequestException as e:
                last_err = e
                time.sleep(1.5 * (attempt + 1))
                continue
            if resp.status_code in self.NOT_FOUND:
                self._bump("not_found")
                return None, False
            if resp.status_code == 429 or resp.status_code >= 500:
                # throttled or transient server error: back off and retry
                self._bump("retries")
                time.sleep(2.0 * (attempt + 1))
                continue
            resp.raise_for_status()
            text = resp.text
            os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
            tmp = cache_path + ".tmp"
            with open(tmp, "w", encoding="utf-8", errors="replace") as f:
                f.write(text)
            os.replace(tmp, cache_path)  # atomic write -> resumable
            self._bump("net_hits")
            return text, False
        raise last_err if last_err else RuntimeError(f"exhausted retries: {url}")
