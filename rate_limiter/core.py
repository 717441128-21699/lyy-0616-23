import time
import math
import threading
from typing import Tuple, Optional
from dataclasses import dataclass, field


@dataclass
class RateLimitResult:
    allowed: bool
    remaining: int
    limit: int
    retry_after: float = 0.0
    timestamp: float = field(default_factory=time.time)


class TokenBucket:
    def __init__(self, rate: float, capacity: int, initial_tokens: Optional[int] = None):
        self.rate = rate
        self.capacity = capacity
        self.tokens = float(initial_tokens if initial_tokens is not None else capacity)
        self.last_refill_time = time.time()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self.last_refill_time
        if elapsed > 0:
            new_tokens = elapsed * self.rate
            self.tokens = min(self.capacity, self.tokens + new_tokens)
            self.last_refill_time = now

    def try_consume(self, tokens: int = 1) -> RateLimitResult:
        with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return RateLimitResult(
                    allowed=True,
                    remaining=int(self.tokens),
                    limit=self.capacity,
                    retry_after=0.0
                )
            else:
                needed = tokens - self.tokens
                if self.rate > 0:
                    retry_after = needed / self.rate
                else:
                    retry_after = float('inf')
                return RateLimitResult(
                    allowed=False,
                    remaining=int(self.tokens),
                    limit=self.capacity,
                    retry_after=retry_after
                )

    def peek(self) -> int:
        with self._lock:
            self._refill()
            return int(self.tokens)

    def add_tokens(self, count: int) -> None:
        with self._lock:
            self.tokens = min(self.capacity, self.tokens + count)

    def set_capacity(self, new_capacity: int) -> None:
        with self._lock:
            self.capacity = new_capacity
            if self.tokens > new_capacity:
                self.tokens = float(new_capacity)


class SlidingWindow:
    def __init__(self, window_size: float, limit: int, bucket_count: int = 10):
        self.window_size = window_size
        self.limit = limit
        self.bucket_count = bucket_count
        self.bucket_duration = window_size / bucket_count
        self.buckets: dict[int, int] = {}
        self.current_bucket: int = 0
        self._lock = threading.Lock()
        self._initialize_buckets()

    def _initialize_buckets(self) -> None:
        now = time.time()
        self.current_bucket = self._get_bucket_key(now)
        for i in range(self.bucket_count):
            self.buckets[self.current_bucket - i] = 0

    def _get_bucket_key(self, timestamp: float) -> int:
        return math.floor(timestamp / self.bucket_duration)

    def _cleanup_old_buckets(self, now: float) -> None:
        current_key = self._get_bucket_key(now)
        window_start_time = now - self.window_size
        oldest_allowed_key = self._get_bucket_key(window_start_time) - 1

        keys_to_remove = [k for k in self.buckets if k < oldest_allowed_key]
        for k in keys_to_remove:
            del self.buckets[k]

        for i in range(self.bucket_count + 2):
            key = current_key - i
            if key not in self.buckets:
                self.buckets[key] = 0

    def _get_window_count(self, now: float) -> Tuple[int, float]:
        current_key = self._get_bucket_key(now)
        window_start_time = now - self.window_size
        oldest_bucket_key = self._get_bucket_key(window_start_time)

        oldest_bucket_start = oldest_bucket_key * self.bucket_duration
        overlap = (oldest_bucket_key + 1) * self.bucket_duration - window_start_time
        oldest_weight = overlap / self.bucket_duration if self.bucket_duration > 0 else 1.0
        oldest_weight = max(0.0, min(1.0, oldest_weight))

        total = 0.0
        for key in range(oldest_bucket_key, current_key + 1):
            count = self.buckets.get(key, 0)
            if key == oldest_bucket_key:
                total += count * oldest_weight
            else:
                total += count

        time_to_next_slot = (oldest_bucket_key + 1) * self.bucket_duration - now
        return int(total), max(0.001, time_to_next_slot)

    def try_acquire(self, count: int = 1, now: float = None) -> RateLimitResult:
        if now is None:
            now = time.time()
        with self._lock:
            self._cleanup_old_buckets(now)

            current_count, time_to_next = self._get_window_count(now)
            bucket_key = self._get_bucket_key(now)

            if current_count + count <= self.limit:
                self.buckets[bucket_key] = self.buckets.get(bucket_key, 0) + count
                return RateLimitResult(
                    allowed=True,
                    remaining=self.limit - current_count - count,
                    limit=self.limit,
                    retry_after=0.0
                )
            else:
                return RateLimitResult(
                    allowed=False,
                    remaining=max(0, self.limit - current_count),
                    limit=self.limit,
                    retry_after=time_to_next
                )

    def rollback_last(self, count: int = 1, now: float = None) -> None:
        if now is None:
            now = time.time()
        with self._lock:
            bucket_key = self._get_bucket_key(now)
            if bucket_key in self.buckets:
                self.buckets[bucket_key] = max(0, self.buckets[bucket_key] - count)

    def get_current_count(self) -> int:
        with self._lock:
            now = time.time()
            self._cleanup_old_buckets(now)
            count, _ = self._get_window_count(now)
            return count

    def reset(self) -> None:
        with self._lock:
            self.buckets.clear()
            self._initialize_buckets()
