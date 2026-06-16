import time
import pytest
from rate_limiter.core import TokenBucket, SlidingWindow, RateLimitResult


class TestTokenBucket:
    def test_initialization(self):
        bucket = TokenBucket(rate=10, capacity=100)
        assert bucket.rate == 10
        assert bucket.capacity == 100
        assert bucket.peek() == 100

    def test_initial_tokens(self):
        bucket = TokenBucket(rate=10, capacity=100, initial_tokens=50)
        assert bucket.peek() == 50

    def test_consume_success(self):
        bucket = TokenBucket(rate=10, capacity=100)
        result = bucket.try_consume(1)
        assert result.allowed is True
        assert result.remaining == 99
        assert result.limit == 100
        assert bucket.peek() == 99

    def test_consume_multiple(self):
        bucket = TokenBucket(rate=10, capacity=100)
        result = bucket.try_consume(10)
        assert result.allowed is True
        assert result.remaining == 90
        assert bucket.peek() == 90

    def test_consume_exceeds_capacity(self):
        bucket = TokenBucket(rate=10, capacity=100)
        result = bucket.try_consume(150)
        assert result.allowed is False
        assert result.retry_after > 0
        assert bucket.peek() == 100

    def test_refill_over_time(self):
        bucket = TokenBucket(rate=100, capacity=100, initial_tokens=0)
        result = bucket.try_consume(1)
        assert result.allowed is False

        time.sleep(0.02)
        result = bucket.try_consume(1)
        assert result.allowed is True
        assert result.remaining >= 0

    def test_retry_after_calculation(self):
        bucket = TokenBucket(rate=10, capacity=100, initial_tokens=5)
        result = bucket.try_consume(10)
        assert result.allowed is False
        assert abs(result.retry_after - 0.5) < 0.01

    def test_add_tokens(self):
        bucket = TokenBucket(rate=10, capacity=100, initial_tokens=0)
        bucket.add_tokens(50)
        assert bucket.peek() == 50
        bucket.add_tokens(100)
        assert bucket.peek() == 100

    def test_set_capacity(self):
        bucket = TokenBucket(rate=10, capacity=100)
        bucket.set_capacity(50)
        assert bucket.capacity == 50
        assert bucket.peek() == 50

    def test_thread_safety(self):
        import threading
        bucket = TokenBucket(rate=1000, capacity=1000)
        results = []

        def consume():
            for _ in range(100):
                results.append(bucket.try_consume(1).allowed)

        threads = [threading.Thread(target=consume) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert sum(1 for r in results if r) == 1000


class TestSlidingWindow:
    def test_initialization(self):
        window = SlidingWindow(window_size=1.0, limit=100, bucket_count=10)
        assert window.window_size == 1.0
        assert window.limit == 100
        assert window.bucket_count == 10
        assert window.bucket_duration == 0.1

    def test_acquire_success(self):
        window = SlidingWindow(window_size=1.0, limit=100)
        result = window.try_acquire(1)
        assert result.allowed is True
        assert result.remaining == 99

    def test_acquire_multiple(self):
        window = SlidingWindow(window_size=1.0, limit=100)
        result = window.try_acquire(50)
        assert result.allowed is True
        assert result.remaining == 50

    def test_acquire_exceeds_limit(self):
        window = SlidingWindow(window_size=1.0, limit=10)
        for _ in range(10):
            result = window.try_acquire(1)
            assert result.allowed is True

        result = window.try_acquire(1)
        assert result.allowed is False
        assert result.retry_after > 0

    def test_window_rollover(self):
        window = SlidingWindow(window_size=0.1, limit=10, bucket_count=5)
        for _ in range(10):
            result = window.try_acquire(1)
            assert result.allowed is True

        result = window.try_acquire(1)
        assert result.allowed is False

        time.sleep(0.12)
        result = window.try_acquire(1)
        assert result.allowed is True

    def test_sliding_behavior(self):
        window = SlidingWindow(window_size=0.3, limit=10, bucket_count=3)

        for _ in range(10):
            window.try_acquire(1)

        time.sleep(0.35)
        result = window.try_acquire(3)
        assert result.allowed is True

    def test_get_current_count(self):
        window = SlidingWindow(window_size=1.0, limit=100)
        for _ in range(42):
            window.try_acquire(1)
        assert window.get_current_count() == 42

    def test_reset(self):
        window = SlidingWindow(window_size=1.0, limit=100)
        for _ in range(50):
            window.try_acquire(1)
        assert window.get_current_count() == 50
        window.reset()
        assert window.get_current_count() == 0

    def test_no_double_spike_at_boundary(self):
        window = SlidingWindow(window_size=0.1, limit=10, bucket_count=10)

        for _ in range(10):
            result = window.try_acquire(1)
            assert result.allowed is True

        time.sleep(0.05)
        result = window.try_acquire(1)
        assert result.allowed is False

        time.sleep(0.06)
        for _ in range(5):
            result = window.try_acquire(1)
            assert result.allowed is True

        result = window.try_acquire(6)
        assert result.allowed is False

    def test_thread_safety(self):
        import threading
        window = SlidingWindow(window_size=1.0, limit=1000)
        results = []

        def acquire():
            for _ in range(100):
                results.append(window.try_acquire(1).allowed)

        threads = [threading.Thread(target=acquire) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        success_count = sum(1 for r in results if r)
        assert 990 <= success_count <= 1010
