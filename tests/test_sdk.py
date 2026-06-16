import time
import asyncio
import pytest
from rate_limiter.sdk import (
    RateLimiterClient,
    AsyncRateLimiterClient,
    create_rate_limiter,
    create_async_rate_limiter
)
from rate_limiter.coordinator import CoordinationMode, DegradationMode
from rate_limiter.storage import InMemoryStorage
from rate_limiter.exceptions import QuotaExceededError


class TestRateLimiterClient:
    def test_create_with_in_memory_storage(self):
        client = RateLimiterClient(
            global_limit=100,
            window_size=1.0
        )
        assert isinstance(client.storage, InMemoryStorage)
        assert client.coordinator.global_limit == 100

    def test_create_with_custom_storage(self):
        storage = InMemoryStorage()
        client = RateLimiterClient(
            global_limit=100,
            storage=storage
        )
        assert client.storage is storage

    def test_try_acquire_success(self):
        client = RateLimiterClient(
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )
        result = client.try_acquire(1)
        assert result.allowed is True
        assert result.remaining == 9

    def test_try_acquire_failure(self):
        client = RateLimiterClient(
            global_limit=5,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )
        for _ in range(5):
            client.try_acquire(1)

        result = client.try_acquire(1)
        assert result.allowed is False

    def test_acquire_raises_exception(self):
        client = RateLimiterClient(
            global_limit=5,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )
        for _ in range(5):
            client.acquire(1)

        with pytest.raises(QuotaExceededError):
            client.acquire(1)

    def test_context_manager(self):
        client = RateLimiterClient(
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        with client.limit() as result:
            assert result.allowed is True
            assert result.remaining == 9

    def test_context_manager_raises(self):
        client = RateLimiterClient(
            global_limit=5,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(5):
            with client.limit():
                pass

        with pytest.raises(QuotaExceededError):
            with client.limit():
                pass

    def test_context_manager_no_raise(self):
        client = RateLimiterClient(
            global_limit=5,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(5):
            with client.limit():
                pass

        with client.limit(raise_on_exceed=False) as result:
            assert result.allowed is False

    def test_decorator(self):
        client = RateLimiterClient(
            global_limit=5,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        @client.decorate()
        def limited_function():
            return "success"

        for _ in range(5):
            assert limited_function() == "success"

        with pytest.raises(QuotaExceededError):
            limited_function()

    def test_wait_for_token(self):
        client = RateLimiterClient(
            global_limit=5,
            window_size=0.2,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(5):
            client.try_acquire(1)

        start = time.time()
        result = client.wait_for_token(1, max_wait=0.5)
        elapsed = time.time() - start

        assert result is True
        assert elapsed >= 0.1

    def test_wait_for_token_timeout(self):
        client = RateLimiterClient(
            global_limit=5,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(5):
            client.try_acquire(1)

        result = client.wait_for_token(1, max_wait=0.1)
        assert result is False

    def test_get_stats(self):
        client = RateLimiterClient(
            global_limit=100,
            window_size=1.0
        )
        client.try_acquire(5)

        stats = client.get_stats()
        assert stats["global_limit"] == 100
        assert stats["local_count"] == 5

    def test_is_degraded(self):
        storage = InMemoryStorage()
        client = RateLimiterClient(
            global_limit=100,
            storage=storage,
            health_check_interval=0.1
        )

        assert client.is_degraded() is False

        storage.set_available(False)
        time.sleep(0.15)

        assert client.is_degraded() is True

    def test_context_manager_resource(self):
        with RateLimiterClient(global_limit=100) as client:
            assert client is not None
            result = client.try_acquire(1)
            assert result.allowed is True

    def test_create_rate_limiter_helper(self):
        client = create_rate_limiter(limit=100, per_second=True)
        assert client.coordinator.global_limit == 100
        assert client.coordinator.window_size == 1.0

    def test_create_rate_limiter_custom_window(self):
        client = create_rate_limiter(limit=1000, per_second=False, window_size=60.0)
        assert client.coordinator.global_limit == 1000
        assert client.coordinator.window_size == 60.0

    def test_prefetch_mode_in_sdk(self):
        client = RateLimiterClient(
            global_limit=100,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            prefetch_ratio=0.2,
            min_prefetch=10
        )

        for i in range(50):
            result = client.try_acquire(1)
            assert result.allowed is True

        stats = client.get_stats()
        assert stats["has_lease"] is True

    def test_degradation_mode_config(self):
        client = RateLimiterClient(
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST,
            degradation_mode=DegradationMode.FAIL_OPEN,
            health_check_interval=0.1
        )

        client.storage.set_available(False)
        time.sleep(0.15)

        for _ in range(100):
            result = client.try_acquire(1)
            assert result.allowed is True


class TestAsyncRateLimiterClient:
    @pytest.mark.asyncio
    async def test_async_try_acquire(self):
        client = AsyncRateLimiterClient(
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        for i in range(10):
            result = await client.try_acquire(1)
            assert result.allowed is True
            assert result.remaining == 9 - i

        result = await client.try_acquire(1)
        assert result.allowed is False

    @pytest.mark.asyncio
    async def test_async_acquire_raises(self):
        client = AsyncRateLimiterClient(
            global_limit=5,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(5):
            await client.acquire(1)

        with pytest.raises(QuotaExceededError):
            await client.acquire(1)

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        client = AsyncRateLimiterClient(
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        async with client.limit() as result:
            assert result.allowed is True

    @pytest.mark.asyncio
    async def test_async_decorator(self):
        client = AsyncRateLimiterClient(
            global_limit=5,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        @client.decorate()
        async def limited_function():
            return "success"

        for _ in range(5):
            assert await limited_function() == "success"

        with pytest.raises(QuotaExceededError):
            await limited_function()

    @pytest.mark.asyncio
    async def test_async_wait_for_token(self):
        client = AsyncRateLimiterClient(
            global_limit=5,
            window_size=0.2,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(5):
            await client.try_acquire(1)

        start = time.time()
        result = await client.wait_for_token(1, max_wait=0.5)
        elapsed = time.time() - start

        assert result is True
        assert elapsed >= 0.1

    @pytest.mark.asyncio
    async def test_async_resource_manager(self):
        async with AsyncRateLimiterClient(global_limit=100) as client:
            assert client is not None
            result = await client.try_acquire(1)
            assert result.allowed is True

    @pytest.mark.asyncio
    async def test_create_async_rate_limiter_helper(self):
        client = create_async_rate_limiter(limit=100, per_second=True)
        assert client.coordinator.global_limit == 100
        assert client.coordinator.window_size == 1.0

    @pytest.mark.asyncio
    async def test_async_degradation(self):
        storage = InMemoryStorage()
        client = AsyncRateLimiterClient(
            global_limit=10,
            storage=storage,
            mode=CoordinationMode.PER_REQUEST,
            degradation_mode=DegradationMode.LOCAL_LIMIT,
            health_check_interval=0.1
        )

        storage.set_available(False)
        await asyncio.sleep(0.15)

        result = await client.try_acquire(1)
        assert result.allowed is True
        assert client.is_degraded() is True


class TestIntegration:
    def test_multiple_clients_shared_storage(self):
        storage = InMemoryStorage()

        client1 = RateLimiterClient(
            global_limit=20,
            storage=storage,
            mode=CoordinationMode.PER_REQUEST,
            instance_id="client1"
        )
        client2 = RateLimiterClient(
            global_limit=20,
            storage=storage,
            mode=CoordinationMode.PER_REQUEST,
            instance_id="client2"
        )

        total_allowed = 0
        for _ in range(15):
            if client1.try_acquire(1).allowed:
                total_allowed += 1
            if client2.try_acquire(1).allowed:
                total_allowed += 1

        assert 18 <= total_allowed <= 22

    def test_prefetch_vs_per_request_latency(self):
        storage = InMemoryStorage()

        prefetch_client = RateLimiterClient(
            global_limit=1000,
            storage=storage,
            mode=CoordinationMode.PRE_FETCH,
            prefetch_ratio=0.1,
            min_prefetch=100
        )

        per_request_client = RateLimiterClient(
            global_limit=1000,
            storage=storage,
            mode=CoordinationMode.PER_REQUEST,
            instance_id="per_request"
        )

        start = time.time()
        for _ in range(500):
            prefetch_client.try_acquire(1)
        prefetch_time = time.time() - start

        start = time.time()
        for _ in range(500):
            per_request_client.try_acquire(1)
        per_request_time = time.time() - start

        assert prefetch_time < per_request_time
