import time
import asyncio
import pytest
from rate_limiter.coordinator import (
    DistributedCoordinator,
    CoordinationMode,
    DegradationMode
)
from rate_limiter.storage import InMemoryStorage
from rate_limiter.exceptions import QuotaExceededError, StorageUnavailableError


class TestDistributedCoordinator:
    def test_initialization(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=100,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH
        )
        assert coordinator.global_limit == 100
        assert coordinator.window_size == 1.0
        assert coordinator.mode == CoordinationMode.PRE_FETCH
        assert coordinator.instance_id is not None

    def test_per_request_mode(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        for i in range(10):
            result = coordinator.try_acquire(1)
            assert result.allowed is True
            assert result.remaining == 9 - i

        result = coordinator.try_acquire(1)
        assert result.allowed is False
        assert result.retry_after > 0

    def test_prefetch_mode(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=100,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            prefetch_ratio=0.1,
            min_prefetch=5
        )

        for i in range(50):
            result = coordinator.try_acquire(1)
            assert result.allowed is True

        stats = coordinator.get_stats()
        assert stats["has_lease"] is True
        assert stats["lease_quota"] >= 5
        assert stats["global_count"] >= 50
        assert "remaining" in stats
        assert stats["remaining"] >= 0

    def test_exceed_global_limit(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(10):
            coordinator.try_acquire(1)

        with pytest.raises(QuotaExceededError) as exc_info:
            coordinator.acquire(1)

        assert exc_info.value.limit == 10
        assert exc_info.value.retry_after > 0

    def test_window_rollover(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=0.1,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(10):
            coordinator.try_acquire(1)

        result = coordinator.try_acquire(1)
        assert result.allowed is False

        time.sleep(0.15)
        result = coordinator.try_acquire(1)
        assert result.allowed is True

    def test_no_double_spike_at_boundary(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=0.2,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(10):
            coordinator.try_acquire(1)

        time.sleep(0.1)
        result = coordinator.try_acquire(1)
        assert result.allowed is False

        time.sleep(0.11)
        for _ in range(5):
            result = coordinator.try_acquire(1)
            assert result.allowed is True

        result = coordinator.try_acquire(6)
        assert result.allowed is False

    def test_degradation_local_limit(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST,
            degradation_mode=DegradationMode.LOCAL_LIMIT,
            local_limit_ratio=1.5
        )

        storage.set_available(False)
        time.sleep(0.1)

        for _ in range(15):
            result = coordinator.try_acquire(1)
            assert result.allowed is True

        result = coordinator.try_acquire(1)
        assert result.allowed is False

    def test_degradation_fail_open(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST,
            degradation_mode=DegradationMode.FAIL_OPEN
        )

        storage.set_available(False)
        time.sleep(0.1)

        for _ in range(100):
            result = coordinator.try_acquire(1)
            assert result.allowed is True

    def test_degradation_fail_closed(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST,
            degradation_mode=DegradationMode.FAIL_CLOSED
        )

        storage.set_available(False)
        time.sleep(0.1)

        result = coordinator.try_acquire(1)
        assert result.allowed is False

    def test_recovery_from_degradation(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST,
            degradation_mode=DegradationMode.LOCAL_LIMIT,
            health_check_interval=0.1
        )

        storage.set_available(False)
        time.sleep(0.3)
        assert coordinator._degraded is True

        storage.set_available(True)
        time.sleep(0.3)

        for _ in range(10):
            result = coordinator.try_acquire(1)
            assert result.allowed is True

    def test_get_stats(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=100,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH
        )

        coordinator.try_acquire(1)

        stats = coordinator.get_stats()
        assert stats["global_limit"] == 100
        assert stats["degraded"] is False
        assert stats["mode"] == "pre_fetch"
        assert "local_count" in stats
        assert "global_count" in stats

    def test_multiple_instances(self):
        storage = InMemoryStorage()
        instance_ids = ["instance_1", "instance_2", "instance_3"]
        coordinators = [
            DistributedCoordinator(
                storage=storage,
                global_limit=30,
                window_size=1.0,
                mode=CoordinationMode.PER_REQUEST,
                instance_id=inst_id
            )
            for inst_id in instance_ids
        ]

        total_allowed = 0
        for _ in range(15):
            for coord in coordinators:
                result = coord.try_acquire(1)
                if result.allowed:
                    total_allowed += 1

        assert 28 <= total_allowed <= 32

    def test_blocking_acquire(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=5,
            window_size=0.2,
            mode=CoordinationMode.PER_REQUEST
        )

        for _ in range(5):
            coordinator.try_acquire(1)

        start = time.time()
        result = coordinator.acquire(1, block=True, timeout=0.5)
        elapsed = time.time() - start

        assert result.allowed is True
        assert elapsed >= 0.1

    def test_consume_multiple_tokens(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=100,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        result = coordinator.try_acquire(50)
        assert result.allowed is True
        assert result.remaining == 50

        result = coordinator.try_acquire(50)
        assert result.allowed is True
        assert result.remaining == 0

        result = coordinator.try_acquire(1)
        assert result.allowed is False

    def test_prefetch_lease_return(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=100,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            prefetch_ratio=0.2,
            min_prefetch=10,
            lease_ttl=0.1
        )

        coordinator.try_acquire(5)
        assert coordinator._current_lease is not None
        lease_quota = coordinator._current_lease.quota

        time.sleep(0.15)
        coordinator.try_acquire(1)

        assert coordinator._current_lease.quota != lease_quota or coordinator._current_lease.acquired_at > lease_quota

    def test_close(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=100,
            window_size=1.0
        )

        coordinator.try_acquire(1)
        coordinator.close()

        assert coordinator._sync_thread is not None

    @pytest.mark.asyncio
    async def test_async_acquire(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        for i in range(10):
            result = await coordinator.atry_acquire(1)
            assert result.allowed is True
            assert result.remaining == 9 - i

        result = await coordinator.atry_acquire(1)
        assert result.allowed is False

    @pytest.mark.asyncio
    async def test_async_degradation(self):
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=10,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST,
            degradation_mode=DegradationMode.LOCAL_LIMIT,
            health_check_interval=0.1
        )

        storage.set_available(False)
        await asyncio.sleep(0.3)

        result = await coordinator.atry_acquire(1)
        assert result.allowed is True
        assert coordinator._degraded is True

    def test_concurrent_acquire(self):
        import threading
        storage = InMemoryStorage()
        coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=1000,
            window_size=1.0,
            mode=CoordinationMode.PER_REQUEST
        )

        results = []

        def acquire():
            for _ in range(100):
                result = coordinator.try_acquire(1)
                results.append(result.allowed)

        threads = [threading.Thread(target=acquire) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        success_count = sum(1 for r in results if r)
        assert 990 <= success_count <= 1010
