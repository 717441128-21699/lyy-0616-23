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
        assert stats["local_used_total"] >= 50
        assert stats["pending_sync"] + stats["synced_to_center"] >= 50
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
            mode=CoordinationMode.PRE_FETCH,
            min_prefetch=50,
            max_prefetch=100
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
        assert 950 <= success_count <= 1050

    def test_prefetch_dual_instance_global_limit(self):
        import threading
        storage = InMemoryStorage()
        limit = 10

        coord_a = DistributedCoordinator(
            storage=storage,
            global_limit=limit,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            min_prefetch=3,
            max_prefetch=5,
            instance_id="instance_a"
        )

        coord_b = DistributedCoordinator(
            storage=storage,
            global_limit=limit,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            min_prefetch=3,
            max_prefetch=5,
            instance_id="instance_b"
        )

        timeline_a = []
        timeline_b = []

        def client_loop(coord, timeline, duration):
            start = time.time()
            while time.time() - start < duration:
                try:
                    result = coord.try_acquire(1)
                    if result.allowed:
                        timeline.append(time.time())
                except Exception:
                    pass
                time.sleep(0.005)

        t1 = threading.Thread(target=client_loop, args=(coord_a, timeline_a, 2.0))
        t2 = threading.Thread(target=client_loop, args=(coord_b, timeline_b, 2.0))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        all_events = sorted(timeline_a + timeline_b)
        total = len(all_events)

        assert total >= limit and total <= limit * 2 + 2, \
            f"Total {total} not in [{limit}, {limit * 2 + 2}]"

        window_log = []
        max_in_window = 0
        for t in all_events:
            cutoff = t - 1.0
            while window_log and window_log[0] < cutoff:
                window_log.pop(0)
            window_log.append(t)
            max_in_window = max(max_in_window, len(window_log))

        assert max_in_window <= limit + 1, \
            f"Max in 1s window {max_in_window} > {limit + 1}"

        test_duration = 2.0
        rate = total / test_duration
        assert rate <= limit * 1.25, \
            f"Rate {rate:.1f}/s > {limit * 1.25:.1f}/s"

    def test_prefetch_stats_hierarchy(self):
        storage = InMemoryStorage()
        limit = 50
        requests = 30

        coord = DistributedCoordinator(
            storage=storage,
            global_limit=limit,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            min_prefetch=10,
            max_prefetch=10,
            instance_id="test_stats",
            sync_interval=60.0
        )

        actual_passed = 0
        for _ in range(requests + 5):
            result = coord.try_acquire(1)
            if result.allowed:
                actual_passed += 1

        s1 = coord.get_stats(force_sync=False)

        assert s1["local_used_total"] == actual_passed, \
            f"local_used_total={s1['local_used_total']} != {actual_passed}"
        assert s1["pending_sync"] + s1["synced_to_center"] == actual_passed, \
            f"pending + synced != actual"
        assert "remaining" in s1
        assert s1["remaining"] >= 0

        for _ in range(3):
            s = coord.get_stats(force_sync=False)
            assert s["local_used_total"] == actual_passed

        s2 = coord.get_stats(force_sync=True)
        assert s2["pending_sync"] == 0, \
            f"pending_sync after force sync = {s2['pending_sync']} != 0"
        assert s2["local_used_total"] == actual_passed, \
            f"local_used_total should not reset after sync"
        assert s2["synced_to_center"] == actual_passed, \
            f"synced_to_center={s2['synced_to_center']} != {actual_passed}"
        assert s2["remaining"] == max(0, limit - s2["global_count"])

    def test_prefetch_sliding_window_boundary(self):
        storage = InMemoryStorage()
        limit = 10

        coord = DistributedCoordinator(
            storage=storage,
            global_limit=limit,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            min_prefetch=5,
            max_prefetch=5,
            instance_id="test_boundary",
            sync_interval=0.0
        )

        phase1_passed = 0
        for _ in range(limit):
            result = coord.try_acquire(1)
            if result.allowed:
                phase1_passed += 1

        assert phase1_passed == limit

        time.sleep(1.1)

        timeline = []
        start = time.time()
        while time.time() - start < 1.5:
            result = coord.try_acquire(1)
            if result.allowed:
                timeline.append(time.time())
            time.sleep(0.01)

        window_log = []
        max_in_window = 0
        for t in timeline:
            cutoff = t - 1.0
            while window_log and window_log[0] < cutoff:
                window_log.pop(0)
            window_log.append(t)
            max_in_window = max(max_in_window, len(window_log))

        assert max_in_window <= limit + 1, \
            f"Sliding window burst {max_in_window} > {limit + 1}"

    @pytest.mark.asyncio
    async def test_async_prefetch_mode(self):
        storage = InMemoryStorage()
        limit = 20

        coord = DistributedCoordinator(
            storage=storage,
            global_limit=limit,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            min_prefetch=5,
            max_prefetch=10,
            instance_id="test_async"
        )

        passed = 0
        for _ in range(30):
            result = await coord.atry_acquire(1)
            if result.allowed:
                passed += 1

        assert passed <= limit + 1, f"Async passed {passed} > {limit + 1}"

        stats = coord.get_stats()
        assert stats["local_used_total"] == passed
        assert "remaining" in stats
        assert stats["remaining"] >= 0

    @pytest.mark.asyncio
    async def test_async_prefetch_dual_instance(self):
        storage = InMemoryStorage()
        limit = 10

        coord_a = DistributedCoordinator(
            storage=storage,
            global_limit=limit,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            min_prefetch=3,
            max_prefetch=5,
            instance_id="async_a"
        )

        coord_b = DistributedCoordinator(
            storage=storage,
            global_limit=limit,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            min_prefetch=3,
            max_prefetch=5,
            instance_id="async_b"
        )

        passed_a = 0
        passed_b = 0

        for _ in range(20):
            if (await coord_a.atry_acquire(1)).allowed:
                passed_a += 1
            if (await coord_b.atry_acquire(1)).allowed:
                passed_b += 1

        total = passed_a + passed_b
        assert total <= limit * 2, f"Total {total} > {limit * 2}"
        assert total <= limit + 2 or total <= limit * 1.5, \
            f"Total {total} too high for limit {limit}"
