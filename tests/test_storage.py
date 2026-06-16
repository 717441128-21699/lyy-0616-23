import time
import pytest
from rate_limiter.storage import InMemoryStorage, StorageResult
from rate_limiter.exceptions import StorageUnavailableError


class TestInMemoryStorage:
    def test_initialization(self):
        storage = InMemoryStorage()
        assert storage.is_available() is True

    def test_get_and_set(self):
        storage = InMemoryStorage()
        result = storage.set("test_key", "test_value")
        assert result.success is True

        result = storage.get("test_key")
        assert result.success is True
        assert result.value == "test_value"

    def test_get_nonexistent_key(self):
        storage = InMemoryStorage()
        result = storage.get("nonexistent")
        assert result.success is True
        assert result.value is None

    def test_set_with_ttl(self):
        storage = InMemoryStorage()
        result = storage.set("ttl_key", "value", ttl=0.1)
        assert result.success is True

        result = storage.get("ttl_key")
        assert result.value == "value"

        time.sleep(0.15)
        result = storage.get("ttl_key")
        assert result.value is None

    def test_incrby(self):
        storage = InMemoryStorage()
        result = storage.incrby("counter", 1)
        assert result.success is True
        assert result.value == 1

        result = storage.incrby("counter", 5)
        assert result.value == 6

    def test_incrby_with_ttl(self):
        storage = InMemoryStorage()
        result = storage.incrby("ttl_counter", 1, ttl=0.1)
        assert result.value == 1

        time.sleep(0.15)
        result = storage.incrby("ttl_counter", 1)
        assert result.value == 1

    def test_eval_limit_and_incr(self):
        storage = InMemoryStorage()
        script = "INCR LIMIT script"
        keys = ["test_limit"]
        args = ["10", "60"]

        for i in range(10):
            result = storage.eval(script, keys, args)
            assert result.success is True
            assert result.value[0] == 1
            assert result.value[1] == 9 - i

        result = storage.eval(script, keys, args)
        assert result.value[0] == 0
        assert result.value[1] == 0

    def test_unavailable_storage(self):
        storage = InMemoryStorage()
        storage.set_available(False)
        assert storage.is_available() is False

        with pytest.raises(StorageUnavailableError):
            storage.get("any_key")

        with pytest.raises(StorageUnavailableError):
            storage.set("any_key", "value")

        with pytest.raises(StorageUnavailableError):
            storage.incrby("any_key", 1)

        with pytest.raises(StorageUnavailableError):
            storage.eval("script", [], [])

    def test_set_available_back(self):
        storage = InMemoryStorage()
        storage.set_available(False)
        assert storage.is_available() is False

        storage.set_available(True)
        assert storage.is_available() is True

        result = storage.set("key", "value")
        assert result.success is True

    def test_cleanup_expired(self):
        storage = InMemoryStorage()
        storage.set("key1", "value1", ttl=0.01)
        storage.set("key2", "value2", ttl=100)

        time.sleep(0.05)
        result = storage.get("key1")
        assert result.value is None
        result = storage.get("key2")
        assert result.value == "value2"

    @pytest.mark.asyncio
    async def test_async_operations(self):
        storage = InMemoryStorage()

        result = await storage.aset("async_key", "async_value")
        assert result.success is True

        result = await storage.aget("async_key")
        assert result.value == "async_value"

        result = await storage.aincrby("async_counter", 5)
        assert result.value == 5

        result = await storage.aeval("INCR LIMIT script", ["async_limit"], ["10", "60"])
        assert result.value[0] == 1

    def test_concurrent_access(self):
        import threading
        storage = InMemoryStorage()
        results = []

        def increment():
            for _ in range(100):
                result = storage.incrby("concurrent", 1)
                results.append(result.value)

        threads = [threading.Thread(target=increment) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        result = storage.get("concurrent")
        assert result.value == 1000
