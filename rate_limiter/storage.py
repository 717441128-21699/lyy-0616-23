import time
import asyncio
import threading
from abc import ABC, abstractmethod
from typing import Optional, Tuple, Dict, Any
from dataclasses import dataclass

try:
    import redis
    import redis.asyncio as aioredis
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

from .exceptions import StorageUnavailableError


@dataclass
class StorageResult:
    success: bool
    value: Any = None
    error: Optional[str] = None


class BaseStorage(ABC):
    @abstractmethod
    def get(self, key: str) -> StorageResult:
        pass

    @abstractmethod
    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> StorageResult:
        pass

    @abstractmethod
    def incrby(self, key: str, amount: int, ttl: Optional[float] = None) -> StorageResult:
        pass

    @abstractmethod
    def eval(self, script: str, keys: list, args: list) -> StorageResult:
        pass

    @abstractmethod
    def is_available(self) -> bool:
        pass

    @abstractmethod
    async def aget(self, key: str) -> StorageResult:
        pass

    @abstractmethod
    async def aset(self, key: str, value: Any, ttl: Optional[float] = None) -> StorageResult:
        pass

    @abstractmethod
    async def aincrby(self, key: str, amount: int, ttl: Optional[float] = None) -> StorageResult:
        pass

    @abstractmethod
    async def aeval(self, script: str, keys: list, args: list) -> StorageResult:
        pass


class InMemoryStorage(BaseStorage):
    def __init__(self):
        self._data: Dict[str, Tuple[Any, Optional[float]]] = {}
        self._lock = threading.Lock()
        self._available = True

    def _cleanup_expired(self) -> None:
        now = time.time()
        expired_keys = [
            k for k, (_, expire_at) in self._data.items()
            if expire_at is not None and now >= expire_at
        ]
        for k in expired_keys:
            del self._data[k]

    def get(self, key: str) -> StorageResult:
        if not self._available:
            raise StorageUnavailableError("In-memory storage is unavailable")
        with self._lock:
            self._cleanup_expired()
            value, expire_at = self._data.get(key, (None, None))
            return StorageResult(success=True, value=value)

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> StorageResult:
        if not self._available:
            raise StorageUnavailableError("In-memory storage is unavailable")
        with self._lock:
            self._cleanup_expired()
            expire_at = time.time() + ttl if ttl is not None else None
            self._data[key] = (value, expire_at)
            return StorageResult(success=True)

    def incrby(self, key: str, amount: int, ttl: Optional[float] = None) -> StorageResult:
        if not self._available:
            raise StorageUnavailableError("In-memory storage is unavailable")
        with self._lock:
            self._cleanup_expired()
            current, expire_at = self._data.get(key, (0, None))
            if expire_at is not None and time.time() >= expire_at:
                current = 0
            new_value = int(current) + amount
            new_expire = time.time() + ttl if ttl is not None else expire_at
            self._data[key] = (new_value, new_expire)
            return StorageResult(success=True, value=new_value)

    def eval(self, script: str, keys: list, args: list) -> StorageResult:
        if not self._available:
            raise StorageUnavailableError("In-memory storage is unavailable")
        with self._lock:
            self._cleanup_expired()

            if "SLIDING_WINDOW" in script.upper() or "bucket_duration" in script:
                return self._eval_sliding_window(keys, args)

            if "PREFETCH" in script.upper() or "lease" in script.lower():
                return self._eval_prefetch(keys, args)

            if "RETURN" in script.upper() and "lease" in script.lower():
                return self._eval_return_lease(keys, args)

            if "WINDOW_SYNC" in script.upper() or "sync" in script.lower():
                return self._eval_window_sync(keys, args)

            if "INCR" in script.upper() and "LIMIT" in script.upper():
                key = keys[0]
                limit = int(args[0])
                ttl = int(args[1])
                amount = int(args[2]) if len(args) > 2 else 1
                current, expire_at = self._data.get(key, (0, None))
                if expire_at is not None and time.time() >= expire_at:
                    current = 0
                if current + amount <= limit:
                    new_value = current + amount
                    self._data[key] = (new_value, time.time() + ttl)
                    return StorageResult(success=True, value=[1, limit - new_value])
                else:
                    return StorageResult(success=True, value=[0, limit - current])

            return StorageResult(success=True, value=None)

    def _eval_sliding_window(self, keys: list, args: list) -> StorageResult:
        import math

        hash_key = keys[0]
        limit = float(args[0])
        window_size = float(args[1])
        bucket_count = int(args[2])
        amount = float(args[3])
        now = float(args[4])

        bucket_duration = window_size / bucket_count
        window_start = now - window_size
        current_bucket = math.floor(now / bucket_duration)

        buckets, expire_at = self._data.get(hash_key, ({}, None))
        if expire_at is not None and time.time() >= expire_at:
            buckets = {}

        total = 0.0
        oldest_bucket_key = None
        oldest_count = 0
        expired_keys = []

        for bucket_key_str, count in buckets.items():
            bucket_key = int(bucket_key_str)
            bucket_start = bucket_key * bucket_duration

            if bucket_start >= window_start:
                if oldest_bucket_key is None or bucket_key < oldest_bucket_key:
                    oldest_bucket_key = bucket_key
                    oldest_count = count
                total += count
            else:
                expired_keys.append(bucket_key_str)

        for k in expired_keys:
            del buckets[k]

        if oldest_bucket_key is not None and oldest_bucket_key != current_bucket:
            overlap = (oldest_bucket_key + 1) * bucket_duration - window_start
            weight = overlap / bucket_duration
            total = total - oldest_count + oldest_count * weight

        if total + amount <= limit:
            current_bucket_str = str(current_bucket)
            buckets[current_bucket_str] = buckets.get(current_bucket_str, 0) + amount
            ttl = window_size * 2
            self._data[hash_key] = (buckets, time.time() + ttl)

            new_remaining = limit - (total + amount)
            return StorageResult(success=True, value=[1, int(new_remaining), 0.0])
        else:
            time_to_next_bucket = (current_bucket + 1) * bucket_duration - now
            retry_after = time_to_next_bucket
            if retry_after <= 0:
                retry_after = 0.001
            return StorageResult(success=True, value=[0, 0, retry_after])

    def _eval_prefetch(self, keys: list, args: list) -> StorageResult:
        import json

        key = keys[0]
        leases_key = keys[1]
        limit = int(args[0])
        request_amount = int(args[1])
        instance_id = args[2]
        lease_ttl = float(args[3])
        window_ttl = float(args[4])
        now = float(args[5])
        old_used = int(args[6]) if len(args) > 6 else 0

        current, _ = self._data.get(key, (0, None))
        if not isinstance(current, int):
            current = 0

        leases, _ = self._data.get(leases_key, ({}, None))
        if not isinstance(leases, dict):
            leases = {}

        old_lease_json = leases.get(instance_id)
        if old_lease_json is not None:
            try:
                old_lease = json.loads(old_lease_json)
                if old_lease['expires_at'] > now:
                    current = current + old_used
                    if current > limit:
                        current = limit
            except (json.JSONDecodeError, KeyError):
                pass

        allocated = 0
        for inst_id, lease_json in leases.items():
            try:
                lease = json.loads(lease_json)
                if inst_id != instance_id and lease['expires_at'] > now:
                    allocated += (lease['quota'] - lease['used'])
            except (json.JSONDecodeError, KeyError):
                pass

        remaining = limit - current - allocated
        granted = min(request_amount, max(0, remaining))

        if granted > 0:
            lease = {
                'instance_id': instance_id,
                'quota': granted,
                'used': 0,
                'expires_at': now + lease_ttl,
                'acquired_at': now
            }
            leases[instance_id] = json.dumps(lease)
            self._data[leases_key] = (leases, time.time() + window_ttl)
            self._data[key] = (current, time.time() + window_ttl)
            return StorageResult(success=True, value=[granted, limit - current - allocated - granted])

        self._data[key] = (current, time.time() + window_ttl)
        return StorageResult(success=True, value=[0, remaining])

    def _eval_return_lease(self, keys: list, args: list) -> StorageResult:
        import json

        leases_key = keys[0]
        instance_id = args[0]
        used = int(args[1])
        now = float(args[2])

        leases, _ = self._data.get(leases_key, ({}, None))
        if not isinstance(leases, dict):
            leases = {}

        lease_json = leases.get(instance_id)
        if lease_json is None:
            return StorageResult(success=True, value=[0, 0])

        try:
            lease = json.loads(lease_json)
            returned = lease['quota'] - used
            lease['used'] = used
            lease['expires_at'] = now
            leases[instance_id] = json.dumps(lease)
            self._data[leases_key] = (leases, time.time() + 60)
            return StorageResult(success=True, value=[returned, used])
        except (json.JSONDecodeError, KeyError):
            return StorageResult(success=True, value=[0, 0])

    def _eval_window_sync(self, keys: list, args: list) -> StorageResult:
        import json

        key = keys[0]
        leases_key = keys[1]
        limit = int(args[0])
        window_ttl = float(args[1])
        local_count = int(args[2])
        now = float(args[3])

        current, _ = self._data.get(key, (0, None))
        if not isinstance(current, int):
            current = 0

        new_global = current + local_count
        if new_global > limit:
            new_global = limit

        self._data[key] = (new_global, time.time() + window_ttl)

        leases, _ = self._data.get(leases_key, ({}, None))
        if not isinstance(leases, dict):
            leases = {}

        active_allocated = 0
        for inst_id, lease_json in leases.items():
            try:
                lease = json.loads(lease_json)
                if lease['expires_at'] > now:
                    active_allocated += (lease['quota'] - lease['used'])
            except (json.JSONDecodeError, KeyError):
                pass

        return StorageResult(success=True, value=[new_global, limit - new_global - active_allocated])

    def is_available(self) -> bool:
        return self._available

    def set_available(self, available: bool) -> None:
        self._available = available

    async def aget(self, key: str) -> StorageResult:
        await asyncio.sleep(0)
        return self.get(key)

    async def aset(self, key: str, value: Any, ttl: Optional[float] = None) -> StorageResult:
        await asyncio.sleep(0)
        return self.set(key, value, ttl)

    async def aincrby(self, key: str, amount: int, ttl: Optional[float] = None) -> StorageResult:
        await asyncio.sleep(0)
        return self.incrby(key, amount, ttl)

    async def aeval(self, script: str, keys: list, args: list) -> StorageResult:
        await asyncio.sleep(0)
        return self.eval(script, keys, args)


class RedisStorage(BaseStorage):
    LIMIT_AND_INCR_SCRIPT = """
    local key = KEYS[1]
    local limit = tonumber(ARGV[1])
    local ttl = tonumber(ARGV[2])
    local amount = tonumber(ARGV[3])
    
    local current = redis.call('GET', key)
    if current == false then
        current = 0
    else
        current = tonumber(current)
    end
    
    if current + amount <= limit then
        redis.call('INCRBY', key, amount)
        redis.call('EXPIRE', key, ttl)
        return {1, limit - current - amount}
    else
        return {0, limit - current}
    end
    """

    def __init__(self, host: str = "localhost", port: int = 6379, db: int = 0,
                 password: Optional[str] = None, socket_timeout: float = 0.5,
                 retry_on_timeout: bool = True, max_retries: int = 2):
        if not HAS_REDIS:
            raise ImportError("redis package is required for RedisStorage")

        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.socket_timeout = socket_timeout
        self.retry_on_timeout = retry_on_timeout
        self.max_retries = max_retries
        self._available = True

        self._sync_client = redis.Redis(
            host=host, port=port, db=db, password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_timeout,
            retry_on_timeout=retry_on_timeout,
            health_check_interval=30
        )

        self._async_client = aioredis.Redis(
            host=host, port=port, db=db, password=password,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_timeout,
            retry_on_timeout=retry_on_timeout,
            health_check_interval=30
        )

    def _handle_error(self, e: Exception) -> StorageResult:
        if isinstance(e, (redis.exceptions.ConnectionError,
                          redis.exceptions.TimeoutError,
                          redis.exceptions.BusyLoadingError)):
            self._available = False
            raise StorageUnavailableError(f"Redis connection error: {str(e)}")
        return StorageResult(success=False, error=str(e))

    def get(self, key: str) -> StorageResult:
        for attempt in range(self.max_retries + 1):
            try:
                value = self._sync_client.get(key)
                self._available = True
                return StorageResult(success=True, value=value)
            except Exception as e:
                if attempt == self.max_retries:
                    return self._handle_error(e)
                time.sleep(0.05 * (attempt + 1))
        return StorageResult(success=False, error="Max retries exceeded")

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> StorageResult:
        for attempt in range(self.max_retries + 1):
            try:
                if ttl is not None:
                    self._sync_client.setex(key, int(ttl), value)
                else:
                    self._sync_client.set(key, value)
                self._available = True
                return StorageResult(success=True)
            except Exception as e:
                if attempt == self.max_retries:
                    return self._handle_error(e)
                time.sleep(0.05 * (attempt + 1))
        return StorageResult(success=False, error="Max retries exceeded")

    def incrby(self, key: str, amount: int, ttl: Optional[float] = None) -> StorageResult:
        for attempt in range(self.max_retries + 1):
            try:
                pipe = self._sync_client.pipeline()
                pipe.incrby(key, amount)
                if ttl is not None:
                    pipe.expire(key, int(ttl))
                result = pipe.execute()
                self._available = True
                return StorageResult(success=True, value=result[0])
            except Exception as e:
                if attempt == self.max_retries:
                    return self._handle_error(e)
                time.sleep(0.05 * (attempt + 1))
        return StorageResult(success=False, error="Max retries exceeded")

    def eval(self, script: str, keys: list, args: list) -> StorageResult:
        for attempt in range(self.max_retries + 1):
            try:
                result = self._sync_client.eval(script, len(keys), *keys, *args)
                self._available = True
                return StorageResult(success=True, value=result)
            except Exception as e:
                if attempt == self.max_retries:
                    return self._handle_error(e)
                time.sleep(0.05 * (attempt + 1))
        return StorageResult(success=False, error="Max retries exceeded")

    def is_available(self) -> bool:
        return self._available

    async def aget(self, key: str) -> StorageResult:
        for attempt in range(self.max_retries + 1):
            try:
                value = await self._async_client.get(key)
                self._available = True
                return StorageResult(success=True, value=value)
            except Exception as e:
                if attempt == self.max_retries:
                    return self._handle_error(e)
                await asyncio.sleep(0.05 * (attempt + 1))
        return StorageResult(success=False, error="Max retries exceeded")

    async def aset(self, key: str, value: Any, ttl: Optional[float] = None) -> StorageResult:
        for attempt in range(self.max_retries + 1):
            try:
                if ttl is not None:
                    await self._async_client.setex(key, int(ttl), value)
                else:
                    await self._async_client.set(key, value)
                self._available = True
                return StorageResult(success=True)
            except Exception as e:
                if attempt == self.max_retries:
                    return self._handle_error(e)
                await asyncio.sleep(0.05 * (attempt + 1))
        return StorageResult(success=False, error="Max retries exceeded")

    async def aincrby(self, key: str, amount: int, ttl: Optional[float] = None) -> StorageResult:
        for attempt in range(self.max_retries + 1):
            try:
                pipe = self._async_client.pipeline()
                pipe.incrby(key, amount)
                if ttl is not None:
                    pipe.expire(key, int(ttl))
                result = await pipe.execute()
                self._available = True
                return StorageResult(success=True, value=result[0])
            except Exception as e:
                if attempt == self.max_retries:
                    return self._handle_error(e)
                await asyncio.sleep(0.05 * (attempt + 1))
        return StorageResult(success=False, error="Max retries exceeded")

    async def aeval(self, script: str, keys: list, args: list) -> StorageResult:
        for attempt in range(self.max_retries + 1):
            try:
                result = await self._async_client.eval(script, len(keys), *keys, *args)
                self._available = True
                return StorageResult(success=True, value=result)
            except Exception as e:
                if attempt == self.max_retries:
                    return self._handle_error(e)
                await asyncio.sleep(0.05 * (attempt + 1))
        return StorageResult(success=False, error="Max retries exceeded")

    def close(self) -> None:
        try:
            self._sync_client.close()
        except Exception:
            pass

    async def aclose(self) -> None:
        try:
            await self._async_client.close()
        except Exception:
            pass
