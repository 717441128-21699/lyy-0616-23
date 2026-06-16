import time
import asyncio
import threading
import uuid
from typing import Optional, Dict, Tuple
from dataclasses import dataclass, field
from enum import Enum

from .core import RateLimitResult, TokenBucket, SlidingWindow
from .storage import BaseStorage, RedisStorage
from .exceptions import StorageUnavailableError, QuotaExceededError


class CoordinationMode(Enum):
    PRE_FETCH = "pre_fetch"
    PER_REQUEST = "per_request"


class DegradationMode(Enum):
    LOCAL_LIMIT = "local_limit"
    FAIL_OPEN = "fail_open"
    FAIL_CLOSED = "fail_closed"


@dataclass
class PrefetchLease:
    instance_id: str
    quota: int
    used: int
    expires_at: float
    acquired_at: float


@dataclass
class WindowState:
    window_start: float
    global_count: int
    local_count: int
    last_sync_time: float


class DistributedCoordinator:
    PREFETCH_SCRIPT = """
    local key = KEYS[1]
    local leases_key = KEYS[2]
    local limit = tonumber(ARGV[1])
    local request_amount = tonumber(ARGV[2])
    local instance_id = ARGV[3]
    local lease_ttl = tonumber(ARGV[4])
    local window_ttl = tonumber(ARGV[5])
    local now = tonumber(ARGV[6])

    local current = redis.call('GET', key)
    if current == false then
        current = 0
    else
        current = tonumber(current)
    end

    local allocated = 0
    local leases = redis.call('HGETALL', leases_key)
    for i = 1, #leases, 2 do
        local lease_data = cjson.decode(leases[i + 1])
        if lease_data.expires_at > now then
            allocated = allocated + (lease_data.quota - lease_data.used)
        end
    end

    local remaining = limit - current - allocated
    local granted = math.min(request_amount, math.max(0, remaining))

    if granted > 0 then
        local lease = {
            instance_id = instance_id,
            quota = granted,
            used = 0,
            expires_at = now + lease_ttl,
            acquired_at = now
        }
        redis.call('HSET', leases_key, instance_id, cjson.encode(lease))
        redis.call('EXPIRE', leases_key, window_ttl)
        return {granted, limit - current - allocated - granted}
    end

    return {0, remaining}
    """

    RETURN_LEASE_SCRIPT = """
    local leases_key = KEYS[1]
    local instance_id = ARGV[1]
    local used = tonumber(ARGV[2])
    local now = tonumber(ARGV[3])

    local lease_data = redis.call('HGET', leases_key, instance_id)
    if lease_data == false then
        return {0, 0}
    end

    local lease = cjson.decode(lease_data)
    local returned = lease.quota - used
    lease.used = used
    lease.expires_at = now

    redis.call('HSET', leases_key, instance_id, cjson.encode(lease))
    return {returned, used}
    """

    WINDOW_SYNC_SCRIPT = """
    local key = KEYS[1]
    local leases_key = KEYS[2]
    local limit = tonumber(ARGV[1])
    local window_ttl = tonumber(ARGV[2])
    local local_count = tonumber(ARGV[3])
    local now = tonumber(ARGV[4])

    local current = redis.call('GET', key)
    if current == false then
        current = 0
    else
        current = tonumber(current)
    end

    local new_global = current + local_count
    if new_global > limit then
        new_global = limit
    end

    redis.call('SET', key, new_global)
    redis.call('EXPIRE', key, window_ttl)

    local leases = redis.call('HGETALL', leases_key)
    local active_allocated = 0
    for i = 1, #leases, 2 do
        local lease_data = cjson.decode(leases[i + 1])
        if lease_data.expires_at > now then
            active_allocated = active_allocated + (lease_data.quota - lease_data.used)
        end
    end

    return {new_global, limit - new_global - active_allocated}
    """

    def __init__(
        self,
        storage: BaseStorage,
        global_limit: int,
        window_size: float = 1.0,
        mode: CoordinationMode = CoordinationMode.PRE_FETCH,
        prefetch_ratio: float = 0.1,
        min_prefetch: int = 5,
        max_prefetch: int = 100,
        sync_interval: float = 0.1,
        lease_ttl: float = 2.0,
        degradation_mode: DegradationMode = DegradationMode.LOCAL_LIMIT,
        local_limit_ratio: float = 1.5,
        health_check_interval: float = 5.0,
        instance_id: Optional[str] = None
    ):
        self.storage = storage
        self.global_limit = global_limit
        self.window_size = window_size
        self.mode = mode
        self.prefetch_ratio = prefetch_ratio
        self.min_prefetch = min_prefetch
        self.max_prefetch = max_prefetch
        self.sync_interval = sync_interval
        self.lease_ttl = lease_ttl
        self.degradation_mode = degradation_mode
        self.local_limit_ratio = local_limit_ratio
        self.health_check_interval = health_check_interval
        self.instance_id = instance_id or str(uuid.uuid4())

        self._local_bucket = TokenBucket(
            rate=global_limit,
            capacity=global_limit,
            initial_tokens=0
        )
        self._window_counter = SlidingWindow(
            window_size=window_size,
            limit=global_limit,
            bucket_count=10
        )

        self._current_lease: Optional[PrefetchLease] = None
        self._window_state = WindowState(
            window_start=self._get_window_start(time.time()),
            global_count=0,
            local_count=0,
            last_sync_time=0
        )

        self._degraded = False
        self._last_degradation_check = 0.0
        self._local_limit = int(global_limit * local_limit_ratio)
        self._local_degraded_bucket = TokenBucket(
            rate=self._local_limit,
            capacity=self._local_limit
        )

        self._lock = threading.Lock()
        self._async_lock: Optional[asyncio.Lock] = None
        self._sync_thread: Optional[threading.Thread] = None
        self._stop_sync = threading.Event()

        self._start_background_sync()

    def _get_async_lock(self) -> asyncio.Lock:
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        return self._async_lock

    def _get_window_start(self, timestamp: float) -> float:
        return int(timestamp / self.window_size) * self.window_size

    def _get_key_prefix(self) -> str:
        return f"rate_limit:{self.global_limit}:{self.window_size}"

    def _get_counter_key(self, window_start: float) -> str:
        return f"{self._get_key_prefix()}:counter:{int(window_start)}"

    def _get_leases_key(self, window_start: float) -> str:
        return f"{self._get_key_prefix()}:leases:{int(window_start)}"

    def _calculate_prefetch_amount(self) -> int:
        amount = int(self.global_limit * self.prefetch_ratio)
        return max(self.min_prefetch, min(self.max_prefetch, amount))

    def _check_window_rollover(self, now: float) -> None:
        current_window_start = self._get_window_start(now)
        if current_window_start != self._window_state.window_start:
            if self._current_lease:
                try:
                    self._return_lease(self._window_state.window_start)
                except Exception:
                    pass
            self._window_state = WindowState(
                window_start=current_window_start,
                global_count=0,
                local_count=0,
                last_sync_time=0
            )
            self._current_lease = None
            self._local_bucket = TokenBucket(
                rate=self.global_limit,
                capacity=self.global_limit,
                initial_tokens=0
            )

    def _check_degradation(self) -> None:
        now = time.time()
        if now - self._last_degradation_check < self.health_check_interval:
            return

        self._last_degradation_check = now
        storage_available = self.storage.is_available()

        if not storage_available and not self._degraded:
            self._degraded = True
            self._local_degraded_bucket = TokenBucket(
                rate=self._local_limit,
                capacity=self._local_limit
            )
        elif storage_available and self._degraded:
            try:
                test_key = f"{self._get_key_prefix()}:health_check"
                self.storage.get(test_key)
                self._degraded = False
            except Exception:
                pass

    def _prefetch_quota(self, now: float) -> Tuple[int, int]:
        window_start = self._get_window_start(now)
        key = self._get_counter_key(window_start)
        leases_key = self._get_leases_key(window_start)
        request_amount = self._calculate_prefetch_amount()

        result = self.storage.eval(
            self.PREFETCH_SCRIPT,
            [key, leases_key],
            [
                str(self.global_limit),
                str(request_amount),
                self.instance_id,
                str(int(self.lease_ttl)),
                str(int(self.window_size * 2)),
                str(int(now))
            ]
        )

        if not result.success or result.value is None:
            raise StorageUnavailableError("Failed to prefetch quota")

        granted, remaining = result.value
        granted = int(granted)
        remaining = int(remaining)

        if granted > 0:
            self._current_lease = PrefetchLease(
                instance_id=self.instance_id,
                quota=granted,
                used=0,
                expires_at=now + self.lease_ttl,
                acquired_at=now
            )
            self._local_bucket.add_tokens(granted)

        return granted, remaining

    async def _aprefetch_quota(self, now: float) -> Tuple[int, int]:
        window_start = self._get_window_start(now)
        key = self._get_counter_key(window_start)
        leases_key = self._get_leases_key(window_start)
        request_amount = self._calculate_prefetch_amount()

        result = await self.storage.aeval(
            self.PREFETCH_SCRIPT,
            [key, leases_key],
            [
                str(self.global_limit),
                str(request_amount),
                self.instance_id,
                str(int(self.lease_ttl)),
                str(int(self.window_size * 2)),
                str(int(now))
            ]
        )

        if not result.success or result.value is None:
            raise StorageUnavailableError("Failed to prefetch quota")

        granted, remaining = result.value
        granted = int(granted)
        remaining = int(remaining)

        if granted > 0:
            self._current_lease = PrefetchLease(
                instance_id=self.instance_id,
                quota=granted,
                used=0,
                expires_at=now + self.lease_ttl,
                acquired_at=now
            )
            self._local_bucket.add_tokens(granted)

        return granted, remaining

    def _return_lease(self, window_start: float) -> None:
        if not self._current_lease:
            return

        leases_key = self._get_leases_key(window_start)
        try:
            self.storage.eval(
                self.RETURN_LEASE_SCRIPT,
                [leases_key],
                [
                    self.instance_id,
                    str(self._current_lease.used),
                    str(int(time.time()))
                ]
            )
        except Exception:
            pass
        self._current_lease = None

    async def _areturn_lease(self, window_start: float) -> None:
        if not self._current_lease:
            return

        leases_key = self._get_leases_key(window_start)
        try:
            await self.storage.aeval(
                self.RETURN_LEASE_SCRIPT,
                [leases_key],
                [
                    self.instance_id,
                    str(self._current_lease.used),
                    str(int(time.time()))
                ]
            )
        except Exception:
            pass
        self._current_lease = None

    def _sync_local_count(self, now: float) -> None:
        if now - self._window_state.last_sync_time < self.sync_interval:
            return
        if self._window_state.local_count == 0:
            return

        window_start = self._get_window_start(now)
        key = self._get_counter_key(window_start)
        leases_key = self._get_leases_key(window_start)

        try:
            result = self.storage.eval(
                self.WINDOW_SYNC_SCRIPT,
                [key, leases_key],
                [
                    str(self.global_limit),
                    str(int(self.window_size * 2)),
                    str(self._window_state.local_count),
                    str(int(now))
                ]
            )

            if result.success and result.value:
                global_count, available = result.value
                self._window_state.global_count = int(global_count)
                self._window_state.local_count = 0
                self._window_state.last_sync_time = now
        except Exception:
            pass

    async def _async_sync_local_count(self, now: float) -> None:
        if now - self._window_state.last_sync_time < self.sync_interval:
            return
        if self._window_state.local_count == 0:
            return

        window_start = self._get_window_start(now)
        key = self._get_counter_key(window_start)
        leases_key = self._get_leases_key(window_start)

        try:
            result = await self.storage.aeval(
                self.WINDOW_SYNC_SCRIPT,
                [key, leases_key],
                [
                    str(self.global_limit),
                    str(int(self.window_size * 2)),
                    str(self._window_state.local_count),
                    str(int(now))
                ]
            )

            if result.success and result.value:
                global_count, available = result.value
                self._window_state.global_count = int(global_count)
                self._window_state.local_count = 0
                self._window_state.last_sync_time = now
        except Exception:
            pass

    def _handle_degraded_mode(self, tokens: int) -> RateLimitResult:
        if self.degradation_mode == DegradationMode.FAIL_OPEN:
            return RateLimitResult(
                allowed=True,
                remaining=-1,
                limit=self.global_limit,
                retry_after=0.0
            )
        elif self.degradation_mode == DegradationMode.FAIL_CLOSED:
            return RateLimitResult(
                allowed=False,
                remaining=0,
                limit=self.global_limit,
                retry_after=self.health_check_interval
            )
        else:
            return self._local_degraded_bucket.try_consume(tokens)

    def _check_per_request(self, tokens: int, now: float) -> RateLimitResult:
        window_start = self._get_window_start(now)
        key = self._get_counter_key(window_start)

        result = self.storage.eval(
            RedisStorage.LIMIT_AND_INCR_SCRIPT,
            [key],
            [str(self.global_limit), str(int(self.window_size * 2)), str(tokens)]
        )

        if not result.success or result.value is None:
            raise StorageUnavailableError("Failed to check rate limit")

        allowed, remaining = result.value
        allowed = bool(allowed)
        remaining = int(remaining)

        if allowed:
            return RateLimitResult(
                allowed=True,
                remaining=remaining,
                limit=self.global_limit,
                retry_after=0.0
            )
        else:
            next_window = window_start + self.window_size
            retry_after = max(0.001, next_window - now)
            return RateLimitResult(
                allowed=False,
                remaining=max(0, remaining),
                limit=self.global_limit,
                retry_after=retry_after
            )

    async def _acheck_per_request(self, tokens: int, now: float) -> RateLimitResult:
        window_start = self._get_window_start(now)
        key = self._get_counter_key(window_start)

        result = await self.storage.aeval(
            RedisStorage.LIMIT_AND_INCR_SCRIPT,
            [key],
            [str(self.global_limit), str(int(self.window_size * 2)), str(tokens)]
        )

        if not result.success or result.value is None:
            raise StorageUnavailableError("Failed to check rate limit")

        allowed, remaining = result.value
        allowed = bool(allowed)
        remaining = int(remaining)

        if allowed:
            return RateLimitResult(
                allowed=True,
                remaining=remaining,
                limit=self.global_limit,
                retry_after=0.0
            )
        else:
            next_window = window_start + self.window_size
            retry_after = max(0.001, next_window - now)
            return RateLimitResult(
                allowed=False,
                remaining=max(0, remaining),
                limit=self.global_limit,
                retry_after=retry_after
            )

    def _check_prefetch(self, tokens: int, now: float) -> RateLimitResult:
        need_prefetch = False

        if self._current_lease is None:
            need_prefetch = True
        elif self._current_lease.expires_at < now:
            self._return_lease(self._window_state.window_start)
            need_prefetch = True
        elif self._current_lease.used + tokens > self._current_lease.quota:
            need_prefetch = True

        if need_prefetch:
            self._prefetch_quota(now)

        local_result = self._local_bucket.try_consume(tokens)

        if local_result.allowed:
            if self._current_lease:
                self._current_lease.used += tokens
            self._window_state.local_count += tokens
            self._sync_local_count(now)
            return local_result
        else:
            if self._local_bucket.peek() == 0:
                try:
                    self._prefetch_quota(now)
                    local_result = self._local_bucket.try_consume(tokens)
                    if local_result.allowed:
                        if self._current_lease:
                            self._current_lease.used += tokens
                        self._window_state.local_count += tokens
                        self._sync_local_count(now)
                        return local_result
                except StorageUnavailableError:
                    pass

            return local_result

    async def _acheck_prefetch(self, tokens: int, now: float) -> RateLimitResult:
        need_prefetch = False

        if self._current_lease is None:
            need_prefetch = True
        elif self._current_lease.expires_at < now:
            await self._areturn_lease(self._window_state.window_start)
            need_prefetch = True
        elif self._current_lease.used + tokens > self._current_lease.quota:
            need_prefetch = True

        if need_prefetch:
            await self._aprefetch_quota(now)

        local_result = self._local_bucket.try_consume(tokens)

        if local_result.allowed:
            if self._current_lease:
                self._current_lease.used += tokens
            self._window_state.local_count += tokens
            await self._async_sync_local_count(now)
            return local_result
        else:
            if self._local_bucket.peek() == 0:
                try:
                    await self._aprefetch_quota(now)
                    local_result = self._local_bucket.try_consume(tokens)
                    if local_result.allowed:
                        if self._current_lease:
                            self._current_lease.used += tokens
                        self._window_state.local_count += tokens
                        await self._async_sync_local_count(now)
                        return local_result
                except StorageUnavailableError:
                    pass

            return local_result

    def acquire(self, tokens: int = 1, block: bool = False, timeout: Optional[float] = None) -> RateLimitResult:
        with self._lock:
            now = time.time()
            self._check_window_rollover(now)
            self._check_degradation()

            if self._degraded:
                result = self._handle_degraded_mode(tokens)
            else:
                try:
                    if self.mode == CoordinationMode.PER_REQUEST:
                        result = self._check_per_request(tokens, now)
                    else:
                        result = self._check_prefetch(tokens, now)
                except StorageUnavailableError:
                    self._degraded = True
                    result = self._handle_degraded_mode(tokens)

            if not result.allowed and block:
                if timeout is None or timeout > 0:
                    wait_time = min(result.retry_after, timeout if timeout else result.retry_after)
                    if wait_time > 0:
                        time.sleep(wait_time)
                        return self.acquire(tokens, block=False)

            if not result.allowed:
                raise QuotaExceededError(
                    limit=result.limit,
                    remaining=result.remaining,
                    retry_after=result.retry_after
                )

            return result

    async def aacquire(self, tokens: int = 1, block: bool = False, timeout: Optional[float] = None) -> RateLimitResult:
        async with self._get_async_lock():
            now = time.time()
            self._check_window_rollover(now)
            self._check_degradation()

            if self._degraded:
                result = self._handle_degraded_mode(tokens)
            else:
                try:
                    if self.mode == CoordinationMode.PER_REQUEST:
                        result = await self._acheck_per_request(tokens, now)
                    else:
                        result = await self._acheck_prefetch(tokens, now)
                except StorageUnavailableError:
                    self._degraded = True
                    result = self._handle_degraded_mode(tokens)

            if not result.allowed and block:
                if timeout is None or timeout > 0:
                    wait_time = min(result.retry_after, timeout if timeout else result.retry_after)
                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                        return await self.aacquire(tokens, block=False)

            if not result.allowed:
                raise QuotaExceededError(
                    limit=result.limit,
                    remaining=result.remaining,
                    retry_after=result.retry_after
                )

            return result

    def try_acquire(self, tokens: int = 1) -> RateLimitResult:
        try:
            return self.acquire(tokens, block=False)
        except QuotaExceededError as e:
            return RateLimitResult(
                allowed=False,
                remaining=e.remaining,
                limit=e.limit,
                retry_after=e.retry_after
            )

    async def atry_acquire(self, tokens: int = 1) -> RateLimitResult:
        try:
            return await self.aacquire(tokens, block=False)
        except QuotaExceededError as e:
            return RateLimitResult(
                allowed=False,
                remaining=e.remaining,
                limit=e.limit,
                retry_after=e.retry_after
            )

    def _start_background_sync(self) -> None:
        def sync_loop():
            while not self._stop_sync.is_set():
                try:
                    with self._lock:
                        now = time.time()
                        self._check_window_rollover(now)
                        if not self._degraded:
                            self._sync_local_count(now)
                except Exception:
                    pass
                self._stop_sync.wait(0.1)

        self._sync_thread = threading.Thread(target=sync_loop, daemon=True)
        self._sync_thread.start()

    def get_stats(self) -> Dict:
        with self._lock:
            return {
                "instance_id": self.instance_id,
                "global_limit": self.global_limit,
                "window_size": self.window_size,
                "mode": self.mode.value,
                "degraded": self._degraded,
                "degradation_mode": self.degradation_mode.value,
                "current_window_start": self._window_state.window_start,
                "global_count": self._window_state.global_count,
                "local_count": self._window_state.local_count,
                "local_tokens": self._local_bucket.peek(),
                "has_lease": self._current_lease is not None,
                "lease_used": self._current_lease.used if self._current_lease else 0,
                "lease_quota": self._current_lease.quota if self._current_lease else 0
            }

    def close(self) -> None:
        self._stop_sync.set()
        if self._sync_thread:
            self._sync_thread.join(timeout=1.0)
        if self._current_lease:
            try:
                self._return_lease(self._window_state.window_start)
            except Exception:
                pass

    async def aclose(self) -> None:
        self._stop_sync.set()
        if self._current_lease:
            try:
                await self._areturn_lease(self._window_state.window_start)
            except Exception:
                pass

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass
