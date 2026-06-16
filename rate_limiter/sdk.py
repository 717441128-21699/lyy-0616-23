import time
import asyncio
import functools
from typing import Optional, Dict, Any, Callable, TypeVar, Union
from contextlib import contextmanager, asynccontextmanager

from .coordinator import (
    DistributedCoordinator,
    CoordinationMode,
    DegradationMode
)
from .storage import BaseStorage, RedisStorage, InMemoryStorage
from .core import RateLimitResult
from .exceptions import QuotaExceededError, RateLimiterError

T = TypeVar('T')


class RateLimiterClient:
    def __init__(
        self,
        global_limit: int,
        window_size: float = 1.0,
        storage: Optional[BaseStorage] = None,
        redis_url: Optional[str] = None,
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
        if storage is None:
            if redis_url:
                from urllib.parse import urlparse
                parsed = urlparse(redis_url)
                storage = RedisStorage(
                    host=parsed.hostname or "localhost",
                    port=parsed.port or 6379,
                    db=int(parsed.path.lstrip('/') or 0),
                    password=parsed.password
                )
            else:
                storage = InMemoryStorage()

        self.storage = storage
        self.coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=global_limit,
            window_size=window_size,
            mode=mode,
            prefetch_ratio=prefetch_ratio,
            min_prefetch=min_prefetch,
            max_prefetch=max_prefetch,
            sync_interval=sync_interval,
            lease_ttl=lease_ttl,
            degradation_mode=degradation_mode,
            local_limit_ratio=local_limit_ratio,
            health_check_interval=health_check_interval,
            instance_id=instance_id
        )

    def acquire(self, tokens: int = 1, block: bool = False, timeout: Optional[float] = None) -> RateLimitResult:
        return self.coordinator.acquire(tokens=tokens, block=block, timeout=timeout)

    def try_acquire(self, tokens: int = 1) -> RateLimitResult:
        return self.coordinator.try_acquire(tokens=tokens)

    @contextmanager
    def limit(self, tokens: int = 1, raise_on_exceed: bool = True):
        result = self.try_acquire(tokens)
        if not result.allowed and raise_on_exceed:
            raise QuotaExceededError(
                limit=result.limit,
                remaining=result.remaining,
                retry_after=result.retry_after
            )
        try:
            yield result
        finally:
            pass

    def decorate(self, tokens: int = 1, raise_on_exceed: bool = True) -> Callable[[Callable[..., T]], Callable[..., T]]:
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                with self.limit(tokens=tokens, raise_on_exceed=raise_on_exceed):
                    return func(*args, **kwargs)
            return wrapper
        return decorator

    def wait_for_token(self, tokens: int = 1, max_wait: Optional[float] = None) -> bool:
        try:
            self.acquire(tokens=tokens, block=True, timeout=max_wait)
            return True
        except QuotaExceededError:
            return False

    def get_stats(self) -> Dict[str, Any]:
        return self.coordinator.get_stats()

    def is_degraded(self) -> bool:
        return self.coordinator._degraded

    def close(self) -> None:
        self.coordinator.close()
        if hasattr(self.storage, 'close'):
            try:
                self.storage.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


class AsyncRateLimiterClient:
    def __init__(
        self,
        global_limit: int,
        window_size: float = 1.0,
        storage: Optional[BaseStorage] = None,
        redis_url: Optional[str] = None,
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
        if storage is None:
            if redis_url:
                from urllib.parse import urlparse
                parsed = urlparse(redis_url)
                storage = RedisStorage(
                    host=parsed.hostname or "localhost",
                    port=parsed.port or 6379,
                    db=int(parsed.path.lstrip('/') or 0),
                    password=parsed.password
                )
            else:
                storage = InMemoryStorage()

        self.storage = storage
        self.coordinator = DistributedCoordinator(
            storage=storage,
            global_limit=global_limit,
            window_size=window_size,
            mode=mode,
            prefetch_ratio=prefetch_ratio,
            min_prefetch=min_prefetch,
            max_prefetch=max_prefetch,
            sync_interval=sync_interval,
            lease_ttl=lease_ttl,
            degradation_mode=degradation_mode,
            local_limit_ratio=local_limit_ratio,
            health_check_interval=health_check_interval,
            instance_id=instance_id
        )

    async def acquire(self, tokens: int = 1, block: bool = False, timeout: Optional[float] = None) -> RateLimitResult:
        return await self.coordinator.aacquire(tokens=tokens, block=block, timeout=timeout)

    async def try_acquire(self, tokens: int = 1) -> RateLimitResult:
        return await self.coordinator.atry_acquire(tokens=tokens)

    @asynccontextmanager
    async def limit(self, tokens: int = 1, raise_on_exceed: bool = True):
        result = await self.try_acquire(tokens)
        if not result.allowed and raise_on_exceed:
            raise QuotaExceededError(
                limit=result.limit,
                remaining=result.remaining,
                retry_after=result.retry_after
            )
        try:
            yield result
        finally:
            pass

    def decorate(self, tokens: int = 1, raise_on_exceed: bool = True) -> Callable[[Callable[..., T]], Callable[..., T]]:
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @functools.wraps(func)
            async def wrapper(*args: Any, **kwargs: Any) -> T:
                async with self.limit(tokens=tokens, raise_on_exceed=raise_on_exceed):
                    return await func(*args, **kwargs)
            return wrapper
        return decorator

    async def wait_for_token(self, tokens: int = 1, max_wait: Optional[float] = None) -> bool:
        try:
            await self.acquire(tokens=tokens, block=True, timeout=max_wait)
            return True
        except QuotaExceededError:
            return False

    def get_stats(self) -> Dict[str, Any]:
        return self.coordinator.get_stats()

    def is_degraded(self) -> bool:
        return self.coordinator._degraded

    async def close(self) -> None:
        await self.coordinator.aclose()
        if hasattr(self.storage, 'aclose'):
            try:
                await self.storage.aclose()
            except Exception:
                pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    def __del__(self):
        try:
            self.coordinator.close()
        except Exception:
            pass


def create_rate_limiter(
    limit: int,
    per_second: bool = True,
    redis_url: Optional[str] = None,
    **kwargs
) -> RateLimiterClient:
    window_size = 1.0 if per_second else kwargs.pop('window_size', 1.0)
    return RateLimiterClient(
        global_limit=limit,
        window_size=window_size,
        redis_url=redis_url,
        **kwargs
    )


def create_async_rate_limiter(
    limit: int,
    per_second: bool = True,
    redis_url: Optional[str] = None,
    **kwargs
) -> AsyncRateLimiterClient:
    window_size = 1.0 if per_second else kwargs.pop('window_size', 1.0)
    return AsyncRateLimiterClient(
        global_limit=limit,
        window_size=window_size,
        redis_url=redis_url,
        **kwargs
    )
