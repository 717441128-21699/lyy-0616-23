class RateLimiterError(Exception):
    pass


class QuotaExceededError(RateLimiterError):
    def __init__(self, limit: int, remaining: int, retry_after: float):
        self.limit = limit
        self.remaining = remaining
        self.retry_after = retry_after
        super().__init__(f"Quota exceeded. Limit: {limit}, Retry after: {retry_after:.2f}s")


class StorageUnavailableError(RateLimiterError):
    def __init__(self, message: str = "Storage is unavailable"):
        super().__init__(message)


class DegradedModeError(RateLimiterError):
    def __init__(self, message: str = "Operating in degraded mode"):
        super().__init__(message)
