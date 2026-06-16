"""
分布式限流服务使用示例

本示例展示了如何在实际项目中使用分布式限流服务的各种场景。
"""

import asyncio
import time
from rate_limiter import (
    create_rate_limiter,
    create_async_rate_limiter,
    RateLimiterClient,
    AsyncRateLimiterClient,
    CoordinationMode,
    DegradationMode,
    QuotaExceededError
)
from rate_limiter.storage import InMemoryStorage


def example_basic_usage():
    """基础使用：每秒最多 100 次"""
    print("=" * 60)
    print("示例 1: 基础使用")
    print("=" * 60)

    limiter = create_rate_limiter(limit=10, per_second=True)

    print("尝试发送 15 次请求（限制 10 次/秒）:")
    for i in range(15):
        result = limiter.try_acquire(1)
        status = "✅" if result.allowed else "❌"
        print(f"  请求 {i+1}: {status} 剩余={result.remaining}")

    limiter.close()
    print()


def example_context_manager():
    """使用上下文管理器"""
    print("=" * 60)
    print("示例 2: 上下文管理器")
    print("=" * 60)

    limiter = create_rate_limiter(limit=5, per_second=True)

    def process_api_request(request_id):
        try:
            with limiter.limit():
                print(f"  请求 {request_id}: 处理中...")
                time.sleep(0.01)
                return f"Response {request_id}"
        except QuotaExceededError as e:
            print(f"  请求 {request_id}: 被限流，重试时间 {e.retry_after:.2f}s")
            return None

    print("发送 8 次请求:")
    for i in range(8):
        process_api_request(i)

    limiter.close()
    print()


def example_decorator():
    """使用装饰器"""
    print("=" * 60)
    print("示例 3: 装饰器模式")
    print("=" * 60)

    limiter = create_rate_limiter(limit=3, per_second=True)

    @limiter.decorate()
    def get_user_info(user_id):
        return {"id": user_id, "name": f"User {user_id}"}

    print("调用装饰器函数:")
    for i in range(5):
        try:
            result = get_user_info(i)
            print(f"  成功: {result}")
        except QuotaExceededError as e:
            print(f"  限流: 请 {e.retry_after:.2f}s 后重试")

    limiter.close()
    print()


def example_blocking_wait():
    """阻塞等待令牌"""
    print("=" * 60)
    print("示例 4: 阻塞等待模式")
    print("=" * 60)

    limiter = create_rate_limiter(limit=3, per_second=True)

    print("快速发送 3 次请求，然后等待第 4 次:")
    for i in range(3):
        limiter.try_acquire(1)
        print(f"  请求 {i+1}: 成功")

    print("  等待第 4 次请求（阻塞等待）...")
    start = time.time()
    success = limiter.wait_for_token(1, max_wait=1.0)
    elapsed = time.time() - start
    print(f"  第 4 次: {'成功' if success else '超时'}，耗时 {elapsed:.2f}s")

    limiter.close()
    print()


def example_degradation():
    """故障降级演示"""
    print("=" * 60)
    print("示例 5: 故障降级策略")
    print("=" * 60)

    storage = InMemoryStorage()
    limiter = RateLimiterClient(
        global_limit=10,
        storage=storage,
        mode=CoordinationMode.PER_REQUEST,
        degradation_mode=DegradationMode.LOCAL_LIMIT,
        health_check_interval=0.1
    )

    print("正常模式下发送请求:")
    for i in range(5):
        result = limiter.try_acquire(1)
        print(f"  请求 {i+1}: {'✅' if result.allowed else '❌'} 降级={limiter.is_degraded()}")

    print("\n模拟 Redis 故障...")
    storage.set_available(False)
    time.sleep(0.15)

    print(f"\n降级模式下发送请求 (本地限制放宽到 15:")
    for i in range(18):
        result = limiter.try_acquire(1)
        status = "✅" if result.allowed else "❌"
        degraded = limiter.is_degraded()
        print(f"  请求 {i+1}: {status} 降级={degraded}")

    print("\n恢复 Redis...")
    storage.set_available(True)
    time.sleep(0.15)

    print(f"\n恢复后状态: 降级={limiter.is_degraded()}")

    limiter.close()
    print()


def example_prefetch_vs_per_request():
    """预取模式 vs 每次请求模式的性能对比"""
    print("=" * 60)
    print("示例 6: 预取模式 vs 每次请求模式性能对比")
    print("=" * 60)

    storage = InMemoryStorage()
    limit = 1000

    prefetch_client = RateLimiterClient(
        global_limit=limit,
        storage=storage,
        mode=CoordinationMode.PRE_FETCH,
        prefetch_ratio=0.2,
        min_prefetch=100
    )

    per_request_client = RateLimiterClient(
        global_limit=limit,
        storage=storage,
        mode=CoordinationMode.PER_REQUEST,
        instance_id="per_request_demo"
    )

    print(f"每秒限制: {limit} 次，各发送 500 次请求:")

    start = time.time()
    for i in range(500):
        prefetch_client.try_acquire(1)
    prefetch_time = time.time() - start
    print(f"  预取模式: {prefetch_time:.4f}s")

    start = time.time()
    for i in range(500):
        per_request_client.try_acquire(1)
    per_request_time = time.time() - start
    print(f"  每次请求模式: {per_request_time:.4f}s")

    speedup = per_request_time / prefetch_time
    print(f"  性能提升: {speedup:.1f}x 倍")

    prefetch_client.close()
    per_request_client.close()
    print()


def example_monitoring():
    """监控指标获取"""
    print("=" * 60)
    print("示例 7: 获取监控指标")
    print("=" * 60)

    limiter = create_rate_limiter(limit=100, per_second=True)

    for _ in range(42):
        limiter.try_acquire(1)

    stats = limiter.get_stats()
    print("监控指标:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    limiter.close()
    print()


def example_multiple_instances():
    """多实例共享配额"""
    print("=" * 60)
    print("示例 8: 多实例共享全局配额")
    print("=" * 60)

    storage = InMemoryStorage()
    global_limit = 20

    print(f"全局限制: {global_limit} 次/秒，3 个实例共享")

    instances = []
    for i in range(3):
        instance = RateLimiterClient(
            global_limit=global_limit,
            storage=storage,
            mode=CoordinationMode.PER_REQUEST,
            instance_id=f"instance_{i}"
        )
        instances.append(instance)

    total_allowed = 0
    total_denied = 0
    print("\n各实例轮流发送请求:")
    for round in range(10):
        for i, inst in enumerate(instances):
            result = inst.try_acquire(1)
            if result.allowed:
                total_allowed += 1
                print(f"  第 {round+1} 轮 - 实例 {i}: ✅")
            else:
                total_denied += 1
                print(f"  第 {round+1} 轮 - 实例 {i}: ❌")

    print(f"\n总计: 允许 {total_allowed} 次，拒绝 {total_denied} 次")
    print(f"全局限制: {global_limit}，实际通过: {total_allowed}")

    for inst in instances:
        inst.close()
    print()


async def example_async_usage():
    """异步使用示例"""
    print("=" * 60)
    print("示例 9: 异步 API 使用")
    print("=" * 60)

    limiter = create_async_rate_limiter(limit=10, per_second=True)

    print("异步发送 15 次请求:")
    for i in range(15):
        result = await limiter.try_acquire(1)
        status = "✅" if result.allowed else "❌"
        print(f"  请求 {i+1}: {status} 剩余={result.remaining}")

    try:
        async with limiter.limit():
            print("\n异步上下文管理器: 处理请求...")
    except QuotaExceededError as e:
        print(f"\n异步上下文管理器: 被限流，{e.retry_after:.2f}s 后重试")
        await asyncio.sleep(e.retry_after)
        async with limiter.limit():
            print("异步上下文管理器: 重试后处理成功...")

    await limiter.close()
    print()


async def example_async_decorator():
    """异步装饰器示例"""
    print("=" * 60)
    print("示例 10: 异步装饰器")
    print("=" * 60)

    limiter = create_async_rate_limiter(limit=5, per_second=True)

    @limiter.decorate()
    async def async_api_call(request_id):
        await asyncio.sleep(0.01)
        return f"Async Response {request_id}"

    print("调用异步装饰器:")
    for i in range(8):
        try:
            result = await async_api_call(i)
            print(f"  {result}")
        except QuotaExceededError as e:
            print(f"  请求 {i}: 限流，{e.retry_after:.2f}s")

    await limiter.close()
    print()


def main():
    print("\n" + "=" * 60)
    print("分布式限流服务 - 完整示例")
    print("=" * 60 + "\n")

    example_basic_usage()
    example_context_manager()
    example_decorator()
    example_blocking_wait()
    example_degradation()
    example_prefetch_vs_per_request()
    example_monitoring()
    example_multiple_instances()

    print("\n" + "=" * 60)
    print("异步示例")
    print("=" * 60 + "\n")

    asyncio.run(example_async_usage())
    asyncio.run(example_async_decorator())

    print("\n" + "=" * 60)
    print("所有示例执行完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()
