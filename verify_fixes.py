"""
验证四个问题的修复效果
"""
import time
import asyncio
from rate_limiter import create_rate_limiter, create_async_rate_limiter
from rate_limiter.coordinator import DistributedCoordinator, CoordinationMode, DegradationMode
from rate_limiter.storage import InMemoryStorage


def test_issue_1_basic_limit():
    """问题1: 默认不接Redis时，limit=10连续请求15次应只通过10次"""
    print("=" * 60)
    print("测试 1: 基础限流精度 (limit=10, 15次请求)")
    print("=" * 60)
    
    limiter = create_rate_limiter(limit=10, per_second=True)
    passed = 0
    rejected = 0
    
    for i in range(15):
        result = limiter.try_acquire(1)
        if result.allowed:
            passed += 1
            print(f"  请求 {i+1}: ✅ 通过 (剩余={result.remaining})")
        else:
            rejected += 1
            print(f"  请求 {i+1}: ❌ 拒绝 (retry_after={result.retry_after:.3f}s)")
    
    print(f"\n结果: 通过={passed}, 拒绝={rejected}")
    print(f"期望: 通过=10, 拒绝=5")
    print(f"测试 {'通过' if passed == 10 else '失败'}!\n")
    return passed == 10


def test_issue_2_wait_for_token():
    """问题2: wait_for_token 应该正确等待，超时按时返回"""
    print("=" * 60)
    print("测试 2: wait_for_token 等待和超时")
    print("=" * 60)
    
    limiter = create_rate_limiter(limit=3, per_second=True)
    
    for i in range(3):
        result = limiter.try_acquire(1)
        print(f"  预热请求 {i+1}: {'✅' if result.allowed else '❌'}")
    
    start = time.time()
    result = limiter.wait_for_token(1, max_wait=0.5)
    elapsed = time.time() - start
    
    print(f"\n额度用完后等待 0.5s:")
    print(f"  返回值: {result}")
    print(f"  耗时: {elapsed:.3f}s")
    print(f"  期望: 返回=False, 耗时≈0.5s")
    
    passed = result is False and 0.4 < elapsed < 0.6
    print(f"测试 {'通过' if passed else '失败'}!\n")
    return passed


def test_issue_3_sliding_window_boundary():
    """问题3: 窗口交界处不应有双倍突发 (PER_REQUEST模式)"""
    print("=" * 60)
    print("测试 3: 窗口边界突发抑制 (PER_REQUEST 模式)")
    print("=" * 60)
    
    storage = InMemoryStorage()
    coordinator = DistributedCoordinator(
        storage=storage,
        global_limit=10,
        window_size=1.0,
        mode=CoordinationMode.PER_REQUEST,
        bucket_count=10
    )
    
    passed_first = 0
    for i in range(20):
        result = coordinator.try_acquire(1)
        if result.allowed:
            passed_first += 1
    
    print(f"  前半窗口通过: {passed_first} 次 (期望=10)")
    
    time.sleep(0.6)
    
    passed_second = 0
    for i in range(20):
        result = coordinator.try_acquire(1)
        if result.allowed:
            passed_second += 1
    
    print(f"  后半窗口通过: {passed_second} 次 (期望≈4, 滑动窗口平滑)")
    
    total = passed_first + passed_second
    print(f"  总计: {total} 次 (固定窗口会≈20, 滑动窗口应≈14-16)")
    
    passed = total < 18 and passed_first == 10
    print(f"测试 {'通过' if passed else '失败'}!\n")
    return passed


def test_issue_4_prefetch_total_limit():
    """问题4: 预取模式下总通过量不应超过全局上限"""
    print("=" * 60)
    print("测试 4: 预取模式多实例总配额限制")
    print("=" * 60)
    
    storage = InMemoryStorage()
    instances = []
    
    for i in range(3):
        coord = DistributedCoordinator(
            storage=storage,
            global_limit=20,
            window_size=1.0,
            mode=CoordinationMode.PRE_FETCH,
            prefetch_ratio=0.2,
            min_prefetch=2,
            max_prefetch=10,
            instance_id=f"inst_{i}"
        )
        instances.append(coord)
    
    total_passed = 0
    
    for round_num in range(15):
        for i, coord in enumerate(instances):
            result = coord.try_acquire(1)
            if result.allowed:
                total_passed += 1
                print(f"  第{round_num+1}轮 - 实例{i}: ✅ (总计={total_passed})")
            else:
                print(f"  第{round_num+1}轮 - 实例{i}: ❌ (总计={total_passed})")
    
    print(f"\n总通过数: {total_passed}")
    print(f"全局上限: 20")
    print(f"测试 {'通过' if total_passed <= 20 else '失败'}!\n")
    return total_passed <= 20


async def test_async_wait():
    """异步等待测试"""
    print("=" * 60)
    print("测试 5: 异步等待接口")
    print("=" * 60)
    
    limiter = create_async_rate_limiter(limit=3, per_second=True)
    
    for i in range(3):
        result = await limiter.try_acquire(1)
        print(f"  预热请求 {i+1}: {'✅' if result.allowed else '❌'}")
    
    start = time.time()
    result = await limiter.wait_for_token(1, max_wait=0.5)
    elapsed = time.time() - start
    
    print(f"\n额度用完后等待 0.5s:")
    print(f"  返回值: {result}")
    print(f"  耗时: {elapsed:.3f}s")
    
    passed = result is False and 0.4 < elapsed < 0.6
    print(f"测试 {'通过' if passed else '失败'}!\n")
    
    await limiter.close()
    return passed


if __name__ == "__main__":
    results = []
    
    results.append(("问题1: 基础限流精度", test_issue_1_basic_limit()))
    results.append(("问题2: wait_for_token 等待超时", test_issue_2_wait_for_token()))
    results.append(("问题3: 窗口边界突发抑制", test_issue_3_sliding_window_boundary()))
    results.append(("问题4: 预取模式总配额限制", test_issue_4_prefetch_total_limit()))
    
    results.append(("异步等待测试", asyncio.run(test_async_wait())))
    
    print("=" * 60)
    print("汇总")
    print("=" * 60)
    for name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {name}: {status}")
    
    all_passed = all(p for _, p in results)
    print(f"\n整体: {'全部通过' if all_passed else '存在失败'}")
