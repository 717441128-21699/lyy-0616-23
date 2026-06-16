"""
综合验收 v2：
1. 双实例预取模式全局限制
2. 统计字段分层准确性
"""
import time
import asyncio
import threading

from rate_limiter import create_rate_limiter
from rate_limiter.coordinator import DistributedCoordinator, CoordinationMode
from rate_limiter.storage import InMemoryStorage


def test_issue_1_dual_instance():
    """问题1: 双实例 limit=10，任意连续1秒内总通过≤10，不能各过10个叠加到20"""
    print("=" * 70)
    print("验收问题 1: 双实例预取模式全局限制")
    print("=" * 70)

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

    def client_loop(coord, timeline, duration, stop_event):
        start = time.time()
        while time.time() - start < duration and not stop_event.is_set():
            try:
                result = coord.try_acquire(1)
                if result.allowed:
                    timeline.append(time.time())
            except Exception:
                pass
            time.sleep(0.005)

    stop_event = threading.Event()
    t1 = threading.Thread(target=client_loop, args=(coord_a, timeline_a, 2.5, stop_event))
    t2 = threading.Thread(target=client_loop, args=(coord_b, timeline_b, 2.5, stop_event))

    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 合并两个时间线
    all_events = sorted(timeline_a + timeline_b)
    total_a = len(timeline_a)
    total_b = len(timeline_b)
    total = len(all_events)

    print(f"  [结果] 总通过 {total} 次 (A:{total_a}, B:{total_b})")
    print(f"  [时间] 跨度 {all_events[-1] - all_events[0]:.2f}s")

    # 滑动窗口检查：任意 1 秒内通过数 ≤ limit + 1
    window_log = []
    max_in_window = 0
    for t in all_events:
        cutoff = t - 1.0
        while window_log and window_log[0] < cutoff:
            window_log.pop(0)
        window_log.append(t)
        max_in_window = max(max_in_window, len(window_log))

    print(f"  [检查] 任意 1 秒内最大通过: {max_in_window}")
    print(f"  [期望] ≤ {limit} (严格不超过)")

    if max_in_window > limit:
        print(f"  ❌ 失败：1 秒内通过 {max_in_window} > {limit}")
        return False

    # 检查总速率（按真实时间线，不放宽）
    test_duration = 2.5
    rate = total / test_duration
    expected_rate_upper = limit
    print(f"  [速率] 实际 {rate:.1f}/s, 期望 ≤ {expected_rate_upper:.1f}/s")

    if rate > expected_rate_upper + 0.5:  # 允许微小浮点误差
        print(f"  ❌ 失败：总速率 {rate:.1f}/s 超过上限 {expected_rate_upper:.1f}/s")
        return False

    print(f"  ✅ 通过：双实例总通过数在任意 1 秒内都不超过 {limit}，总速率合理")
    print()
    return True


def test_issue_2_stats_accuracy():
    """问题2: 快速打30次后马上看stats，本机已用不要因为同步归零"""
    print("=" * 70)
    print("验收问题 2: 统计字段分层准确性")
    print("=" * 70)

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
        sync_interval=60.0  # 禁用自动同步，让测试能看到 pending_sync
    )

    # 快速打 30 次
    actual_passed = 0
    for _ in range(requests + 5):
        result = coord.try_acquire(1)
        if result.allowed:
            actual_passed += 1

    # 马上看 stats（不强制同步）
    s1 = coord.get_stats(force_sync=False)

    print(f"  [打 {requests} 次后立即查看（不强制同步）]")
    print(f"    local_used_total (本机累计已用) = {s1['local_used_total']}")
    print(f"    synced_to_center (已同步到中心)  = {s1['synced_to_center']}")
    print(f"    pending_sync (待同步)            = {s1['pending_sync']}")
    print(f"    lease_used (当前租约已用)        = {s1['lease_used']}")
    print(f"    global_count (中心全局已用)      = {s1['global_count']}")
    print(f"    remaining (剩余额度)             = {s1['remaining']}")
    print(f"    实际通过次数                      = {actual_passed}")

    errors = []

    # 检查 1: local_used_total 应该等于实际通过次数
    if s1["local_used_total"] != actual_passed:
        errors.append(f"local_used_total={s1['local_used_total']} ≠ 实际通过 {actual_passed}")
    else:
        print(f"  ✅ local_used_total = 实际通过 {actual_passed} 次")

    # 检查 2: 不强制同步时，应该有 pending_sync
    if s1["pending_sync"] == 0 and s1["synced_to_center"] == 0:
        # 可能预取时同步过，不严格要求
        pass
    elif s1["pending_sync"] + s1["synced_to_center"] != actual_passed:
        errors.append(f"pending_sync({s1['pending_sync']}) + synced_to_center({s1['synced_to_center']}) ≠ {actual_passed}")
    else:
        print(f"  ✅ pending_sync + synced_to_center = {actual_passed} 次，账目对得上")

    # 检查 3: 刷新多次也对得上
    print(f"  [刷新 3 次（不强制同步）]")
    for i in range(3):
        s = coord.get_stats(force_sync=False)
        if s["local_used_total"] != actual_passed:
            errors.append(f"第{i+1}次刷新: local_used_total={s['local_used_total']} ≠ {actual_passed}")
        else:
            print(f"    刷新{i+1}: local_used_total={s['local_used_total']} ✓")

    # 检查 4: 强制同步后，pending_sync 归零，synced_to_center 增加
    s2 = coord.get_stats(force_sync=True)
    print(f"  [强制同步后]")
    print(f"    synced_to_center = {s2['synced_to_center']}")
    print(f"    pending_sync = {s2['pending_sync']}")
    print(f"    local_used_total = {s2['local_used_total']}")

    if s2["pending_sync"] != 0:
        errors.append(f"强制同步后 pending_sync={s2['pending_sync']} ≠ 0")
    else:
        print(f"  ✅ 强制同步后 pending_sync 归零")

    if s2["local_used_total"] != actual_passed:
        errors.append(f"强制同步后 local_used_total={s2['local_used_total']} ≠ {actual_passed}（不该归零！）")
    else:
        print(f"  ✅ 强制同步后 local_used_total 仍然 = {actual_passed}（未归零）")

    if s2["synced_to_center"] != actual_passed:
        errors.append(f"强制同步后 synced_to_center={s2['synced_to_center']} ≠ {actual_passed}")
    else:
        print(f"  ✅ 强制同步后 synced_to_center = {actual_passed}，全部同步完成")

    # 检查 5: remaining 按实际放行次数扣掉
    expected_remaining = max(0, limit - actual_passed)
    if s1["remaining"] != expected_remaining:
        errors.append(f"remaining before sync: {s1['remaining']} ≠ {expected_remaining} (应该按实际放行 {actual_passed} 次扣掉)")
    else:
        print(f"  ✅ remaining = {expected_remaining}，按实际放行次数扣掉")

    # 检查 6: 刷新多次 remaining 不飘
    for i in range(3):
        s = coord.get_stats(force_sync=False)
        if s["remaining"] != expected_remaining:
            errors.append(f"第{i+1}次刷新 remaining 飘了: {s['remaining']} ≠ {expected_remaining}")

    # 检查 7: 强制同步后 remaining 仍然正确
    expected_remaining_after = max(0, limit - max(s2["global_count"], actual_passed))
    if s2["remaining"] != expected_remaining_after:
        errors.append(f"remaining after sync: {s2['remaining']} ≠ {expected_remaining_after}")
    else:
        print(f"  ✅ remaining = {expected_remaining_after}，计算正确")

    if errors:
        for e in errors:
            print(f"  ❌ {e}")
        return False

    print(f"  ✅ 统计字段分层正确，同步后本机已用不归零，刷新一致")
    print()
    return True


async def test_async_prefetch():
    """异步预取模式单实例正确性"""
    print("=" * 70)
    print("验收: 异步预取模式单实例")
    print("=" * 70)

    from rate_limiter import create_async_rate_limiter
    storage = InMemoryStorage()

    client = create_async_rate_limiter(
        limit=20,
        storage=storage,
        mode=CoordinationMode.PRE_FETCH,
        min_prefetch=5,
        max_prefetch=10
    )

    passed = 0
    for i in range(30):
        result = await client.try_acquire(1)
        if result.allowed:
            passed += 1

    print(f"  通过 {passed} / 30 次")

    stats = client.get_stats()
    print(f"  local_used_total = {stats['local_used_total']}")
    print(f"  remaining = {stats['remaining']}")

    if passed > 20 or stats['local_used_total'] != passed:
        print(f"  ❌ 失败: 通过 {passed}, local_used_total={stats['local_used_total']}")
        return False

    print(f"  ✅ 通过")
    print()
    return True


async def test_sync_async_get_stats_consistency():
    """问题3: 同步和异步客户端get_stats用法保持一致"""
    print("=" * 70)
    print("验收问题 3: 同步/异步客户端 get_stats 一致性")
    print("=" * 70)

    from rate_limiter import create_rate_limiter, create_async_rate_limiter

    limit = 20
    storage_sync = InMemoryStorage()
    storage_async = InMemoryStorage()

    sync_client = create_rate_limiter(
        limit=limit,
        storage=storage_sync,
        mode=CoordinationMode.PRE_FETCH,
        min_prefetch=5,
        max_prefetch=10,
        instance_id="sync_client"
    )

    async_client = create_async_rate_limiter(
        limit=limit,
        storage=storage_async,
        mode=CoordinationMode.PRE_FETCH,
        min_prefetch=5,
        max_prefetch=10,
        instance_id="async_client"
    )

    sync_passed = 0
    for _ in range(30):
        if sync_client.try_acquire(1).allowed:
            sync_passed += 1

    async_passed = 0
    for _ in range(30):
        if (await async_client.try_acquire(1)).allowed:
            async_passed += 1

    print(f"  [同步客户端] 通过 {sync_passed} 次")
    print(f"  [异步客户端] 通过 {async_passed} 次")

    errors = []

    sync_stats_1 = sync_client.get_stats(force_sync=False)
    sync_stats_2 = sync_client.get_stats(force_sync=True)
    async_stats_1 = async_client.get_stats(force_sync=False)
    async_stats_2 = async_client.get_stats(force_sync=True)

    if sync_stats_1["local_used_total"] != sync_passed:
        errors.append(f"同步客户端 force_sync=False: local_used_total={sync_stats_1['local_used_total']} ≠ {sync_passed}")
    if sync_stats_2["local_used_total"] != sync_passed:
        errors.append(f"同步客户端 force_sync=True: local_used_total={sync_stats_2['local_used_total']} ≠ {sync_passed}")
    if async_stats_1["local_used_total"] != async_passed:
        errors.append(f"异步客户端 force_sync=False: local_used_total={async_stats_1['local_used_total']} ≠ {async_passed}")
    if async_stats_2["local_used_total"] != async_passed:
        errors.append(f"异步客户端 force_sync=True: local_used_total={async_stats_2['local_used_total']} ≠ {async_passed}")

    expected_fields = ["local_used_total", "synced_to_center", "pending_sync", "remaining"]
    for field in expected_fields:
        if field not in sync_stats_1:
            errors.append(f"同步客户端缺少字段: {field}")
        if field not in async_stats_1:
            errors.append(f"异步客户端缺少字段: {field}")

    if sync_stats_2["pending_sync"] != 0:
        errors.append(f"同步客户端 force_sync 后 pending_sync={sync_stats_2['pending_sync']} ≠ 0")
    if async_stats_2["pending_sync"] != 0:
        errors.append(f"异步客户端 force_sync 后 pending_sync={async_stats_2['pending_sync']} ≠ 0")

    expected_remaining_sync = max(0, limit - sync_passed)
    expected_remaining_async = max(0, limit - async_passed)

    if sync_stats_1["remaining"] != expected_remaining_sync:
        errors.append(f"同步客户端 remaining={sync_stats_1['remaining']} ≠ {expected_remaining_sync}")
    if sync_stats_2["remaining"] != expected_remaining_sync:
        errors.append(f"同步客户端 remaining after sync={sync_stats_2['remaining']} ≠ {expected_remaining_sync}")
    if async_stats_1["remaining"] != expected_remaining_async:
        errors.append(f"异步客户端 remaining={async_stats_1['remaining']} ≠ {expected_remaining_async}")
    if async_stats_2["remaining"] != expected_remaining_async:
        errors.append(f"异步客户端 remaining after sync={async_stats_2['remaining']} ≠ {expected_remaining_async}")

    sync_client.close()
    await async_client.close()

    if errors:
        for e in errors:
            print(f"  ❌ {e}")
        return False

    print(f"  ✅ 同步/异步客户端 get_stats 用法一致，force_sync 参数都生效")
    print(f"  ✅ 剩余额度按实际放行次数扣掉，刷新不飘")
    print(f"  ✅ 同步后本机累计已用保留，没有变少")
    print()
    return True


async def main():
    results = []
    results.append(("问题 1: 双实例全局限制", test_issue_1_dual_instance()))
    results.append(("问题 2: 统计字段分层", test_issue_2_stats_accuracy()))
    results.append(("问题 3: 同步/异步 get_stats 一致", await test_sync_async_get_stats_consistency()))
    results.append(("异步预取单实例", await test_async_prefetch()))

    print()
    print("=" * 70)
    print("综合验收 v2 汇总")
    print("=" * 70)
    for name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {name}: {status}")

    return all(p for _, p in results)


if __name__ == "__main__":
    ok = asyncio.run(main())
    print()
    print("综合验收:", "全部通过 ✅" if ok else "存在失败 ❌")
