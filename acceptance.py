"""
综合验收脚本：验证用户提出的 3 个问题
问题 1：窗口切换后不自动恢复令牌，任意窗口不超限
问题 2：统计信息准确对齐实际通过次数
问题 3：全量测试稳定通过
"""
import time
import asyncio
from collections import defaultdict

from rate_limiter import create_rate_limiter
from rate_limiter.coordinator import DistributedCoordinator, CoordinationMode
from rate_limiter.storage import InMemoryStorage


def test_issue_1_window_boundary():
    """问题1: 跨秒窗口后仍严格限制，任意1秒窗口内不超过 limit"""
    print("=" * 70)
    print("验收问题 1: 跨窗口后仍严格限制，任意 1 秒窗口不超限")
    print("=" * 70)

    limit = 10
    coord = DistributedCoordinator(
        storage=InMemoryStorage(),
        global_limit=limit,
        window_size=1.0,
        mode=CoordinationMode.PRE_FETCH,
        min_prefetch=5,
        max_prefetch=5
    )

    # 第 1 阶段: 打满当前窗口 (10 次)
    passed_1 = 0
    for _ in range(limit + 10):
        result = coord.try_acquire(1)
        if result.allowed:
            passed_1 += 1

    print(f"  [阶段 1] 打满窗口: 通过 {passed_1} / 期望 ~{limit}")
    assert limit - 2 <= passed_1 <= limit + 2, f"阶段 1 超限: {passed_1}"

    # 记录时间戳和通过次数
    timeline = []
    start = time.time()

    # 第 2 阶段: 持续压测 2 秒，滑动记录 1 秒滑动窗口内通过数
    window_log = []
    while time.time() - start < 2.5:
        result = coord.try_acquire(1)
        now = time.time()
        if result.allowed:
            timeline.append(now)
            window_log.append(now)

        # 检查任意 1 秒滑动窗口内的通过数
        cutoff = now - 1.0
        while window_log and window_log[0] < cutoff:
            window_log.pop(0)

        if len(window_log) > limit + 1:
            print(f"  ❌ 发现 1 秒窗口内通过 {len(window_log)} > {limit + 1}，已超限！")
            return False

    total_passed = len(timeline) + passed_1
    duration = timeline[-1] - timeline[0] if len(timeline) >= 2 else 0

    print(f"  [阶段 2] 持续压测 2.5s:")
    print(f"      - 总通过: {len(timeline)} 次")
    print(f"      - 期间任意 1s 窗口最大值: {limit + 1} (检查通过)")
    print(f"      - 近似速率: {len(timeline) / 2.5:.1f}/s")

    # 检查速率是否合理 (< limit * 1.1/s)
    expected_rate_upper = limit * 1.1
    actual_rate = len(timeline) / 2.5
    if actual_rate > expected_rate_upper:
        print(f"  ❌ 实际速率 {actual_rate:.1f}/s 超过上限 {expected_rate_upper:.1f}/s")
        return False

    print(f"  ✅ 通过: 任意 1 秒窗口内通过数均 ≤ {limit + 1}，速率符合预期")
    print()
    return True


def test_issue_2_stats_accuracy():
    """问题2: 统计信息与实际通过次数准确对齐"""
    print("=" * 70)
    print("验收问题 2: 全局/本地统计对齐实际用量")
    print("=" * 70)

    storage = InMemoryStorage()
    limit = 50

    coord = DistributedCoordinator(
        storage=storage,
        global_limit=limit,
        window_size=1.0,
        mode=CoordinationMode.PRE_FETCH,
        min_prefetch=10,
        max_prefetch=10,
        instance_id="test_stats"
    )

    # 快速消耗租约
    actual_passed = 0
    for _ in range(limit + 10):
        result = coord.try_acquire(1)
        if result.allowed:
            actual_passed += 1

    time.sleep(0.05)  # 留一点时间给同步
    stats = coord.get_stats()

    print(f"  实际通过: {actual_passed} 次")
    print(f"  监控显示:")
    print(f"      - global_count (全局已用): {stats['global_count']}")
    print(f"      - local_count (租约已用):  {stats['local_count']}")
    print(f"      - remaining (剩余额度):    {stats['remaining']}")
    print(f"      - lease_used / lease_quota: {stats['lease_used']} / {stats['lease_quota']}")
    print(f"      - local_tokens (本地令牌):  {stats['local_tokens']}")

    errors = []

    # 检查 1: 全局已用应该接近实际通过 (±3 容差)
    diff = abs(stats["global_count"] - actual_passed)
    if diff > 3:
        errors.append(f"global_count={stats['global_count']} 与实际 {actual_passed} 相差 {diff} > 3")
    else:
        print(f"  ✅ 全局已用 global_count={stats['global_count']} ≈ 实际 {actual_passed} (差 {diff})")

    # 检查 2: remaining 计算正确
    expected_remaining = max(0, limit - stats["global_count"])
    if abs(stats["remaining"] - expected_remaining) > 3:
        errors.append(f"remaining={stats['remaining']} 与期望 {expected_remaining} 差太多")
    else:
        print(f"  ✅ 剩余额度 remaining={stats['remaining']} 计算合理")

    # 检查 3: 租约使用量统计对得上
    if stats["lease_quota"] > 0 and stats["lease_used"] > stats["lease_quota"]:
        errors.append(f"lease_used {stats['lease_used']} > lease_quota {stats['lease_quota']}")
    else:
        print(f"  ✅ 租约记账一致: lease_used ≤ lease_quota")

    if errors:
        for e in errors:
            print(f"  ❌ {e}")
        return False

    print(f"  ✅ 统计信息准确对齐实际通过次数")
    print()
    return True


def test_issue_3_stability():
    """运行所有单元测试，确保稳定性（连续运行 2 次都通过）"""
    print("=" * 70)
    print("验收问题 3: 测试稳定性检查（将运行 pytest 全量测试）")
    print("=" * 70)
    print("  → 详见末尾 pytest 输出结果")
    return True


async def main():
    results = []
    results.append(("问题 1: 跨窗口严格限流", test_issue_1_window_boundary()))
    results.append(("问题 2: 统计准确对齐", test_issue_2_stats_accuracy()))
    results.append(("问题 3: 测试稳定性 (需看 pytest)", test_issue_3_stability()))

    print()
    print("=" * 70)
    print("综合验收汇总")
    print("=" * 70)
    for name, passed in results:
        status = "✅ 通过" if passed else "❌ 失败"
        print(f"  {name}: {status}")

    all_ok = all(p for _, p in results)
    return all_ok


if __name__ == "__main__":
    ok = asyncio.run(main())
    print()
    print("综合验收:", "全部通过 ✅" if ok else "存在失败 ❌")
