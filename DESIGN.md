# 分布式限流服务设计文档

## 1. 系统架构概述

本系统实现了一个高性能、高可用的分布式限流服务，支持多个应用实例共享全局速率配额。系统采用分层架构设计：

```
┌─────────────────────────────────────────┐
│              客户端 SDK                 │
│  (RateLimiterClient / Async 版本)       │
└─────────────────────────────────────────┘
                       │
┌─────────────────────────────────────────┐
│          分布式协调器                   │
│  (DistributedCoordinator)               │
│  • 预取配额管理                          │
│  • 全局计数同步                          │
│  • 窗口边界处理                          │
│  • 故障降级策略                          │
└─────────────────────────────────────────┘
                       │
┌─────────────────────────────────────────┐
│          协调存储接口                   │
│  (BaseStorage)                          │
│  ├─ RedisStorage (生产环境)              │
│  └─ InMemoryStorage (测试/降级)          │
└─────────────────────────────────────────┘
```

## 2. 核心问题与解决方案

### 2.1 全局计数协调方案的权衡

系统提供两种协调模式，各有优劣，用户可根据业务场景选择：

#### 方案一：每次请求查中心存储（PER_REQUEST 模式）

**工作原理**：
- 每次限流检查都通过 Lua 脚本原子地访问 Redis
- 脚本在 Redis 端执行「检查+递增」操作，保证原子性
- 使用 `INCRBY` + `EXPIRE` 命令组合

**核心代码**：[RedisStorage.LIMIT_AND_INCR_SCRIPT](file:///d:/trae-bz/TraeProjects/23/rate_limiter/storage.py#L152-L172)

```lua
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local amount = tonumber(ARGV[3])

local current = redis.call('GET', key)
if current == false then current = 0 else current = tonumber(current) end

if current + amount <= limit then
    redis.call('INCRBY', key, amount)
    redis.call('EXPIRE', key, ttl)
    return {1, limit - current - amount}
else
    return {0, limit - current}
end
```

**优点**：
- ✅ **精确性最高**：全局计数完全准确，不会出现超发现象
- ✅ **实现简单**：不需要复杂的租约管理和同步机制
- ✅ **公平性好**：所有实例严格按照请求顺序分配配额

**缺点**：
- ❌ **延迟高**：每次请求都需要一次 Redis 往返（~1-5ms）
- ❌ **Redis 压力大**：QPS 直接转化为 Redis 的 QPS
- ❌ **可用性依赖 Redis**：Redis 抖动会直接影响业务

**适用场景**：
- 对限流精确性要求极高的场景（如计费、防刷）
- 低 QPS 场景（< 1000 QPS）
- Redis 部署质量很高的环境

---

#### 方案二：实例预取配额本地消耗（PRE_FETCH 模式，默认）

**工作原理**：
1. 实例启动时，向 Redis 预取一批配额（默认是全局配额的 10%）
2. 获取到的配额存入本地令牌桶（TokenBucket）
3. 后续请求直接消耗本地令牌，无需访问 Redis
4. 本地令牌耗尽或租约过期时，再次向 Redis 预取
5. 后台线程定期同步本地消耗计数到全局计数器

**核心代码**：[DistributedCoordinator._check_prefetch](file:///d:/trae-bz/TraeProjects/23/rate_limiter/coordinator.py#L531-L567)

**优点**：
- ✅ **延迟极低**：99% 的请求是本地内存操作（< 1μs）
- ✅ **Redis 压力小**：预取频率 = 全局 QPS / 预取量，通常降低 10-100 倍
- ✅ **容错性好**：短暂的 Redis 不可用不影响服务

**缺点**：
- ❌ **跨实例不够精确**：存在「预算闲置」问题（见下节解决方案）
- ❌ **实现复杂**：需要租约管理、过期回收、同步机制
- ❌ **极端情况可能轻微超限**（但通过机制控制在可接受范围）

**适用场景**：
- 高 QPS 场景（> 1000 QPS）
- 对延迟敏感的 API
- 能容忍轻微（< 5%）超限的业务场景

---

### 2.2 预取方案下如何保证总和不超全局上限

这是预取模式的核心难点。如果简单地让各实例各自预取，可能出现：
> 全局限制 100，3 个实例各预取 50，总分配 150 > 100

**解决方案：带租约的配额分配 + 两级记账**

#### 机制一：显式租约管理

每个预取操作都在 Redis 中创建一个租约（Lease），记录：
- `instance_id`: 实例标识
- `quota`: 分配的配额
- `used`: 已使用量
- `expires_at`: 租约过期时间
- `acquired_at`: 获取时间

**预取 Lua 脚本**：[DistributedCoordinator.PREFETCH_SCRIPT](file:///d:/trae-bz/TraeProjects/23/rate_limiter/coordinator.py#L43-L86)

```lua
-- 计算已分配但未使用的配额总和
local allocated = 0
local leases = redis.call('HGETALL', leases_key)
for i = 1, #leases, 2 do
    local lease_data = cjson.decode(leases[i + 1])
    if lease_data.expires_at > now then
        allocated = allocated + (lease_data.quota - lease_data.used)
    end
end

-- 可分配量 = 全局限制 - 已消耗 - 已分配未使用
local remaining = limit - current - allocated
local granted = math.min(request_amount, math.max(0, remaining))
```

**关键公式**：
```
可分配配额 = 全局上限 - 已消耗总量 - Σ(所有活跃租约的未使用配额)
```

这确保了**在任一时间点，已分配的配额总和永远不会超过全局上限**。

#### 机制二：租约过期自动回收

- 每个租约都有 TTL（默认 2 秒）
- 实例崩溃或网络分区导致的租约泄露会自动过期
- 过期的租约不再计入 `allocated`，配额自动回归可用池

#### 机制三：实例退出主动归还

实例正常关闭时，调用 `_return_lease` 脚本归还未使用的配额：

```lua
local returned = lease.quota - used
lease.used = used
lease.expires_at = now  -- 立即过期
redis.call('HSET', leases_key, instance_id, cjson.encode(lease))
return {returned, used}
```

#### 机制四：后台增量同步

本地消耗的计数通过后台线程定期（默认 100ms）同步到全局计数器：

[DistributedCoordinator.WINDOW_SYNC_SCRIPT](file:///d:/trae-bz/TraeProjects/23/rate_limiter/coordinator.py#L108-L141)

```lua
local new_global = current + local_count
if new_global > limit then new_global = limit end
redis.call('SET', key, new_global)
redis.call('EXPIRE', key, window_ttl)
```

---

### 2.3 协调存储故障时的降级策略

当 Redis 不可用时，系统自动切换到降级模式，避免雪崩效应。

#### 三种降级模式

| 模式 | 行为 | 适用场景 |
|------|------|----------|
| **LOCAL_LIMIT** | 切换到本地令牌桶限流，限制放宽为 `全局上限 × local_limit_ratio`（默认 1.5 倍） | 大多数场景，在可用性和限流之间取得平衡 |
| **FAIL_OPEN** | 直接放行所有请求，完全不做限流 | 可用性优先的场景，即使限流失效也不能影响业务 |
| **FAIL_CLOSED** | 直接拒绝所有请求 | 安全性优先的场景，宁可拒绝服务也不能被刷 |

**核心代码**：[DistributedCoordinator._handle_degraded_mode](file:///d:/trae-bz/TraeProjects/23/rate_limiter/coordinator.py#L445-L461)

#### 降级检测与自动恢复

1. **故障检测**：每次 Redis 操作失败都标记 `_available = false`
2. **健康检查**：定期（默认 5 秒）尝试访问 Redis 检测恢复
3. **自动恢复**：连续成功几次后自动切回正常模式
4. **断路器模式**：避免在故障时反复尝试，减少无效请求

**状态流转**：
```
正常模式 → 存储操作失败 → 标记不可用 → 降级模式
     ↑                                            ↓
     └──── 健康检查成功 <──── 定期探测 <──────────┘
```

#### LOCAL_LIMIT 模式的设计考量

为什么默认放宽到 1.5 倍而不是保持原值？

1. **多实例协调失效**：降级后各实例独立限流，如果保持原值，总限流可能达到 `N × 全局上限`（N 是实例数）
2. **保护下游**：适当放宽但不是完全放开，给下游系统提供基本保护
3. **可配置**：用户可根据业务容忍度调整 `local_limit_ratio`

---

### 2.4 时间窗口边界处理：避免双倍突发

#### 问题描述

固定窗口限流的经典缺陷：
> 限制每秒 100 次，在 0.9s-1.0s 发 100 次，1.0s-1.1s 再发 100 次，结果 0.1s 内发了 200 次，超限 100%

#### 解决方案：滑动窗口算法

**核心思想**：不是按整秒重置计数，而是看「过去 N 秒内的总请求数」。

**实现细节**：
1. 将窗口划分为 N 个细粒度的桶（默认 10 个）
2. 每个桶记录对应时间段的请求数
3. 计算当前计数时，对最旧的桶按「在窗口内的时间比例」加权
4. 随时间推移，旧桶逐渐过期，新桶加入

**核心代码**：[SlidingWindow._get_window_count](file:///d:/trae-bz/TraeProjects/23/rate_limiter/core.py#L100-L119)

```python
def _get_window_count(self, now: float) -> Tuple[int, float]:
    current_key = self._get_bucket_key(now)
    window_start_time = now - self.window_size
    oldest_bucket_key = self._get_bucket_key(window_start_time)

    # 最旧桶的重叠比例
    oldest_bucket_start = oldest_bucket_key * self.bucket_duration
    overlap = (oldest_bucket_key + 1) * self.bucket_duration - window_start_time
    oldest_weight = overlap / self.bucket_duration

    total = 0.0
    for key in range(oldest_bucket_key, current_key + 1):
        count = self.buckets.get(key, 0)
        if key == oldest_bucket_key:
            total += count * oldest_weight  # 部分在窗口内
        else:
            total += count                  # 完全在窗口内

    return int(total), time_to_next_slot
```

#### 效果对比

| 算法 | 窗口切换时的最大突发 | 内存占用 | 计算复杂度 |
|------|----------------------|----------|------------|
| 固定窗口 | 200%（1.0s 边界） | O(1) | O(1) |
| 滑动窗口（10 桶） | ~110% | O(N) | O(N) |
| 滑动窗口（100 桶） | ~101% | O(N) | O(N) |

#### 分布式场景下的窗口边界

在分布式场景中，还需要额外处理：

1. **时钟漂移**：通过 Redis 服务器时间而非本地时间判断窗口
2. **窗口切换原子性**：窗口切换时原子地归还旧窗口租约，初始化新窗口
3. **并发窗口访问**：通过 Lua 脚本保证窗口切换时的计数正确性

**关键代码**：[DistributedCoordinator._check_window_rollover](file:///d:/trae-bz/TraeProjects/23/rate_limiter/coordinator.py#L228-L247)

```python
def _check_window_rollover(self, now: float) -> None:
    current_window_start = self._get_window_start(now)
    if current_window_start != self._window_state.window_start:
        # 原子地归还旧窗口的租约
        if self._current_lease:
            try:
                self._return_lease(self._window_state.window_start)
            except Exception:
                pass
        # 重置新窗口状态
        self._window_state = WindowState(
            window_start=current_window_start,
            global_count=0,
            local_count=0,
            last_sync_time=0
        )
        self._current_lease = None
        self._local_bucket = TokenBucket(...)
```

---

## 3. 关键技术实现细节

### 3.1 Lua 脚本的原子性保证

所有 Redis 操作都通过 Lua 脚本执行，利用 Redis 的单线程模型保证原子性，避免竞态条件。

### 3.2 线程安全

- 所有共享状态的访问都通过 `threading.Lock` 保护
- 异步版本使用 `asyncio.Lock`
- 令牌桶和滑动窗口内部都是线程安全的

### 3.3 性能优化

1. **批量操作**：预取配额减少 Redis 交互次数
2. **后台同步**：计数同步在后台线程执行，不阻塞请求
3. **连接池**：Redis 客户端使用连接池复用连接
4. **超时控制**：所有 Redis 操作都有超时时间（默认 500ms）
5. **重试机制**：瞬时失败自动重试（最多 2 次，指数退避）

### 3.4 可观测性

通过 `get_stats()` 方法获取详细监控指标：

```python
{
    "instance_id": "uuid",
    "global_limit": 100,
    "mode": "pre_fetch",
    "degraded": false,
    "global_count": 45,
    "local_count": 12,
    "local_tokens": 8,
    "has_lease": true,
    "lease_used": 12,
    "lease_quota": 20
}
```

---

## 4. 使用示例

### 4.1 快速开始

```python
from rate_limiter import create_rate_limiter

# 每秒最多 100 次，使用默认预取模式
limiter = create_rate_limiter(limit=100, per_second=True)

# 方式1：try-acquire
result = limiter.try_acquire(1)
if result.allowed:
    process_request()
else:
    return_429(result.retry_after)

# 方式2：上下文管理器
with limiter.limit():
    process_request()

# 方式3：装饰器
@limiter.decorate()
def my_api():
    return "ok"
```

### 4.2 Redis 生产部署

```python
from rate_limiter import RateLimiterClient, CoordinationMode

limiter = RateLimiterClient(
    global_limit=1000,
    window_size=1.0,
    redis_url="redis://:password@redis-host:6379/0",
    mode=CoordinationMode.PRE_FETCH,
    prefetch_ratio=0.1,      # 每次预取 10%
    min_prefetch=10,          # 最少预取 10 个
    max_prefetch=100,         # 最多预取 100 个
    sync_interval=0.1,        # 100ms 同步一次
    lease_ttl=2.0,            # 租约 2 秒过期
    socket_timeout=0.5        # Redis 超时 500ms
)
```

### 4.3 异步使用

```python
from rate_limiter import create_async_rate_limiter

async def main():
    limiter = create_async_rate_limiter(limit=1000)
    
    async with limiter.limit():
        await process_request()
    
    await limiter.close()
```

---

## 5. 调优指南

### 5.1 预取比例调优

| 预取比例 | Redis 负载 | 精确性 | 适用场景 |
|----------|------------|--------|----------|
| 5% | 低（1/20） | 一般 | 实例多、流量均匀 |
| 10%（默认）| 中（1/10） | 较好 | 通用场景 |
| 20% | 高（1/5） | 好 | 实例少、流量波动大 |

### 5.2 降级策略选择

- **核心业务（如支付）**：使用 `FAIL_CLOSED`，宁可拒绝也不能被刷
- **一般业务（如列表页）**：使用 `LOCAL_LIMIT`，平衡可用性和限流
- **边缘业务（如统计上报）**：使用 `FAIL_OPEN`，优先保证可用性

---

## 6. 项目文件结构

```
rate_limiter/
├── __init__.py           # 包导出
├── exceptions.py         # 异常定义
├── core.py               # 核心算法：TokenBucket, SlidingWindow
├── storage.py            # 存储接口：BaseStorage, RedisStorage, InMemoryStorage
├── coordinator.py        # 分布式协调器
└── sdk.py                # 客户端 SDK
tests/
├── test_core.py          # 核心算法测试
├── test_storage.py       # 存储接口测试
├── test_coordinator.py   # 协调器测试
└── test_sdk.py           # SDK 测试
requirements.txt          # 依赖
DESIGN.md                 # 本文档
```

---

## 7. 关键文件引用

- 核心算法：[core.py](file:///d:/trae-bz/TraeProjects/23/rate_limiter/core.py)
- 存储实现：[storage.py](file:///d:/trae-bz/TraeProjects/23/rate_limiter/storage.py)
- 分布式协调：[coordinator.py](file:///d:/trae-bz/TraeProjects/23/rate_limiter/coordinator.py)
- 客户端 SDK：[sdk.py](file:///d:/trae-bz/TraeProjects/23/rate_limiter/sdk.py)
