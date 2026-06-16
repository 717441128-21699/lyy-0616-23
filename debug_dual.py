"""调试双实例预取全局限制"""
import time
from rate_limiter.coordinator import DistributedCoordinator, CoordinationMode
from rate_limiter.storage import InMemoryStorage

storage = InMemoryStorage()
limit = 10

coord_a = DistributedCoordinator(
    storage=storage,
    global_limit=limit,
    window_size=1.0,
    mode=CoordinationMode.PRE_FETCH,
    min_prefetch=3,
    max_prefetch=5,
    instance_id="instance_a",
    sync_interval=0.0  # 禁用节流，每次都同步
)

coord_b = DistributedCoordinator(
    storage=storage,
    global_limit=limit,
    window_size=1.0,
    mode=CoordinationMode.PRE_FETCH,
    min_prefetch=3,
    max_prefetch=5,
    instance_id="instance_b",
    sync_interval=0.0
)

ws = coord_a._get_window_start(time.time())
ck = coord_a._get_counter_key(ws)
lk = coord_a._get_leases_key(ws)

print(f"窗口起始: {ws}, counter_key={ck}, leases_key={lk}")
print()

for i in range(30):
    # A 和 B 交替请求
    for inst_name, coord in [("A", coord_a), ("B", coord_b)]:
        try:
            result = coord.try_acquire(1)
            s = coord.get_stats()
            cval = storage.get(ck)
            c = cval.value if cval and cval.success else "N/A"
            lval = storage.get(lk)
            l = "有" if lval and lval.success else "无"

            print(f"[{inst_name} req{i+1}] allowed={result.allowed} "
                  f"local_total={s['local_used_total']} "
                  f"synced={s['synced_to_center']} pending={s['pending_sync']} "
                  f"lease={s['lease_used']}/{s['lease_quota']} "
                  f"tokens={s['local_tokens']} rem={s['remaining']}")
            print(f"    storage: counter={c}, leases={l}")

            if not result.allowed and inst_name == "B" and i > 5:
                # 同时看两者的状态
                sa = coord_a.get_stats()
                sb = coord_b.get_stats()
                print(f"  [B拒绝时状态]")
                print(f"    A: total={sa['local_used_total']} synced={sa['synced_to_center']} lease={sa['lease_used']}/{sa['lease_quota']}")
                print(f"    B: total={sb['local_used_total']} synced={sb['synced_to_center']} lease={sb['lease_used']}/{sb['lease_quota']}")

        except Exception as e:
            print(f"[{inst_name} req{i+1}] EXCEPTION: {e}")
            import traceback
            traceback.print_exc()

    print()
