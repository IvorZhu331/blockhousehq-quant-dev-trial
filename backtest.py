import json
from kafka import KafkaConsumer
from collections import defaultdict
from math import ceil
from datetime import datetime
from itertools import product

# === Configuration ===
TOTAL_ORDER         = 5000
TOPIC               = "mock_l1_stream"
BROKER              = "kafka:9092"
STEP                = 100
# parameter grid
GRID = {
    "lambda_over":  [5],
    "lambda_under": [0.1],
    "theta_queue":  [0.3]
}

# === Data Structures ===
class Venue:
    def __init__(self, ask, ask_size, fee=0.0, rebate=0.0):
        self.ask      = ask
        self.ask_size = ask_size
        self.fee      = fee
        self.rebate   = rebate

# === Cost / Allocator ===
def compute_cost(split, venues, order_size,
                 lambda_over, lambda_under, theta_queue):
    executed, cash_spent = 0, 0.0
    for i, v in enumerate(venues):
        eve = min(split[i], v.ask_size)
        executed    += eve
        cash_spent  += eve * (v.ask + v.fee)
        # penalty/rebate for any over-allocation
        rebate_amt  = max(split[i] - eve, 0) * v.rebate
        cash_spent -= rebate_amt

    underfill = max(order_size - executed, 0)
    overfill  = max(executed - order_size, 0)
    risk_pen  = theta_queue * (underfill + overfill)
    cost_pen  = lambda_under * underfill + lambda_over * overfill

    return cash_spent + risk_pen + cost_pen

def allocate(order_size, venues,
             lambda_over, lambda_under, theta_queue):
    # build candidate splits in `step`-sized chunks
    splits = [[]]
    for v in venues:
        new_splits = []
        for alloc in splits:
            used  = sum(alloc)
            cap   = min(order_size - used, v.ask_size)
            for q in range(0, cap + 1, STEP):
                new_splits.append(alloc + [q])
        splits = new_splits

    # pick lowest-cost split that sums to order_size
    best_cost, best_split = float('inf'), []
    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size,
                            lambda_over, lambda_under, theta_queue)
        if cost < best_cost:
            best_cost, best_split = cost, alloc[:]
    return best_split, best_cost

# === Backtest runner ===

def run_sor(snapshots, lambda_over, lambda_under, theta_queue):
    filled, cash = 0, 0.0

    for snap in snapshots:
        if filled >= TOTAL_ORDER:
            break

        remaining = TOTAL_ORDER - filled
        venues = snap.get("venues", [])
        v = venues[0]
        sz = v.get("ask_sz_00", 0)
        px = v.get("ask_px_00", 0.0)
        allocatable = min(sz, remaining - (remaining % STEP))

        vs = [Venue(ask=px, ask_size=sz)]
        split, _ = allocate(allocatable, vs,
                            lambda_over, lambda_under, theta_queue)

        for i, qty in enumerate(split):
            exe = min(qty, vs[i].ask_size)
            filled += exe
            cash   += exe * vs[i].ask
            ts = snap.get("timestamp", "n/a")
            print(f"[{ts}] Buy {qty:4d} @ {vs[0].ask:.2f}   "
                f"(filled {filled}/{TOTAL_ORDER})")

    avg_price = cash / filled if filled else float('inf')
    return {"total_cash": cash, "avg_price": avg_price}

# === Baselines ===
def run_best_ask(snapshots):
    filled, cash = 0, 0.0

    for snap in snapshots:
        if filled >= TOTAL_ORDER:
            break

        # pick the venue with the lowest ask that actually has size
        best = min(
            (v for v in snap.get("venues", []) if v.get("ask_sz_00", 0) > 0),
            key=lambda v: v["ask_px_00"],
            default=None
        )
        if not best:
            continue

        exe = min(best["ask_sz_00"], TOTAL_ORDER - filled)
        filled += exe
        cash   += exe * best["ask_px_00"]

    avg_price = (cash / filled) if filled else float("inf")
    return {"total_cash": cash, "avg_price": avg_price}

def run_twap(snapshots, bucket_secs=60):
    def to_secs(ts_str):
        ts = ts_str.replace("Z", "+00:00")
        if "." in ts:
            base, frac = ts.split(".", 1)
            frac = frac.split("+", 1)[0][:6]
            ts = f"{base}.{frac}+00:00"
        return int(datetime.fromisoformat(ts).timestamp())

    snaps = sorted(snapshots, key=lambda s: to_secs(s["timestamp"]))
    start, end = to_secs(snaps[0]["timestamp"]), to_secs(snaps[-1]["timestamp"])
    n_buckets  = max(1, ceil((end - start) / bucket_secs))
    per_bucket = TOTAL_ORDER / n_buckets

    filled, cash = 0, 0.0
    bucket_filled = defaultdict(float)

    for snap in snaps:
        if filled >= TOTAL_ORDER:
            break

        sec   = to_secs(snap["timestamp"]) - start
        b_idx = int(sec // bucket_secs)
        if bucket_filled[b_idx] >= per_bucket:
            continue

        venues = snap.get("venues", [])
        if not venues:
            continue

        v = venues[0]
        price = v.get("ask_px_00", 0.0)
        size  = v.get("ask_sz_00", 0)

        if size <= 0:
            continue

        need = per_bucket - bucket_filled[b_idx]
        exe  = min(size, need)
        bucket_filled[b_idx] += exe
        filled   += exe
        cash     += exe * price

    avg_price = (cash / filled) if filled else float('inf')
    return {"total_cash": cash, "avg_price": avg_price}

def run_vwap(snapshots):
    # Pass 1 ─ total liquidity across the window
    total_liquidity = sum(
        v.get("ask_sz_00", 0)
        for snap in snapshots
        for v in snap.get("venues", [])
        if v.get("ask_sz_00", 0) > 0
    )
    if total_liquidity == 0:
        return {"total_cash": 0.0, "avg_price": float("inf")}

    filled, cash = 0, 0.0

    # Pass 2 ─ allocate proportionally snapshot-by-snapshot
    for snap in snapshots:
        if filled >= TOTAL_ORDER:
            break

        # volume in this snapshot
        snap_vol = sum(
            v.get("ask_sz_00", 0)
            for v in snap.get("venues", [])
            if v.get("ask_sz_00", 0) > 0
        )
        if snap_vol == 0:
            continue

        # target fill for this snapshot (proportional to its share of total volume)
        target = TOTAL_ORDER * snap_vol / total_liquidity
        target = min(target, TOTAL_ORDER - filled)  # don’t over-fill

        # fill across venues, cheapest first
        for v in sorted(snap.get("venues", []), key=lambda vv: vv["ask_px_00"]):
            size = v.get("ask_sz_00", 0)
            if size <= 0 or target <= 0:
                continue

            exe = min(size, target)
            filled += exe
            cash   += exe * v["ask_px_00"]
            target -= exe

            if filled >= TOTAL_ORDER:
                break  # done

    avg_price = (cash / filled) if filled else float("inf")
    return {"total_cash": cash, "avg_price": avg_price}

# === Main ===
def main():
    # 1) load all snapshots
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BROKER],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=1000
    )

    snapshots = []

    for msg in consumer:
        snap = msg.value
        snapshots.append(snap)

    consumer.close()
    best_config = {}
    best_result = {"avg_price": float("inf"), "total_cash": 0.0}
    tuning_results = []

    for lo, lu, tq in product(GRID["lambda_over"], GRID["lambda_under"], GRID["theta_queue"]):
        print(f"Running SOR with λ_over={lo}, λ_under={lu}, θ_queue={tq}")
        result = run_sor(snapshots, lo, lu, tq)
        tuning_results.append({
            "lambda_over": lo,
            "lambda_under": lu,
            "theta_queue": tq,
            "avg_fill_px": result["avg_price"],
            "total_cash": result["total_cash"]
        })
        if result["avg_price"] < best_result["avg_price"]:
            best_result = result
            best_config = {
                "lambda_over": lo,
                "lambda_under": lu,
                "theta_queue": tq
            }


    # 3) evaluate baselines
    best_ask = run_best_ask(snapshots)
    twap     = run_twap(snapshots)
    vwap     = run_vwap(snapshots)

    # 4) compute savings (bps)
    def savings_bps(baseline, optimized):
        return 10000 * (baseline["avg_price"] - optimized["avg_price"]) / baseline["avg_price"]

    report = {
        "best_parameters": best_config,
        "optimized": {
            "total_cash":  best_result["total_cash"],
            "avg_fill_px": best_result["avg_price"]
        },
        "baselines": {
            "best_ask": {
                "total_cash": best_ask["total_cash"],
                "avg_fill_px": best_ask["avg_price"]
            },
            "twap": {
                "total_cash": twap["total_cash"],
                "avg_fill_px": twap["avg_price"]
            },
            "vwap": {
                "total_cash": vwap["total_cash"],
                "avg_fill_px": vwap["avg_price"]
            }
        },
        "savings_vs_baselines_bps": {
            "best_ask": savings_bps(best_ask, best_result),
            "twap":     savings_bps(twap,     best_result),
            "vwap":     savings_bps(vwap,     best_result)
        }
    }


    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
