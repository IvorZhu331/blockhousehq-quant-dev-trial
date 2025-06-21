import random

# Copy Venue, compute_cost, and allocate from backtest.py
class Venue:
    def __init__(self, ask, ask_size, fee=0.0, rebate=0.0):
        self.ask = ask
        self.ask_size = ask_size
        self.fee = fee
        self.rebate = rebate

def compute_cost(split, venues, order_size, lambda_over, lambda_under, theta):
    executed = 0
    cash_spent = 0.0
    for i in range(len(venues)):
        exe = min(split[i], venues[i].ask_size)
        executed += exe
        cash_spent += exe * (venues[i].ask + venues[i].fee)
        maker_rebate = max(split[i] - exe, 0) * venues[i].rebate
        cash_spent -= maker_rebate

    underfill = max(order_size - executed, 0)
    overfill = max(executed - order_size, 0)
    risk_pen = theta * (underfill + overfill)
    cost_pen = lambda_under * underfill + lambda_over * overfill
    return cash_spent + risk_pen + cost_pen

def allocate(order_size, venues, lambda_over, lambda_under, theta):
    STEP = 1
    splits = [[]]
    for v in range(len(venues)):
        new_splits = []
        for alloc in splits:
            used = sum(alloc)
            max_v = min(order_size - used, venues[v].ask_size)
            for q in range(0, max_v + 1, STEP):
                new_splits.append(alloc + [q])
        splits = new_splits

    best_cost = float('inf')
    best_split = []
    for alloc in splits:
        if sum(alloc) != order_size:
            continue
        cost = compute_cost(alloc, venues, order_size, lambda_over, lambda_under, theta)
        if cost < best_cost:
            best_cost = cost
            best_split = alloc
    return best_split, best_cost

# Simulate a few venue snapshots
snapshots = [
    [
        Venue(ask=100.0, ask_size=200),
        Venue(ask=100.5, ask_size=300),
        Venue(ask=101.0, ask_size=500),
    ],
    [
        Venue(ask=99.8, ask_size=100),
        Venue(ask=100.2, ask_size=400),
        Venue(ask=100.7, ask_size=600),
    ],
]

TOTAL_ORDER = 500
LAMBDA_OVER = 0.1
LAMBDA_UNDER = 0.2
THETA_QUEUE = 0.05

filled = 0
cash_spent = 0.0

for i, venues in enumerate(snapshots):
    shares_remaining = TOTAL_ORDER - filled
    if shares_remaining <= 0:
        break
    split, cost = allocate(shares_remaining, venues, LAMBDA_OVER, LAMBDA_UNDER, THETA_QUEUE)
    print(f"Snapshot {i+1}: split={split}, cost={cost:.2f}")
    for j, shares in enumerate(split):
        fill = min(shares, venues[j].ask_size)
        filled += fill
        cash_spent += fill * venues[j].ask
        print(f"  Venue {j} filled {fill} at {venues[j].ask}")
    print(f"  Total filled: {filled}, Cash spent: {cash_spent:.2f}\n")

avg_price = cash_spent / filled if filled else 0
print(f"\nTest complete. Total filled: {filled}, Avg price: {avg_price:.4f}, Cash: {cash_spent:.2f}") 