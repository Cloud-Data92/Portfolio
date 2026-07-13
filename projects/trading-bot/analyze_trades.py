#!/usr/bin/env python3
"""Analyze trade history from history.db"""
import sqlite3, json, statistics, time
from collections import defaultdict, Counter

db = sqlite3.connect('data/history.db')
db.row_factory = sqlite3.Row

rows = db.execute('SELECT * FROM trades ORDER BY timestamp').fetchall()
trades = [dict(r) for r in rows]

live = [t for t in trades if not t['dry_run']]
dry = [t for t in trades if t['dry_run']]
print("Total trades: %d, Live: %d, Dry: %d" % (len(trades), len(live), len(dry)))

# Parse extra JSON
for t in live:
    try:
        t['meta'] = json.loads(t['extra']) if t['extra'] else {}
    except:
        t['meta'] = {}

if not live:
    print("No live trades found")
    exit()

costs = [t['total_cost'] for t in live]
profits = [t['profit_pct'] for t in live]
investments = [t['investment'] for t in live]

print("\n=== LIVE TRADE STATS ===")
print("Count: %d" % len(live))
print("Total invested: $%.2f" % sum(investments))
print("Avg cost: $%.4f" % statistics.mean(costs))
print("Avg profit_pct: %.4f" % statistics.mean(profits))

wins = [t for t in live if t['profit_pct'] > 0]
losses = [t for t in live if t['profit_pct'] <= 0]
breakeven = [t for t in live if t['profit_pct'] == 0]
print("Winners: %d, Losers: %d, Breakeven: %d" % (len(wins), len(losses), len(breakeven)))
if wins:
    print("  Avg win pct: %.4f" % statistics.mean([t['profit_pct'] for t in wins]))
    print("  Max win pct: %.4f" % max(t['profit_pct'] for t in wins))
if losses:
    print("  Avg loss pct: %.4f" % statistics.mean([t['profit_pct'] for t in losses]))
    print("  Max loss pct: %.4f" % min(t['profit_pct'] for t in losses))

# Side analysis
sides = Counter(t['meta'].get('side', 'UNK') for t in live)
print("\n=== SIDE ANALYSIS ===")
print("Distribution:", dict(sides))
for side in ['UP', 'DOWN']:
    st = [t for t in live if t['meta'].get('side') == side]
    if st:
        w = sum(1 for t in st if t['profit_pct'] > 0)
        l = len(st) - w
        avg_pnl = statistics.mean([t['profit_pct'] for t in st])
        total_inv = sum(t['investment'] for t in st)
        avg_cost = statistics.mean([t['total_cost'] for t in st])
        print("  %s: %d trades, %dW/%dL (%.1f%%), avg P&L: %.4f, total inv: $%.2f, avg cost: $%.4f" % (
            side, len(st), w, l, 100*w/len(st), avg_pnl, total_inv, avg_cost))

# Approach analysis
approaches = Counter(t['meta'].get('approach', 'UNK') for t in live)
print("\n=== APPROACH ANALYSIS ===")
print("Distribution:", dict(approaches))
for app in approaches:
    at = [t for t in live if t['meta'].get('approach') == app]
    if at:
        w = sum(1 for t in at if t['profit_pct'] > 0)
        avg_pnl = statistics.mean([t['profit_pct'] for t in at])
        print("  %s: %d trades, %dW/%dL (%.1f%%), avg P&L: %.4f" % (
            app, len(at), w, len(at)-w, 100*w/len(at), avg_pnl))

# Conviction bins
print("\n=== CONVICTION ANALYSIS ===")
convictions = [t['meta'].get('conviction', t['meta'].get('confidence', 0)) for t in live]
if convictions:
    print("Min: %.4f, Max: %.4f, Avg: %.4f" % (min(convictions), max(convictions), statistics.mean(convictions)))
    bins = [(0, 0.05), (0.05, 0.10), (0.10, 0.20), (0.20, 0.50), (0.50, 1.0)]
    for lo, hi in bins:
        b = [t for t in live if lo <= (t['meta'].get('conviction', t['meta'].get('confidence', 0))) < hi]
        if b:
            w = sum(1 for t in b if t['profit_pct'] > 0)
            avg_p = statistics.mean([t['profit_pct'] for t in b])
            total_pnl_dollars = sum(t['profit_pct'] * t['investment'] for t in b)
            print("  [%.2f-%.2f): %d trades, %dW/%dL (%.1f%%), avg P&L: %.4f, dollar P&L: $%.2f" % (
                lo, hi, len(b), w, len(b)-w, 100*w/len(b), avg_p, total_pnl_dollars))

# Entry price analysis - where do we win vs lose
print("\n=== ENTRY PRICE ANALYSIS ===")
for side in ['UP', 'DOWN']:
    st = [t for t in live if t['meta'].get('side') == side]
    if not st:
        continue
    # up_price is the UP ask at entry, down_price is DOWN ask
    prices = [t['up_price'] if side == 'UP' else t['down_price'] for t in st]
    win_prices = [t['up_price'] if side == 'UP' else t['down_price'] for t in st if t['profit_pct'] > 0]
    loss_prices = [t['up_price'] if side == 'UP' else t['down_price'] for t in st if t['profit_pct'] <= 0]
    print("  %s entry prices:" % side)
    if prices: print("    All:   avg=%.4f, med=%.4f" % (statistics.mean(prices), statistics.median(prices)))
    if win_prices: print("    Wins:  avg=%.4f, med=%.4f" % (statistics.mean(win_prices), statistics.median(win_prices)))
    if loss_prices: print("    Losses: avg=%.4f, med=%.4f" % (statistics.mean(loss_prices), statistics.median(loss_prices)))

# Entry drag analysis (up_price + down_price - 1.0)
print("\n=== ENTRY DRAG ANALYSIS ===")
drags = [(t['up_price'] + t['down_price'] - 1.0, t) for t in live]
drag_vals = [d[0] for d in drags]
print("Drag: min=%.4f, max=%.4f, avg=%.4f, med=%.4f" % (
    min(drag_vals), max(drag_vals), statistics.mean(drag_vals), statistics.median(drag_vals)))
drag_bins = [(0, 0.02), (0.02, 0.04), (0.04, 0.06), (0.06, 0.10), (0.10, 1.0)]
for lo, hi in drag_bins:
    b = [(d, t) for d, t in drags if lo <= d < hi]
    if b:
        bt = [t for _, t in b]
        w = sum(1 for t in bt if t['profit_pct'] > 0)
        avg_p = statistics.mean([t['profit_pct'] for t in bt])
        print("  Drag [%.2f-%.2f): %d trades, %dW/%dL (%.1f%%), avg P&L: %.4f" % (
            lo, hi, len(bt), w, len(bt)-w, 100*w/len(bt), avg_p))

# Time analysis - recent vs old performance
print("\n=== PERFORMANCE OVER TIME (last 50 trades vs first 50) ===")
if len(live) >= 100:
    first = live[:50]
    last = live[-50:]
    f_w = sum(1 for t in first if t['profit_pct'] > 0)
    l_w = sum(1 for t in last if t['profit_pct'] > 0)
    f_pnl = statistics.mean([t['profit_pct'] for t in first])
    l_pnl = statistics.mean([t['profit_pct'] for t in last])
    print("  First 50: %dW/%dL (%.1f%%), avg P&L: %.4f" % (f_w, 50-f_w, 100*f_w/50, f_pnl))
    print("  Last 50:  %dW/%dL (%.1f%%), avg P&L: %.4f" % (l_w, 50-l_w, 100*l_w/50, l_pnl))

# Biggest wins and losses
print("\n=== TOP 10 WINS ===")
by_pnl = sorted(live, key=lambda t: t['profit_pct'], reverse=True)
for t in by_pnl[:10]:
    m = t['meta']
    print("  %.4f pct | $%.4f cost | side=%s conv=%.3f approach=%s drag=%.4f" % (
        t['profit_pct'], t['total_cost'], m.get('side','?'),
        m.get('conviction', m.get('confidence', 0)),
        m.get('approach','?'), t['up_price'] + t['down_price'] - 1.0))

print("\n=== TOP 10 LOSSES ===")
for t in by_pnl[-10:]:
    m = t['meta']
    print("  %.4f pct | $%.4f cost | side=%s conv=%.3f approach=%s drag=%.4f" % (
        t['profit_pct'], t['total_cost'], m.get('side','?'),
        m.get('conviction', m.get('confidence', 0)),
        m.get('approach','?'), t['up_price'] + t['down_price'] - 1.0))

# How many windows had UP vs DOWN bets and outcomes
print("\n=== MARKET-LEVEL ANALYSIS ===")
by_market = defaultdict(list)
for t in live:
    by_market[t['market']].append(t)
print("Unique windows: %d" % len(by_market))
print("Avg trades per window: %.1f" % (len(live) / max(len(by_market), 1)))

# Windows with both UP and DOWN trades (hedged)
hedged_windows = {k: v for k, v in by_market.items() if
    any(t['meta'].get('side') == 'UP' for t in v) and
    any(t['meta'].get('side') == 'DOWN' for t in v)}
print("Hedged windows (both sides): %d / %d" % (len(hedged_windows), len(by_market)))

# Per-window P&L
window_pnls = []
for mkt, ts in by_market.items():
    total_inv = sum(t['investment'] for t in ts)
    total_pnl = sum(t['profit_pct'] * t['investment'] for t in ts)
    window_pnls.append((mkt, total_pnl, total_inv, len(ts)))

window_pnls.sort(key=lambda x: x[1])
print("\n=== WORST 10 WINDOWS ===")
for mkt, pnl, inv, cnt in window_pnls[:10]:
    ts = by_market[mkt]
    sides = [t['meta'].get('side','?') for t in ts]
    approaches = set(t['meta'].get('approach','?') for t in ts)
    print("  $%.2f P&L | $%.2f inv | %d trades | sides=%s | approach=%s | %s" % (
        pnl, inv, cnt, Counter(sides), approaches, mkt[-20:]))

print("\n=== BEST 10 WINDOWS ===")
for mkt, pnl, inv, cnt in window_pnls[-10:]:
    ts = by_market[mkt]
    sides = [t['meta'].get('side','?') for t in ts]
    approaches = set(t['meta'].get('approach','?') for t in ts)
    print("  $%.2f P&L | $%.2f inv | %d trades | sides=%s | approach=%s | %s" % (
        pnl, inv, cnt, Counter(sides), approaches, mkt[-20:]))

# Dollar P&L summary
total_dollar_pnl = sum(t['profit_pct'] * t['investment'] for t in live)
print("\n=== DOLLAR P&L SUMMARY ===")
print("Total dollar P&L: $%.2f" % total_dollar_pnl)
print("Total invested (cumulative): $%.2f" % sum(t['investment'] for t in live))
print("ROI: %.2f%%" % (100 * total_dollar_pnl / max(sum(t['investment'] for t in live), 0.01)))
