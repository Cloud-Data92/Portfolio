#!/usr/bin/env python3
"""
PolyBot Trade Database Analysis
Find optimal probability/Kelly thresholds for standalone betting mode.
"""

import sqlite3
import json
import pandas as pd
import numpy as np
import re
from collections import defaultdict

pd.set_option('display.width', 140)
pd.set_option('display.max_columns', 20)
pd.set_option('display.float_format', '{:.4f}'.format)

DB_PATH = 'data/history.db'

def load_data():
    conn = sqlite3.connect(DB_PATH)
    
    # Load all trades
    df = pd.read_sql_query("SELECT * FROM trades", conn)
    conn.close()
    
    # Parse extra JSON
    def parse_extra(x):
        if pd.isna(x) or not x:
            return {}
        try:
            return json.loads(x)
        except:
            return {}
    
    df['extra_parsed'] = df['extra'].apply(parse_extra)
    df['action'] = df['extra_parsed'].apply(lambda x: x.get('action'))
    df['side'] = df['extra_parsed'].apply(lambda x: x.get('side'))
    df['result'] = df['extra_parsed'].apply(lambda x: x.get('result'))
    df['exit_pnl'] = df['extra_parsed'].apply(lambda x: x.get('pnl'))
    df['copy'] = df['extra_parsed'].apply(lambda x: x.get('copy', False))
    df['model_confidence'] = df['extra_parsed'].apply(lambda x: x.get('confidence'))
    df['model_kelly'] = df['extra_parsed'].apply(lambda x: x.get('kelly'))
    df['exit_reason'] = df['extra_parsed'].apply(lambda x: x.get('exit_reason'))
    
    return df


def build_trade_pairs(df):
    """Match each BUY with its outcome (SETTLE or SELL on same market+side)."""
    
    buys = df[df['action'] == 'BUY'].copy()
    outcomes = df[df['action'].isin(['SETTLE', 'SELL'])].copy()
    
    records = []
    
    for _, buy in buys.iterrows():
        market = buy['market']
        side = buy['side']
        
        # Find outcome for this market+side
        outcome = outcomes[(outcomes['market'] == market) & (outcomes['side'] == side)]
        
        if outcome.empty:
            continue
        
        # Take the first outcome (SELL or SETTLE)
        out = outcome.iloc[0]
        
        # Determine buy price based on side
        if side == 'UP':
            buy_price = buy['up_price']
        else:
            buy_price = buy['down_price']
        
        # Extract window start from market slug
        # e.g., btc-updown-5m-1771437900
        match = re.search(r'-(\d{10})$', market)
        window_start = int(match.group(1)) if match else None
        
        # Extract window duration
        dur_match = re.search(r'-(\d+)m-', market)
        window_minutes = int(dur_match.group(1)) if dur_match else 5
        
        seconds_into_window = buy['timestamp'] - window_start if window_start else None
        
        # Determine outcome
        out_action = out['action']
        pnl = out['exit_pnl'] if out['exit_pnl'] is not None else 0
        
        if out_action == 'SETTLE':
            win = out['result'] == 'WIN'
        elif out_action == 'SELL':
            win = (pnl > 0) if pnl is not None else False
        else:
            continue
        
        records.append({
            'market': market,
            'side': side,
            'buy_price': buy_price,
            'investment': buy['investment'],
            'shares': buy['shares'],
            'pnl': pnl,
            'win': win,
            'outcome_type': out_action,
            'copy': buy['copy'],
            'dry_run': buy['dry_run'],
            'window_start': window_start,
            'window_minutes': window_minutes,
            'buy_timestamp': buy['timestamp'],
            'seconds_into_window': seconds_into_window,
            'exit_reason': out.get('exit_reason'),
        })
    
    return pd.DataFrame(records)


def analysis_1_price_ranges(trades):
    """Group by buy price ranges and show W/L/PnL."""
    print("\n" + "="*100)
    print("1. PROFITABILITY BY BUY PRICE RANGE")
    print("="*100)
    
    # Create price buckets (0.05 increments)
    bins = np.arange(0.0, 1.05, 0.05)
    labels = [f"{b:.2f}-{b+0.05:.2f}" for b in bins[:-1]]
    trades['price_bucket'] = pd.cut(trades['buy_price'], bins=bins, labels=labels, right=False)
    
    grouped = trades.groupby('price_bucket', observed=True).agg(
        count=('win', 'size'),
        wins=('win', 'sum'),
        total_pnl=('pnl', 'sum'),
        avg_pnl=('pnl', 'mean'),
        avg_investment=('investment', 'mean'),
    ).reset_index()
    
    grouped['losses'] = grouped['count'] - grouped['wins']
    grouped['win_rate'] = grouped['wins'] / grouped['count']
    grouped['roi'] = grouped['total_pnl'] / (grouped['avg_investment'] * grouped['count'])
    
    print(f"\n{'Price Range':<14} {'Count':>6} {'Wins':>5} {'Loss':>5} {'WinRate':>8} {'Total PnL':>10} {'Avg PnL':>9} {'ROI':>8}")
    print("-" * 80)
    for _, r in grouped.iterrows():
        print(f"{r['price_bucket']:<14} {int(r['count']):>6} {int(r['wins']):>5} {int(r['losses']):>5} "
              f"{r['win_rate']:>7.1%} {r['total_pnl']:>10.2f} {r['avg_pnl']:>9.2f} {r['roi']:>7.1%}")
    
    totals = trades.agg({'win': ['size', 'sum'], 'pnl': ['sum', 'mean'], 'investment': 'mean'})
    total_count = len(trades)
    total_wins = int(trades['win'].sum())
    total_pnl = trades['pnl'].sum()
    print("-" * 80)
    print(f"{'TOTAL':<14} {total_count:>6} {total_wins:>5} {total_count - total_wins:>5} "
          f"{total_wins/total_count:>7.1%} {total_pnl:>10.2f} {total_pnl/total_count:>9.2f}")
    
    return trades  # return with price_bucket column added


def analysis_2_implied_vs_actual(trades):
    """Compare implied probability (buy price) vs actual win rate."""
    print("\n" + "="*100)
    print("2. IMPLIED PROBABILITY vs ACTUAL WIN RATE")
    print("="*100)
    print("   Implied P = buy_price (e.g., buy UP at $0.35 => implied P(up) = 35%)")
    
    grouped = trades.groupby('price_bucket', observed=True).agg(
        count=('win', 'size'),
        win_rate=('win', 'mean'),
        avg_buy_price=('buy_price', 'mean'),
    ).reset_index()
    
    grouped['implied_prob'] = grouped['avg_buy_price']
    
    print(f"\n{'Price Range':<14} {'Count':>6} {'Implied P':>10} {'Actual WR':>10} {'Calibration':>12}")
    print("-" * 65)
    for _, r in grouped.iterrows():
        cal = "OVERCONFIDENT" if r['win_rate'] < r['implied_prob'] else "UNDERPRICED" if r['win_rate'] > r['implied_prob'] else "FAIR"
        print(f"{r['price_bucket']:<14} {int(r['count']):>6} {r['implied_prob']:>9.1%} {r['win_rate']:>9.1%}   {cal}")


def analysis_3_edge(trades):
    """Calculate edge: actual_win_rate - implied_probability."""
    print("\n" + "="*100)
    print("3. EDGE ANALYSIS (actual_win_rate - implied_probability)")
    print("="*100)
    print("   Positive = we have edge over the market. Negative = market is smarter.")
    
    grouped = trades.groupby('price_bucket', observed=True).agg(
        count=('win', 'size'),
        win_rate=('win', 'mean'),
        avg_buy_price=('buy_price', 'mean'),
        total_pnl=('pnl', 'sum'),
    ).reset_index()
    
    grouped['implied_prob'] = grouped['avg_buy_price']
    grouped['edge'] = grouped['win_rate'] - grouped['implied_prob']
    
    print(f"\n{'Price Range':<14} {'Count':>6} {'Implied':>8} {'Actual':>8} {'Edge':>8} {'Total PnL':>10} {'Signal':>10}")
    print("-" * 75)
    for _, r in grouped.iterrows():
        if r['edge'] > 0.05:
            signal = "+++ EDGE"
        elif r['edge'] > 0:
            signal = "+ slight"
        elif r['edge'] > -0.05:
            signal = "- slight"
        else:
            signal = "--- LEAK"
        print(f"{r['price_bucket']:<14} {int(r['count']):>6} {r['implied_prob']:>7.1%} {r['win_rate']:>7.1%} "
              f"{r['edge']:>+7.1%} {r['total_pnl']:>10.2f} {signal:>10}")
    
    # Summary
    print("\n  KEY INSIGHT:")
    profitable = grouped[grouped['edge'] > 0]
    unprofitable = grouped[grouped['edge'] <= 0]
    if not profitable.empty:
        print(f"    Price ranges WITH edge: {', '.join(profitable['price_bucket'].astype(str).tolist())}")
        print(f"      Total PnL from these: ${profitable['total_pnl'].sum():.2f}")
    if not unprofitable.empty:
        print(f"    Price ranges WITHOUT edge: {', '.join(unprofitable['price_bucket'].astype(str).tolist())}")
        print(f"      Total PnL from these: ${unprofitable['total_pnl'].sum():.2f}")


def analysis_4_time_in_window(trades):
    """Analyze performance by entry timing within the window."""
    print("\n" + "="*100)
    print("4. TIME-IN-WINDOW ANALYSIS")
    print("="*100)
    
    valid = trades.dropna(subset=['seconds_into_window']).copy()
    valid = valid[valid['seconds_into_window'] >= 0]
    
    print(f"   Trades with valid timing data: {len(valid)}")
    print(f"   Seconds into window range: {valid['seconds_into_window'].min():.0f}s - {valid['seconds_into_window'].max():.0f}s")
    
    # Group by time ranges
    time_bins = [0, 60, 120, 180, 240, 300, 600, 900, float('inf')]
    time_labels = ['0-60s', '60-120s', '120-180s', '180-240s', '240-300s', '300-600s', '600-900s', '900s+']
    valid['time_bucket'] = pd.cut(valid['seconds_into_window'], bins=time_bins, labels=time_labels, right=False)
    
    grouped = valid.groupby('time_bucket', observed=True).agg(
        count=('win', 'size'),
        wins=('win', 'sum'),
        win_rate=('win', 'mean'),
        total_pnl=('pnl', 'sum'),
        avg_pnl=('pnl', 'mean'),
        avg_buy_price=('buy_price', 'mean'),
    ).reset_index()
    
    grouped['losses'] = grouped['count'] - grouped['wins']
    
    print(f"\n{'Time Bucket':<12} {'Count':>6} {'Wins':>5} {'Loss':>5} {'WinRate':>8} {'Total PnL':>10} {'Avg PnL':>9} {'Avg Price':>10}")
    print("-" * 80)
    for _, r in grouped.iterrows():
        print(f"{r['time_bucket']:<12} {int(r['count']):>6} {int(r['wins']):>5} {int(r['losses']):>5} "
              f"{r['win_rate']:>7.1%} {r['total_pnl']:>10.2f} {r['avg_pnl']:>9.2f} {r['avg_buy_price']:>9.3f}")
    
    # Also break down by window duration
    print(f"\n  By window duration:")
    for dur in sorted(valid['window_minutes'].unique()):
        sub = valid[valid['window_minutes'] == dur]
        print(f"    {dur}m windows: {len(sub)} trades, WR={sub['win'].mean():.1%}, PnL=${sub['pnl'].sum():.2f}")


def analysis_5_kelly(trades):
    """Kelly criterion simulation."""
    print("\n" + "="*100)
    print("5. KELLY CRITERION ANALYSIS")
    print("="*100)
    
    grouped = trades.groupby('price_bucket', observed=True).agg(
        count=('win', 'size'),
        win_rate=('win', 'mean'),
        avg_buy_price=('buy_price', 'mean'),
        total_pnl=('pnl', 'sum'),
    ).reset_index()
    
    print(f"\n{'Price Range':<14} {'Count':>6} {'WinRate':>8} {'Payout':>8} {'Kelly%':>8} {'Edge':>8} {'Action':>12}")
    print("-" * 78)
    
    for _, r in grouped.iterrows():
        p = r['win_rate']
        price = r['avg_buy_price']
        if price > 0 and price < 1:
            b = (1 - price) / price  # payout ratio (net profit per $1 risked on a win)
            kelly = (p * b - (1 - p)) / b
            edge = p - price
        else:
            b = 0
            kelly = 0
            edge = 0
        
        if kelly > 0.10:
            action = "STRONG BET"
        elif kelly > 0.05:
            action = "MODERATE"
        elif kelly > 0:
            action = "SMALL BET"
        else:
            action = "NO BET"
        
        print(f"{r['price_bucket']:<14} {int(r['count']):>6} {p:>7.1%} {b:>7.2f}x {kelly:>7.1%} {edge:>+7.1%} {action:>12}")
    
    # Assign Kelly per trade based on bucket stats
    bucket_stats = grouped.set_index('price_bucket')[['win_rate', 'avg_buy_price']].to_dict('index')
    
    kelly_values = []
    for _, t in trades.iterrows():
        bucket = t['price_bucket']
        if bucket in bucket_stats:
            stats = bucket_stats[bucket]
            p = stats['win_rate']
            price = stats['avg_buy_price']
            if price > 0 and price < 1:
                b = (1 - price) / price
                k = (p * b - (1 - p)) / b
            else:
                k = 0
        else:
            k = 0
        kelly_values.append(k)
    
    trades['kelly_fraction'] = kelly_values
    
    winners = trades[trades['win'] == True]
    losers = trades[trades['win'] == False]
    
    print(f"\n  Summary:")
    print(f"    Average Kelly for WINNING trades: {winners['kelly_fraction'].mean():.4f} ({winners['kelly_fraction'].median():.4f} median)")
    print(f"    Average Kelly for LOSING trades:  {losers['kelly_fraction'].mean():.4f} ({losers['kelly_fraction'].median():.4f} median)")
    print(f"    Trades where Kelly > 0 (positive edge): {(trades['kelly_fraction'] > 0).sum()} / {len(trades)}")
    print(f"    Trades where Kelly > 5%: {(trades['kelly_fraction'] > 0.05).sum()} / {len(trades)}")
    print(f"    Trades where Kelly > 10%: {(trades['kelly_fraction'] > 0.10).sum()} / {len(trades)}")
    
    return trades


def analysis_6_threshold_simulation(trades):
    """Simulate different edge/confidence thresholds to find optimal cutoff."""
    print("\n" + "="*100)
    print("6. THRESHOLD OPTIMIZATION SIMULATION")
    print("="*100)
    
    # Method 1: Filter by minimum edge (actual_win_rate - implied_prob per bucket)
    print("\n--- METHOD A: Minimum Edge Threshold ---")
    print("  (Only take trades where historical bucket edge > threshold)")
    print(f"\n{'Min Edge':>10} {'Trades':>7} {'Wins':>5} {'WinRate':>8} {'Total PnL':>10} {'Avg PnL':>9} {'ROI':>8}")
    print("-" * 65)
    
    best_pnl_a = -float('inf')
    best_thresh_a = 0
    
    for edge_thresh in np.arange(-0.20, 0.35, 0.025):
        subset = trades[trades['kelly_fraction'] > 0]  # need to recalculate based on edge
        # Calculate edge per trade
        bucket_stats = trades.groupby('price_bucket', observed=True).agg(
            win_rate=('win', 'mean'),
            avg_price=('buy_price', 'mean'),
        ).to_dict('index')
        
        edges = []
        for _, t in trades.iterrows():
            b = t['price_bucket']
            if b in bucket_stats:
                edges.append(bucket_stats[b]['win_rate'] - bucket_stats[b]['avg_price'])
            else:
                edges.append(0)
        trades['edge'] = edges
        
        subset = trades[trades['edge'] >= edge_thresh]
        if len(subset) == 0:
            continue
        
        total_pnl = subset['pnl'].sum()
        avg_pnl = subset['pnl'].mean()
        wins = subset['win'].sum()
        wr = subset['win'].mean()
        roi = total_pnl / subset['investment'].sum() if subset['investment'].sum() > 0 else 0
        
        marker = " <-- BEST" if total_pnl > best_pnl_a else ""
        if total_pnl > best_pnl_a:
            best_pnl_a = total_pnl
            best_thresh_a = edge_thresh
        
        print(f"{edge_thresh:>+9.1%} {len(subset):>7} {int(wins):>5} {wr:>7.1%} {total_pnl:>10.2f} {avg_pnl:>9.2f} {roi:>7.1%}{marker}")
    
    # Method 2: Filter by minimum buy price (only buy cheap contracts)
    print("\n--- METHOD B: Maximum Buy Price Threshold ---")
    print("  (Only buy contracts cheaper than threshold)")
    print(f"\n{'Max Price':>10} {'Trades':>7} {'Wins':>5} {'WinRate':>8} {'Total PnL':>10} {'Avg PnL':>9} {'ROI':>8}")
    print("-" * 65)
    
    best_pnl_b = -float('inf')
    best_thresh_b = 0
    
    for max_price in np.arange(0.20, 0.85, 0.05):
        subset = trades[trades['buy_price'] <= max_price]
        if len(subset) == 0:
            continue
        
        total_pnl = subset['pnl'].sum()
        avg_pnl = subset['pnl'].mean()
        wins = subset['win'].sum()
        wr = subset['win'].mean()
        roi = total_pnl / subset['investment'].sum() if subset['investment'].sum() > 0 else 0
        
        marker = " <-- BEST" if total_pnl > best_pnl_b else ""
        if total_pnl > best_pnl_b:
            best_pnl_b = total_pnl
            best_thresh_b = max_price
        
        print(f"  <= {max_price:.2f} {len(subset):>7} {int(wins):>5} {wr:>7.1%} {total_pnl:>10.2f} {avg_pnl:>9.2f} {roi:>7.1%}{marker}")
    
    # Method 3: Filter by Kelly fraction
    print("\n--- METHOD C: Minimum Kelly Fraction Threshold ---")
    print("  (Only take trades where Kelly fraction > threshold)")
    print(f"\n{'Min Kelly':>10} {'Trades':>7} {'Wins':>5} {'WinRate':>8} {'Total PnL':>10} {'Avg PnL':>9} {'ROI':>8}")
    print("-" * 65)
    
    best_pnl_c = -float('inf')
    best_thresh_c = 0
    
    for kelly_thresh in np.arange(-0.30, 0.35, 0.025):
        subset = trades[trades['kelly_fraction'] >= kelly_thresh]
        if len(subset) == 0:
            continue
        
        total_pnl = subset['pnl'].sum()
        avg_pnl = subset['pnl'].mean()
        wins = subset['win'].sum()
        wr = subset['win'].mean()
        roi = total_pnl / subset['investment'].sum() if subset['investment'].sum() > 0 else 0
        
        marker = " <-- BEST" if total_pnl > best_pnl_c else ""
        if total_pnl > best_pnl_c:
            best_pnl_c = total_pnl
            best_thresh_c = kelly_thresh
        
        print(f"{kelly_thresh:>+9.1%} {len(subset):>7} {int(wins):>5} {wr:>7.1%} {total_pnl:>10.2f} {avg_pnl:>9.2f} {roi:>7.1%}{marker}")
    
    # Method 4: Minimum buy price (floor) -- only take more extreme bets
    print("\n--- METHOD D: Minimum Buy Price Floor ---")
    print("  (Only buy contracts at least this cheap = more contrarian)")
    print(f"\n{'Min Price':>10} {'Trades':>7} {'Wins':>5} {'WinRate':>8} {'Total PnL':>10} {'Avg PnL':>9} {'ROI':>8}")
    print("-" * 65)
    
    for min_price in np.arange(0.05, 0.60, 0.05):
        subset = trades[(trades['buy_price'] >= min_price)]
        if len(subset) == 0:
            continue
        
        total_pnl = subset['pnl'].sum()
        avg_pnl = subset['pnl'].mean()
        wins = subset['win'].sum()
        wr = subset['win'].mean()
        roi = total_pnl / subset['investment'].sum() if subset['investment'].sum() > 0 else 0
        
        print(f"  >= {min_price:.2f} {len(subset):>7} {int(wins):>5} {wr:>7.1%} {total_pnl:>10.2f} {avg_pnl:>9.2f} {roi:>7.1%}")
    
    print(f"\n  OPTIMAL THRESHOLDS SUMMARY:")
    print(f"    Method A (Edge):     edge >= {best_thresh_a:+.1%} => PnL ${best_pnl_a:.2f}")
    print(f"    Method B (Max Price): price <= {best_thresh_b:.2f} => PnL ${best_pnl_b:.2f}")
    print(f"    Method C (Kelly):    kelly >= {best_thresh_c:+.1%} => PnL ${best_pnl_c:.2f}")


def analysis_copy_vs_standalone(trades):
    """Bonus: compare copy trades vs non-copy trades."""
    print("\n" + "="*100)
    print("BONUS: COPY TRADES vs NON-COPY (STANDALONE) TRADES")
    print("="*100)
    
    for label, sub in [("Copy trades", trades[trades['copy'] == True]), 
                        ("Non-copy trades", trades[trades['copy'] == False])]:
        if len(sub) == 0:
            print(f"\n  {label}: No trades")
            continue
        print(f"\n  {label}: {len(sub)} trades")
        print(f"    Win rate: {sub['win'].mean():.1%}")
        print(f"    Total PnL: ${sub['pnl'].sum():.2f}")
        print(f"    Avg PnL: ${sub['pnl'].mean():.2f}")
        print(f"    Avg buy price: {sub['buy_price'].mean():.3f}")
        print(f"    Total invested: ${sub['investment'].sum():.2f}")
        print(f"    ROI: {sub['pnl'].sum() / sub['investment'].sum():.1%}")

    # Also dry_run breakdown
    print(f"\n  Dry run breakdown:")
    for dr in [0, 1]:
        sub = trades[trades['dry_run'] == dr]
        label = "LIVE" if dr == 0 else "DRY RUN"
        if len(sub) == 0:
            continue
        print(f"    {label}: {len(sub)} trades, WR={sub['win'].mean():.1%}, PnL=${sub['pnl'].sum():.2f}, ROI={sub['pnl'].sum()/sub['investment'].sum():.1%}")


def analysis_settle_only(trades):
    """Re-run key metrics for SETTLE-only trades (held to expiry, cleaner signal)."""
    print("\n" + "="*100)
    print("SUPPLEMENT: SETTLE-ONLY TRADES (held to expiry)")
    print("="*100)
    
    settled = trades[trades['outcome_type'] == 'SETTLE']
    sold = trades[trades['outcome_type'] == 'SELL']
    
    print(f"  Settled (held to expiry): {len(settled)} trades, PnL=${settled['pnl'].sum():.2f}")
    print(f"  Sold early: {len(sold)} trades, PnL=${sold['pnl'].sum():.2f}")
    
    if len(settled) > 0:
        grouped = settled.groupby('price_bucket', observed=True).agg(
            count=('win', 'size'),
            wins=('win', 'sum'),
            win_rate=('win', 'mean'),
            avg_price=('buy_price', 'mean'),
            total_pnl=('pnl', 'sum'),
        ).reset_index()
        
        grouped['edge'] = grouped['win_rate'] - grouped['avg_price']
        
        print(f"\n{'Price Range':<14} {'Count':>6} {'Wins':>5} {'WinRate':>8} {'Implied':>8} {'Edge':>8} {'Total PnL':>10}")
        print("-" * 70)
        for _, r in grouped.iterrows():
            print(f"{r['price_bucket']:<14} {int(r['count']):>6} {int(r['wins']):>5} {r['win_rate']:>7.1%} "
                  f"{r['avg_price']:>7.1%} {r['edge']:>+7.1%} {r['total_pnl']:>10.2f}")


def main():
    print("="*100)
    print("POLYBOT TRADE DATABASE ANALYSIS")
    print("Finding optimal probability/Kelly thresholds for standalone betting")
    print("="*100)
    
    df = load_data()
    print(f"\nLoaded {len(df)} total records from database")
    print(f"  BUY: {(df['action'] == 'BUY').sum()}")
    print(f"  SELL: {(df['action'] == 'SELL').sum()}")
    print(f"  SETTLE: {(df['action'] == 'SETTLE').sum()}")
    print(f"  Prediction (no action): {df['action'].isna().sum()}")
    
    trades = build_trade_pairs(df)
    print(f"\nMatched {len(trades)} BUY-to-outcome trade pairs")
    print(f"  Outcome via SETTLE: {(trades['outcome_type'] == 'SETTLE').sum()}")
    print(f"  Outcome via SELL: {(trades['outcome_type'] == 'SELL').sum()}")
    print(f"  Copy trades: {trades['copy'].sum()}")
    print(f"  Live trades: {(trades['dry_run'] == 0).sum()}")
    print(f"  Dry run trades: {(trades['dry_run'] == 1).sum()}")
    
    trades = analysis_1_price_ranges(trades)
    analysis_2_implied_vs_actual(trades)
    analysis_3_edge(trades)
    analysis_4_time_in_window(trades)
    trades = analysis_5_kelly(trades)
    analysis_6_threshold_simulation(trades)
    analysis_copy_vs_standalone(trades)
    analysis_settle_only(trades)
    
    print("\n" + "="*100)
    print("FINAL RECOMMENDATIONS FOR STANDALONE MODE")
    print("="*100)
    
    # Calculate final recommendations
    bucket_stats = trades.groupby('price_bucket', observed=True).agg(
        count=('win', 'size'),
        win_rate=('win', 'mean'),
        avg_price=('buy_price', 'mean'),
        total_pnl=('pnl', 'sum'),
        kelly=('kelly_fraction', 'mean'),
    ).reset_index()
    bucket_stats['edge'] = bucket_stats['win_rate'] - bucket_stats['avg_price']
    
    profitable = bucket_stats[bucket_stats['edge'] > 0].sort_values('edge', ascending=False)
    
    print("\n  1. PROFITABLE PRICE RANGES (positive edge):")
    for _, r in profitable.iterrows():
        print(f"     {r['price_bucket']}: edge={r['edge']:+.1%}, kelly={r['kelly']:.1%}, PnL=${r['total_pnl']:.2f} ({int(r['count'])} trades)")
    
    unprofitable = bucket_stats[bucket_stats['edge'] <= 0].sort_values('edge')
    print("\n  2. AVOID THESE RANGES (negative edge):")
    for _, r in unprofitable.iterrows():
        print(f"     {r['price_bucket']}: edge={r['edge']:+.1%}, PnL=${r['total_pnl']:.2f} ({int(r['count'])} trades)")
    
    print("\n  3. SUGGESTED THRESHOLDS:")
    print(f"     - Minimum Kelly fraction: 0.0 (only bet when Kelly > 0)")
    print(f"     - Equivalently: only bet when observed_win_rate > buy_price")
    
    kelly_positive = trades[trades['kelly_fraction'] > 0]
    kelly_negative = trades[trades['kelly_fraction'] <= 0]
    print(f"     - Kelly > 0 trades: {len(kelly_positive)}, PnL=${kelly_positive['pnl'].sum():.2f}, WR={kelly_positive['win'].mean():.1%}")
    print(f"     - Kelly <= 0 trades: {len(kelly_negative)}, PnL=${kelly_negative['pnl'].sum():.2f}, WR={kelly_negative['win'].mean():.1%}")
    
    print(f"\n  4. POSITION SIZING:")
    print(f"     Use fractional Kelly (e.g., half-Kelly) for safety:")
    for _, r in profitable.iterrows():
        price = r['avg_price']
        if price > 0 and price < 1:
            b = (1 - price) / price
            k = (r['win_rate'] * b - (1 - r['win_rate'])) / b
            print(f"     {r['price_bucket']}: full Kelly={k:.1%}, half Kelly={k/2:.1%}")


if __name__ == '__main__':
    main()
