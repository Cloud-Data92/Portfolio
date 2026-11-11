"""
Generate the synthetic dataset for the Strategy & Operations analytics project.

Everything is invented: organization names, people, plans, objectives and
metric values. The generator is seeded, so the CSVs and every committed query
output reproduce exactly.

Defects are seeded on purpose, because the SQL is meant to cope with them:

  * workforce events are recorded late and occasionally twice
  * a few terminations are dated before the employee's hire date
  * a few events point at an org that does not exist
  * some key results stop receiving check-ins (stale)
  * one objective's key-result weights do not sum to 1
  * daily KPI series contain a handful of genuine anomalies and one missing day

Run:  python3 generate_data.py        (standard library only)
"""

import csv
import math
import random
from datetime import date, datetime, timedelta
from pathlib import Path

SEED = 20261031
AS_OF = date(2026, 10, 31)            # "today" for every query
START = date(2025, 1, 1)
OUT = Path(__file__).parent / "data"

ORGS = [  # org_id, org_name, function, starting headcount, monthly attrition, growth bias
    ("ORG01", "Capacity Planning",      "Planning",   60, 0.010, 1.6),
    ("ORG02", "Demand Forecasting",     "Planning",   35, 0.008, 0.9),
    ("ORG03", "Site Operations East",   "Operations", 140, 0.016, 2.6),
    ("ORG04", "Site Operations West",   "Operations", 120, 0.022, 2.0),
    ("ORG05", "Logistics & Supply",     "Operations", 90, 0.014, 1.2),
    ("ORG06", "Network Engineering",    "Engineering", 110, 0.009, 2.2),
    ("ORG07", "Reliability Engineering", "Engineering", 75, 0.011, 1.4),
    ("ORG08", "Program Management",     "Engineering", 40, 0.012, 0.5),
]
KPIS = [  # metric_name, base level, daily noise (sd), weekly amplitude
    ("tickets_closed", 420, 22, 60),
    ("on_time_delivery_pct", 94.0, 0.8, 0.0),
    ("avg_cycle_time_hours", 31.0, 1.6, 2.0),
]
ANOMALIES = [  # org, metric, date, multiplier
    ("ORG03", "tickets_closed", date(2026, 9, 14), 0.45),
    ("ORG04", "on_time_delivery_pct", date(2026, 10, 6), 0.90),
    ("ORG05", "avg_cycle_time_hours", date(2026, 8, 19), 1.55),
    ("ORG06", "tickets_closed", date(2026, 10, 22), 1.80),
    ("ORG04", "avg_cycle_time_hours", date(2026, 10, 27), 1.40),
]


def month_starts(first, last):
    d = first
    while d <= last:
        yield d
        d = date(d.year + (d.month == 12), d.month % 12 + 1, 1)


def month_end(d):
    return date(d.year + (d.month == 12), d.month % 12 + 1, 1) - timedelta(days=1)


def main():
    rnd = random.Random(SEED)
    OUT.mkdir(exist_ok=True)
    events, plan = [], []
    emp_seq = ev_seq = 0

    def record(emp, kind, org, eff, dup_ok=True):
        nonlocal ev_seq
        lag = rnd.choice([0, 0, 0, 1, 1, 2, 3, 5]) if rnd.random() > 0.06 else rnd.randint(12, 45)
        copies = 2 if dup_ok and rnd.random() < 0.015 else 1
        for n in range(copies):
            ev_seq += 1
            rec = datetime.combine(eff, datetime.min.time()) + timedelta(
                days=lag + n, hours=rnd.randint(8, 17), minutes=rnd.randint(0, 59))
            events.append({"event_id": f"WE{ev_seq:06d}", "employee_id": emp, "event_type": kind,
                           "org_id": org, "effective_date": eff.isoformat(),
                           "recorded_ts": rec.strftime("%Y-%m-%d %H:%M:%S+00")})

    active = {o[0]: [] for o in ORGS}
    for org_id, _, _, start_hc, _, _ in ORGS:           # opening population
        for _ in range(start_hc):
            emp_seq += 1
            emp = f"EMP{emp_seq:05d}"
            hired = START - timedelta(days=rnd.randint(30, 1500))
            record(emp, "HIRE", org_id, hired, dup_ok=False)
            active[org_id].append((emp, hired))

    headcount_by_month = {}
    for m in month_starts(START, AS_OF):
        for org_id, _, _, _, attr, growth in ORGS:
            # Site Operations West loses people faster from mid-2026 onward
            a = attr * (1.9 if org_id == "ORG04" and m >= date(2026, 6, 1) else 1.0)
            for emp, hired in list(active[org_id]):
                if rnd.random() < a:
                    eff = m + timedelta(days=rnd.randint(0, 27))
                    record(emp, "TERMINATION", org_id, eff)
                    active[org_id].remove((emp, hired))
                elif rnd.random() < 0.004:
                    dest = rnd.choice([o[0] for o in ORGS if o[0] != org_id])
                    eff = m + timedelta(days=rnd.randint(0, 27))
                    record(emp, "TRANSFER", dest, eff)
                    active[org_id].remove((emp, hired))
                    active[dest].append((emp, hired))
            hires = max(0, round(rnd.gauss(growth + len(active[org_id]) * a, 1.2)))
            if org_id == "ORG04" and m >= date(2026, 6, 1):
                hires = max(0, hires - 2)                # backfills not keeping up
            for _ in range(hires):
                emp_seq += 1
                emp = f"EMP{emp_seq:05d}"
                eff = m + timedelta(days=rnd.randint(0, 27))
                record(emp, "HIRE", org_id, eff)
                active[org_id].append((emp, eff))
            headcount_by_month[(org_id, m)] = len(active[org_id])

    # seeded integrity defects
    hires = [e for e in events if e["event_type"] == "HIRE" and e["effective_date"] >= "2025-03-01"]
    for h in rnd.sample(hires, 4):                      # termination dated before hire
        ev_seq += 1
        events.append({"event_id": f"WE{ev_seq:06d}", "employee_id": h["employee_id"],
                       "event_type": "TERMINATION", "org_id": h["org_id"],
                       "effective_date": (date.fromisoformat(h["effective_date"]) - timedelta(days=20)).isoformat(),
                       "recorded_ts": h["recorded_ts"]})
    for h in rnd.sample(hires, 3):                      # unknown org
        h["org_id"] = "ORG99"
    rnd.shuffle(events)

    # plan: set each January as starting headcount plus a quarterly growth step
    for org_id, _, _, start_hc, _, growth in ORGS:
        for year in (2025, 2026, 2027):
            # each year's plan starts from January actuals; 2027 is not yet
            # re-based, so it simply continues the 2026 plan line
            base = headcount_by_month.get((org_id, date(year, 1, 1)),
                                          headcount_by_month[(org_id, date(2026, 1, 1))] + growth * 13.2)
            for q in range(1, 5):
                if year == 2027 and q > 2:
                    continue
                q_end = month_end(date(year, q * 3, 1))
                plan.append({"org_id": org_id, "quarter_end_date": q_end.isoformat(),
                             "planned_headcount": round(base + growth * 3.3 * q)})

    # OKRs for the current quarter (2026 Q4)
    q_start, q_end = date(2026, 10, 1), date(2026, 12, 31)
    elapsed = (AS_OF - q_start).days / (q_end - q_start).days
    krs, checkins = [], []
    kr_templates = [
        ("Reduce average cycle time (hours)", "DECREASE", 34, 28),
        ("Raise on-time delivery (%)", "INCREASE", 92, 96),
        ("Close open headcount requisitions", "INCREASE", 0, 24),
        ("Automate manual weekly reports", "INCREASE", 0, 10),
        ("Cut priority-1 incident count", "DECREASE", 40, 25),
        ("Complete site readiness audits", "INCREASE", 0, 12),
    ]
    kr_seq = 0
    for i, (org_id, *_rest) in enumerate(ORGS):
        for o in range(2):
            obj = f"OBJ-{org_id[-2:]}-{o + 1}"
            picks = rnd.sample(kr_templates, 3 if o == 0 else 2)
            weights = [0.5, 0.3, 0.2] if o == 0 else [0.6, 0.4]
            if obj == "OBJ-05-2":
                weights = [0.6, 0.6]                     # defect: weights sum to 1.2
            for (name, direction, start, target), w in zip(picks, weights):
                kr_seq += 1
                kr = f"KR{kr_seq:03d}"
                krs.append({"kr_id": kr, "objective_id": obj, "org_id": org_id,
                            "quarter_start_date": q_start.isoformat(),
                            "quarter_end_date": q_end.isoformat(), "kr_name": name,
                            "direction": direction, "start_value": start,
                            "target_value": target, "weight": w})
                pace = rnd.choice([1.3, 1.15, 1.05, 1.0, 1.0, 0.95, 0.85, 0.7, 0.5, 0.25])
                stale = rnd.random() < 0.15
                d = q_start + timedelta(days=rnd.randint(2, 6))
                while d <= AS_OF:
                    if stale and d > q_start + timedelta(days=12):
                        break
                    frac = min(1.0, (d - q_start).days / (q_end - q_start).days * pace
                               * rnd.uniform(0.9, 1.1))
                    val = start + (target - start) * frac
                    checkins.append({"kr_id": kr, "checkin_date": d.isoformat(),
                                     "current_value": round(val, 1)})
                    d += timedelta(days=7)

    # daily KPI series
    kpi = []
    d = date(2026, 4, 1)
    while d <= AS_OF:
        for org_id, _, function, *_r in ORGS:
            if function == "Planning":
                continue
            for name, base, sd, weekly in KPIS:
                if (org_id, name, d) == ("ORG07", "tickets_closed", date(2026, 10, 13)):
                    continue                              # a missing day
                level = base * (0.85 + 0.05 * int(org_id[-1]))
                if name == "on_time_delivery_pct":
                    level = base - 0.4 * (int(org_id[-1]) - 3)
                v = level + rnd.gauss(0, sd) + weekly * math.sin(2 * math.pi * d.weekday() / 7)
                for a_org, a_name, a_date, mult in ANOMALIES:
                    if (org_id, name, d) == (a_org, a_name, a_date):
                        v *= mult
                kpi.append({"org_id": org_id, "metric_name": name, "metric_date": d.isoformat(),
                            "metric_value": round(v, 2)})
        d += timedelta(days=1)

    def write(name, rows, cols):
        with open(OUT / name, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows)
        print(f"  {name:<24} {len(rows):>6,} rows")

    print("Wrote synthetic strategy & operations data:")
    write("orgs.csv", [{"org_id": o[0], "org_name": o[1], "function_name": o[2]} for o in ORGS],
          ["org_id", "org_name", "function_name"])
    write("workforce_events.csv", events,
          ["event_id", "employee_id", "event_type", "org_id", "effective_date", "recorded_ts"])
    write("headcount_plan.csv", plan, ["org_id", "quarter_end_date", "planned_headcount"])
    write("okr_key_results.csv", krs, list(krs[0].keys()))
    write("okr_checkins.csv", checkins, ["kr_id", "checkin_date", "current_value"])
    write("kpi_daily.csv", kpi, ["org_id", "metric_name", "metric_date", "metric_value"])


if __name__ == "__main__":
    main()
