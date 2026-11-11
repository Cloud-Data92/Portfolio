"""
Generate the synthetic delivery-event dataset used by case studies 11-13.

Everything here is invented: store IDs, carrier names, volumes, rates and the
status vocabulary. The generator is seeded, so the CSVs (and every committed
query output) reproduce exactly.

The data is deliberately messy in the ways event feeds usually are:

  * many status events per delivery, one row per webhook received
  * duplicate webhooks (same status + event time, re-sent and re-ingested)
  * events ingested out of order and, for one carrier on one day, hours late
    (a simulated webhook outage followed by a batch backfill)
  * a delivery can be MISSED and later COMPLETED (redelivery)
  * CANCELED can arrive before or after a MISSED
  * some deliveries never reach a terminal status
  * event times are UTC; stores sit in four different time zones

Run:  python3 generate_events.py        (standard library only)
"""

import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEED = 20260628
AS_OF = datetime(2026, 6, 28, tzinfo=timezone.utc)   # "today" for every query
DAYS = 120
N_STORES = 40
OUT = Path(__file__).parent / "data"

TIME_ZONES = [
    ("America/New_York", -4), ("America/Chicago", -5),
    ("America/Denver", -6), ("America/Los_Angeles", -7),
]
# carrier_id, name, share of volume, typical webhook lag (seconds), miss rate
CARRIERS = [
    ("C1", "Carrier A", 0.40, 20, 0.035),
    ("C2", "Carrier B", 0.30, 45, 0.050),
    ("C3", "Carrier C", 0.20, 90, 0.080),
    ("C4", "Carrier D", 0.10, 300, 0.060),
]
OUTAGE_CARRIER = "C3"
OUTAGE_START = datetime(2026, 6, 12, 15, 0, tzinfo=timezone.utc)
OUTAGE_END = datetime(2026, 6, 13, 3, 0, tzinfo=timezone.utc)


def fmt(ts):
    return ts.strftime("%Y-%m-%d %H:%M:%S+00")


def main():
    rnd = random.Random(SEED)
    OUT.mkdir(exist_ok=True)

    stores = []
    for i in range(1, N_STORES + 1):
        tz, offset = TIME_ZONES[(i - 1) % 4]
        stores.append({
            "store_id": f"S{i:03d}",
            "market": f"Market {((i - 1) % 10) + 1:02d}",
            "time_zone": tz,
            "_offset": offset,
            "_volume": rnd.uniform(1.5, 5.0),          # deliveries per day
            "_miss_bias": rnd.choice([0.6, 0.8, 1.0, 1.0, 1.2, 1.6, 2.4]),
        })

    deliveries, events = [], []
    delivery_seq = event_seq = 0

    def emit(delivery_id, carrier, status, event_ts):
        """Record one webhook; sometimes the carrier re-sends it."""
        nonlocal event_seq
        lag = rnd.expovariate(1 / carrier[3])
        if rnd.random() < 0.02:                         # straggler
            lag += rnd.uniform(600, 5400)
        ingested = event_ts + timedelta(seconds=lag)
        if carrier[0] == OUTAGE_CARRIER and OUTAGE_START <= event_ts < OUTAGE_END:
            # webhooks queued during the outage land in one backfill batch
            ingested = OUTAGE_END + timedelta(hours=3, seconds=rnd.uniform(0, 1800))
        copies = 2 if rnd.random() < 0.04 else 1
        for n in range(copies):
            event_seq += 1
            events.append({
                "event_id": f"E{event_seq:07d}",
                "delivery_id": delivery_id,
                "status": status,
                "event_ts": fmt(event_ts),
                "ingested_ts": fmt(ingested + timedelta(seconds=n * rnd.uniform(5, 900))),
            })

    start = AS_OF - timedelta(days=DAYS)
    for day in range(DAYS):
        for store in stores:
            n = max(0, round(rnd.gauss(store["_volume"], 1.0)))
            for _ in range(n):
                delivery_seq += 1
                did = f"D{delivery_seq:06d}"
                carrier = rnd.choices(CARRIERS, weights=[c[2] for c in CARRIERS])[0]
                # created between 08:00 and 19:00 store-local time
                local_minute = rnd.uniform(8 * 60, 19 * 60)
                created = start + timedelta(days=day, minutes=local_minute - store["_offset"] * 60)
                promised = created + timedelta(hours=rnd.choice([2, 3, 4]))
                deliveries.append({
                    "delivery_id": did, "store_id": store["store_id"],
                    "carrier_id": carrier[0], "created_ts": fmt(created),
                    "promised_by_ts": fmt(promised),
                })

                t = created + timedelta(minutes=rnd.uniform(1, 15))
                emit(did, carrier, "ASSIGNED", t)

                age_hours = (AS_OF - created).total_seconds() / 3600
                if age_hours < 6 and rnd.random() < 0.6:
                    continue                            # still in flight
                if rnd.random() < 0.012:
                    continue                            # feed went silent
                if rnd.random() < 0.045:                # canceled before pickup
                    cancel_ts = t + timedelta(minutes=rnd.uniform(2, 40))
                    emit(did, carrier, "CANCELED", cancel_ts)
                    if rnd.random() < 0.10:             # driver still records a miss
                        emit(did, carrier, "MISSED", cancel_ts + timedelta(minutes=rnd.uniform(20, 180)))
                    continue

                t += timedelta(minutes=rnd.uniform(10, 50))
                emit(did, carrier, "ARRIVED_PICKUP", t)
                dwell = rnd.lognormvariate(2.2, 0.55) * (1.5 if store["_miss_bias"] > 1.5 else 1.0)
                if 11 * 60 <= local_minute < 14 * 60:   # midday rush at the pickup desk
                    dwell *= 1.4
                t += timedelta(minutes=dwell)
                if rnd.random() < 0.985:                # occasionally never sent
                    emit(did, carrier, "DEPARTED_PICKUP", t)
                t += timedelta(minutes=rnd.uniform(12, 70))
                emit(did, carrier, "ARRIVED_DROPOFF", t)
                t += timedelta(minutes=rnd.uniform(2, 12))

                if rnd.random() >= carrier[4] * store["_miss_bias"]:
                    emit(did, carrier, "COMPLETED", t)
                    continue

                emit(did, carrier, "MISSED", t)
                roll = rnd.random()
                if roll < 0.35:                         # redelivered later
                    emit(did, carrier, "COMPLETED", t + timedelta(hours=rnd.uniform(1, 26)))
                elif roll < 0.55:                       # customer cancels after the miss
                    emit(did, carrier, "CANCELED", t + timedelta(hours=rnd.uniform(0.2, 20)))
                elif roll < 0.60:                       # second miss
                    emit(did, carrier, "MISSED", t + timedelta(hours=rnd.uniform(2, 24)))

    rnd.shuffle(events)                                 # feeds do not arrive sorted

    def write(name, rows, cols):
        with open(OUT / name, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"  {name:<28} {len(rows):>7,} rows")

    print("Wrote synthetic delivery-event data:")
    write("stores.csv", stores, ["store_id", "market", "time_zone"])
    write("carriers.csv", [{"carrier_id": c[0], "carrier_name": c[1]} for c in CARRIERS],
          ["carrier_id", "carrier_name"])
    write("deliveries.csv", deliveries,
          ["delivery_id", "store_id", "carrier_id", "created_ts", "promised_by_ts"])
    write("delivery_status_events.csv", events,
          ["event_id", "delivery_id", "status", "event_ts", "ingested_ts"])


if __name__ == "__main__":
    main()
