"""
OmniRoute daily batch generator.

Generates the four batch data assets described in the BRD:
  - vehicle_registry.csv     (full snapshot, daily)
  - vehicle_assignment.csv   (incremental, daily)
  - maintenance_schedules.csv (yearly, Jan 1st)
  - fuel_transactions.csv    (daily)
and the static restricted_zones.json reference file.

Each file is written locally under ./data/<date>/ and then uploaded to
GCS at:
  gs://<bucket>/raw/<asset>/dt=<date>/<asset>.csv

State that must persist across days (which driver/rate is currently
assigned to which vehicle, running odometer readings) is kept in
state/vehicle_state.json so re-runs are continuity-safe.

Usage:
  python daily_batch_generator.py --date 2026-09-01
  python daily_batch_generator.py --date 2026-01-01 --force-maintenance
  python daily_batch_generator.py --no-upload   # local-only dry run
"""

import argparse
import csv
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.fleet_config import VEHICLES, DRIVER_IDS, REGIONS, RESTRICTED_ZONES  # noqa: E402
from gcs_uploader import upload_file  # noqa: E402

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
STATE_DIR = os.path.join(BASE_DIR, "..", "state")
STATE_FILE = os.path.join(STATE_DIR, "vehicle_state.json")

MAINTENANCE_TYPES = ["Engine Overhaul", "Tire Rotation", "Brake Service", "Oil Change", "Full Inspection"]

DRIVER_SWAP_PROB = 0.08          # per-vehicle chance of a driver swap on a given day
CONFLICT_RECORD_PROB = 0.05      # per-swap chance of injecting a duplicate/conflicting record
FUEL_OUTLIER_PROB = 0.10         # per-vehicle chance of a flagged fuel-efficiency day


def to_unix(dt: datetime) -> int:
    return int(dt.timestamp())


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {}


def save_state(state):
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


def bootstrap_state(state, run_dt: datetime):
    """Create an initial assignment for any vehicle with no prior state."""
    changed = False
    for v in VEHICLES:
        if v["vin"] not in state:
            state[v["vin"]] = {
                "driver_id": v["driver_id"],
                "daily_rate": round(random.uniform(400, 600), 2),
                "region": random.choice(REGIONS),
                "start_timestamp": to_unix(run_dt),
                "odometer_km": round(random.uniform(1000, 50000), 1),
            }
            changed = True
    return changed


def gen_vehicle_registry(out_path: str):
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["vin", "model", "mfg_year", "fuel_type"])
        for v in VEHICLES:
            writer.writerow([v["vin"], v["model"], v["mfg_year"], v["fuel_type"]])


def gen_vehicle_assignment(out_path: str, state: dict, run_dt: datetime):
    """
    Incremental file: only vehicles with a driver swap (or an injected
    conflict record) appear. Handles the "close old / open new" continuity
    logic and occasionally emits a duplicate same-day record with a
    different rate, to exercise the highest-rate conflict resolution rule.
    """
    rows = []
    swap_ts = to_unix(run_dt)

    for v in VEHICLES:
        vin = v["vin"]
        if random.random() >= DRIVER_SWAP_PROB:
            continue

        old = state[vin]

        # Close the previous assignment
        rows.append({
            "vin": vin,
            "driver_id": old["driver_id"],
            "start_timestamp": old["start_timestamp"],
            "end_timestamp": swap_ts,
            "daily_rate": old["daily_rate"],
            "region": old["region"],
        })

        # Pick a new driver (different from the current one)
        candidates = [d for d in DRIVER_IDS if d != old["driver_id"]]
        new_driver = random.choice(candidates)
        new_rate = round(random.uniform(400, 650), 2)
        new_region = random.choice(REGIONS)

        rows.append({
            "vin": vin,
            "driver_id": new_driver,
            "start_timestamp": swap_ts,
            "end_timestamp": "",
            "daily_rate": new_rate,
            "region": new_region,
        })

        # Occasionally inject a duplicate/conflicting record for the same
        # vin + start_timestamp with a different (lower) rate, to test
        # the ROW_NUMBER()-highest-rate conflict resolution logic downstream.
        if random.random() < CONFLICT_RECORD_PROB:
            rows.append({
                "vin": vin,
                "driver_id": random.choice(candidates),
                "start_timestamp": swap_ts,
                "end_timestamp": "",
                "daily_rate": round(new_rate - random.uniform(20, 80), 2),
                "region": new_region,
            })

        # Update state to the new assignment
        state[vin] = {
            "driver_id": new_driver,
            "daily_rate": new_rate,
            "region": new_region,
            "start_timestamp": swap_ts,
            "odometer_km": old["odometer_km"],
        }

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["vin", "driver_id", "start_timestamp", "end_timestamp", "daily_rate", "region"])
        for r in rows:
            writer.writerow([r["vin"], r["driver_id"], r["start_timestamp"], r["end_timestamp"], r["daily_rate"], r["region"]])

    return len(rows)


def gen_maintenance_schedules(out_path: str, run_dt: datetime):
    """Only meaningful on Jan 1st per the BRD; generates the year's schedule."""
    year = run_dt.year
    rows = []
    for v in VEHICLES:
        num_events = random.randint(1, 2)
        used_days = set()
        for _ in range(num_events):
            day_offset = random.randint(0, 364)
            while day_offset in used_days:
                day_offset = random.randint(0, 364)
            used_days.add(day_offset)
            service_date = datetime(year, 1, 1) + timedelta(days=day_offset)
            rows.append({
                "vin": v["vin"],
                "service_date": service_date.strftime("%Y-%m-%d"),
                "service_type": random.choice(MAINTENANCE_TYPES),
            })

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["vin", "service_date", "service_type"])
        for r in rows:
            writer.writerow([r["vin"], r["service_date"], r["service_type"]])

    return len(rows)


def gen_fuel_transactions(out_path: str, state: dict, run_dt: datetime):
    rows = []
    for v in VEHICLES:
        vin = v["vin"]
        baseline_kmpl = v["baseline_kmpl"]

        # Simulate distance covered today
        distance_km = round(random.uniform(150, 450), 1)

        # Most days near baseline; occasionally inject an outlier
        # (a vehicle running well below its baseline km/L)
        if random.random() < FUEL_OUTLIER_PROB:
            kmpl = baseline_kmpl * random.uniform(0.55, 0.85)  # flagged: >12% below baseline
        else:
            kmpl = baseline_kmpl * random.uniform(0.95, 1.08)

        fuel_liters = round(distance_km / kmpl, 2)
        old_odometer = state[vin]["odometer_km"]
        new_odometer = round(old_odometer + distance_km, 1)
        state[vin]["odometer_km"] = new_odometer

        rows.append({
            "transaction_id": f"TXN_{vin}_{int(time.time())}_{random.randint(1000,9999)}",
            "vin": vin,
            "fuel_liters": fuel_liters,
            "odometer_reading": new_odometer,
            "timestamp": run_dt.strftime("%Y-%m-%d %H:%M:%S"),
        })

    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["transaction_id", "vin", "fuel_liters", "odometer_reading", "timestamp"])
        for r in rows:
            writer.writerow([r["transaction_id"], r["vin"], r["fuel_liters"], r["odometer_reading"], r["timestamp"]])

    return len(rows)


def write_restricted_zones(out_path: str):
    with open(out_path, "w") as f:
        json.dump(RESTRICTED_ZONES, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Run date YYYY-MM-DD (defaults to today, UTC)")
    parser.add_argument("--bucket", help="GCS bucket name (defaults to GCS_BUCKET_NAME env var)")
    parser.add_argument("--no-upload", action="store_true", help="Skip GCS upload (local dry run)")
    parser.add_argument("--force-maintenance", action="store_true", help="Generate maintenance schedule even if not Jan 1st")
    parser.add_argument("--upload-static", action="store_true", help="Also (re-)upload restricted_zones.json")
    args = parser.parse_args()

    run_dt = datetime.strptime(args.date, "%Y-%m-%d") if args.date else datetime.now(timezone.utc)
    date_str = run_dt.strftime("%Y-%m-%d")

    out_dir = os.path.join(DATA_DIR, date_str)
    os.makedirs(out_dir, exist_ok=True)

    state = load_state()
    if bootstrap_state(state, run_dt):
        print(f"Bootstrapped initial assignment state for {len(VEHICLES)} vehicles.")

    uploads = []

    # 1. Vehicle registry — full snapshot, every day
    registry_path = os.path.join(out_dir, "vehicle_registry.csv")
    gen_vehicle_registry(registry_path)
    uploads.append((registry_path, f"raw/vehicle_registry/dt={date_str}/vehicle_registry.csv"))
    print(f"vehicle_registry.csv: {len(VEHICLES)} rows")

    # 2. Vehicle assignment — incremental
    assignment_path = os.path.join(out_dir, "vehicle_assignment.csv")
    n_assignment_rows = gen_vehicle_assignment(assignment_path, state, run_dt)
    if n_assignment_rows:
        uploads.append((assignment_path, f"raw/vehicle_assignment/dt={date_str}/vehicle_assignment.csv"))
    print(f"vehicle_assignment.csv: {n_assignment_rows} rows (incremental)")

    # 3. Maintenance schedules — Jan 1st only (or --force-maintenance)
    if run_dt.month == 1 and run_dt.day == 1 or args.force_maintenance:
        maintenance_path = os.path.join(out_dir, "maintenance_schedules.csv")
        n_maint_rows = gen_maintenance_schedules(maintenance_path, run_dt)
        uploads.append((maintenance_path, f"raw/maintenance_schedules/dt={date_str}/maintenance_schedules.csv"))
        print(f"maintenance_schedules.csv: {n_maint_rows} rows")
    else:
        print("maintenance_schedules.csv: skipped (not Jan 1st)")

    # 4. Fuel transactions — daily
    fuel_path = os.path.join(out_dir, "fuel_transactions.csv")
    n_fuel_rows = gen_fuel_transactions(fuel_path, state, run_dt)
    uploads.append((fuel_path, f"raw/fuel_transactions/dt={date_str}/fuel_transactions.csv"))
    print(f"fuel_transactions.csv: {n_fuel_rows} rows")

    # 5. Restricted zones — static, upload only on request
    if args.upload_static:
        zones_path = os.path.join(out_dir, "restricted_zones.json")
        write_restricted_zones(zones_path)
        uploads.append((zones_path, "raw/restricted_zones/restricted_zones.json"))
        print("restricted_zones.json: (re)uploaded")

    save_state(state)

    if args.no_upload:
        print("\n--no-upload set: skipping GCS upload. Files are in", out_dir)
        return

    print(f"\nUploading {len(uploads)} file(s) to GCS bucket "
          f"'{args.bucket or os.getenv('GCS_BUCKET_NAME')}'...")
    for local_path, blob_path in uploads:
        upload_file(local_path, blob_path, bucket_name=args.bucket)


if __name__ == "__main__":
    main()
