# OmniRoute — Data Generation Layer

This repo is the data-generation tier for the OmniRoute Smart Logistics
Engine (see BRD). It has two independent pieces:

- **`producer/`** — real-time telemetry generator, pushes GPS/speed events
  to Kafka topic `vehicle-telemetry` (matches the BRD's Telemetry Stream schema).
- **`batch/`** — daily batch generator, produces `vehicle_registry.csv`,
  `vehicle_assignment.csv`, `maintenance_schedules.csv`, `fuel_transactions.csv`
  (and the static `restricted_zones.json`) and uploads each to GCS.

Both share vehicle/driver definitions from `common/fleet_config.py` so
VINs line up across the streaming and batch sides.

## 1. VM setup

```bash
bash setup.sh   # installs docker, git, and other base tooling
```

## 2. Real-time telemetry (Kafka)

```bash
docker compose up -d kafka producer spark
docker compose logs -f producer
```

To run the producer without Docker:

```bash
cd omniroute
pip install -r producer/requirements.txt
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export KAFKA_TOPIC=vehicle-telemetry
python producer/telemetry_producer.py
```

## 3. Daily batch data → GCS

The VM's attached service account is used automatically — no key file
needed, just make sure it has write access (`roles/storage.objectAdmin`
or equivalent) on the target bucket.

```bash
cd omniroute
pip install -r batch/requirements.txt
export GCS_BUCKET_NAME=your-bucket-name

# Generate + upload for today
python batch/daily_batch_generator.py

# Generate for a specific date (useful for backfills/testing)
python batch/daily_batch_generator.py --date 2026-09-01

# Local dry run, no upload
python batch/daily_batch_generator.py --no-upload

# First run only — also push the static restricted_zones.json
python batch/daily_batch_generator.py --upload-static
```

Output layout in GCS:

```
gs://<bucket>/raw/vehicle_registry/dt=<date>/vehicle_registry.csv
gs://<bucket>/raw/vehicle_assignment/dt=<date>/vehicle_assignment.csv
gs://<bucket>/raw/maintenance_schedules/dt=<date>/maintenance_schedules.csv   (Jan 1st only)
gs://<bucket>/raw/fuel_transactions/dt=<date>/fuel_transactions.csv
gs://<bucket>/raw/restricted_zones/restricted_zones.json                     (static)
```

### Scheduling (cron)

Per the BRD, batch sources land at 00:00 UTC. Add to the VM's crontab
(`crontab -e`):

```cron
# Daily batch generation, 00:00 UTC
0 0 * * * cd /path/to/omniroute && GCS_BUCKET_NAME=your-bucket-name /usr/bin/python3 batch/daily_batch_generator.py >> /var/log/omniroute-batch.log 2>&1
```

State that must persist across runs (current driver/rate per vehicle,
running odometer) lives in `state/vehicle_state.json` — keep this file
on persistent disk; don't wipe it between runs or continuity breaks.

## 4. Repo layout

```
omniroute/
├── common/
│   └── fleet_config.py        # shared VIN/driver/model config
├── producer/
│   ├── telemetry_producer.py  # real-time Kafka producer
│   ├── Dockerfile
│   └── requirements.txt
├── batch/
│   ├── daily_batch_generator.py
│   ├── gcs_uploader.py
│   └── requirements.txt
├── spark/
│   ├── kafka_stream.py        # streaming consumer (console sink for now)
│   └── Dockerfile
├── data/                      # generated CSVs land here locally (gitignored)
├── state/                     # continuity state (gitignored)
├── docker-compose.yml
├── setup.sh
└── .env.example
```
