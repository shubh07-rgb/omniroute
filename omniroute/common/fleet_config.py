"""
Shared fleet configuration for OmniRoute.

Both the real-time telemetry producer and the daily batch generator
import from here so that vin / driver_id / model values line up
across the streaming and batch sides of the system.
"""

MODELS = [
    {"model": "Freightliner M2", "fuel_type": "Diesel", "baseline_kmpl": 5.0},
    {"model": "Volvo VNL", "fuel_type": "LNG", "baseline_kmpl": 4.2},
    {"model": "Isuzu N-Series", "fuel_type": "Diesel", "baseline_kmpl": 6.5},
]

# Fixed fleet — extend NUM_VEHICLES if you need a bigger simulated fleet.
NUM_VEHICLES = 20

VEHICLES = [
    {
        "vin": f"VIN_{i:03d}",
        "driver_id": f"DRV_{i:03d}",
        "model": MODELS[i % len(MODELS)]["model"],
        "fuel_type": MODELS[i % len(MODELS)]["fuel_type"],
        "baseline_kmpl": MODELS[i % len(MODELS)]["baseline_kmpl"],
        "mfg_year": 2018 + (i % 7),
    }
    for i in range(1, NUM_VEHICLES + 1)
]

DRIVER_IDS = [v["driver_id"] for v in VEHICLES]

REGIONS = ["North", "South", "East", "West", "Central"]

# From the BRD example — kept as the canonical restricted zone.
RESTRICTED_ZONES = [
    {
        "zone_name": "High_Risk_Pass_A",
        "min_lat": 34.05,
        "max_lat": 34.10,
        "min_long": -118.25,
        "max_long": -118.20,
    }
]
