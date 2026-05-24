import random
from datetime import datetime, timedelta, timezone


FLEET_BLUEPRINT = [
    {
        "vin": "WVWZZZ1KZ6W000101",
        "owner_name": "Arun Das",
        "manufacturer": "Volkswagen",
        "vehicle_model": "Golf GT",
        "manufacture_year": 2022,
        "status_profile": ["ACTIVE", "ACTIVE", "SERVICE_DUE", "HEALTH_WARN"],
        "temperature_band": (188.0, 207.0),
        "speed_band": (62, 118),
        "battery_band": (54, 88),
        "note_pool": [
            "Engine feels slightly flat during uphill acceleration and there is a whistle near 3000 RPM.",
            "Power delivery hesitates for a second after long idle periods in city traffic.",
            "Fuel economy dropped this week and the engine sounds strained under load.",
        ],
    },
    {
        "vin": "WBA8E91070K000202",
        "owner_name": "Mira Sen",
        "manufacturer": "BMW",
        "vehicle_model": "320d",
        "manufacture_year": 2023,
        "status_profile": ["ACTIVE", "HEALTH_WARN", "ACTIVE", "SERVICE_DUE"],
        "temperature_band": (192.0, 221.0),
        "speed_band": (75, 138),
        "battery_band": (42, 78),
        "note_pool": [
            "Steering wheel shudders above highway speed and the cabin smells warm after long runs.",
            "The cooling fan keeps spinning after shutdown and the engine bay feels hotter than normal.",
            "Dashboard flashed a warning briefly during a fast overtake, then cleared by itself.",
        ],
    },
    {
        "vin": "VF1RFB0067000303",
        "owner_name": "Karan Iyer",
        "manufacturer": "Renault",
        "vehicle_model": "Clio RS",
        "manufacture_year": 2021,
        "status_profile": ["ACTIVE", "ACTIVE", "ACTIVE", "INACTIVE"],
        "temperature_band": (181.0, 199.0),
        "speed_band": (48, 108),
        "battery_band": (60, 92),
        "note_pool": [
            "Cold start is rough for a minute and idle keeps hunting until the engine warms up.",
            "Cabin AC is weak and I hear a belt-like chirp while waiting at traffic lights.",
            "Braking is fine, but the engine response feels inconsistent during short trips.",
        ],
    },
    {
        "vin": "WAUZZZ8V2JA000404",
        "owner_name": "Sara Khan",
        "manufacturer": "Audi",
        "vehicle_model": "A3 TDI",
        "manufacture_year": 2020,
        "status_profile": ["SERVICE_DUE", "ACTIVE", "HEALTH_WARN", "ACTIVE"],
        "temperature_band": (195.0, 224.0),
        "speed_band": (58, 130),
        "battery_band": (38, 72),
        "note_pool": [
            "The engine warning light appeared after a long drive and throttle response became sluggish.",
            "There is a burnt rubber smell after motorway driving even though no alert stays on.",
            "Acceleration is smooth at low speed but weakens badly after sustained load.",
        ],
    },
]


def get_demo_fleet():
    return [dict(vehicle) for vehicle in FLEET_BLUEPRINT]


VIN_ALLOWED_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"


def build_synthetic_vin(unique_index):
    """Build a deterministic 17-character VIN-like key for synthetic rows."""
    value = max(0, int(unique_index))
    base = len(VIN_ALLOWED_CHARS)
    encoded = []
    while value > 0:
        value, remainder = divmod(value, base)
        encoded.append(VIN_ALLOWED_CHARS[remainder])
    suffix = "".join(reversed(encoded)) or "0"
    suffix = suffix.rjust(11, "0")[-11:]
    return f"SNTL00{suffix}"


def build_synthetic_vehicle(template, unique_index):
    synthetic = dict(template)
    synthetic["vin"] = build_synthetic_vin(unique_index)
    synthetic["owner_name"] = f"Demo Owner {unique_index:03d}"
    synthetic["status_profile"] = list(template["status_profile"])
    return synthetic


def expand_fleet_for_distinct_rows(fleet, distinct_rows):
    distinct_rows = max(0, int(distinct_rows))
    if distinct_rows <= 0:
        return [dict(vehicle) for vehicle in fleet]

    expanded = []
    for index in range(distinct_rows):
        template = fleet[index % len(fleet)]
        expanded.append(build_synthetic_vehicle(template, index + 1))
    return expanded


def build_vehicle_record(vehicle, event_index, rng=None):
    rng = rng or random.Random()
    status_profile = vehicle["status_profile"]
    status = status_profile[event_index % len(status_profile)]
    return {
        "vin": vehicle["vin"],
        "owner_name": vehicle["owner_name"],
        "manufacturer": vehicle["manufacturer"],
        "vehicle_model": vehicle["vehicle_model"],
        "manufacture_year": vehicle["manufacture_year"],
        "status": status,
        "service_score": rng.randint(72, 98) if status != "HEALTH_WARN" else rng.randint(38, 70),
    }


def build_telemetry_event(vehicle, event_index, rng=None, event_time=None):
    rng = rng or random.Random()
    event_time = event_time or datetime.now(timezone.utc)

    temperature_floor, temperature_ceiling = vehicle["temperature_band"]
    speed_floor, speed_ceiling = vehicle["speed_band"]
    battery_floor, battery_ceiling = vehicle["battery_band"]

    engine_temp_f = round(rng.uniform(temperature_floor, temperature_ceiling), 2)
    speed_kmh = rng.randint(speed_floor, speed_ceiling)
    battery_soc = rng.randint(battery_floor, battery_ceiling)

    return {
        "vin": vehicle["vin"],
        "event_time": event_time + timedelta(seconds=event_index * 5),
        "engine_temp_f": engine_temp_f,
        "speed_kmh": speed_kmh,
        "battery_soc": battery_soc,
    }


def build_unstructured_context(vehicle, batch_suffix, item_index, rng=None):
    rng = rng or random.Random()
    note_text = rng.choice(vehicle["note_pool"])
    log_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    alert_code = f"0x{rng.randint(0x4A0, 0x4FF):X}"
    vehicle_status = vehicle["status_profile"][item_index % len(vehicle["status_profile"])]
    log_lines = [
        f"{log_timestamp} [SYS] VIN={vehicle['vin']} Status={vehicle_status} Model={vehicle['vehicle_model']} Year={vehicle['manufacture_year']}",
        f"{log_timestamp} [SYS] WARN: Alert={alert_code} Cooling variance detected during sustained acceleration.",
        f"{log_timestamp} [SYS] INFO: Owner={vehicle['owner_name']} Manufacturer={vehicle['manufacturer']} NoteTheme={note_text}",
        f"{log_timestamp} [SYS] DEBUG: CorrelationSeed=DEMO-CORR-{batch_suffix}-{item_index:02d} FileStub=DEMO-{batch_suffix}-{item_index:02d}-{vehicle['vin'][-6:]}",
    ]
    file_stub = f"DEMO-{batch_suffix}-{item_index:02d}-{vehicle['vin'][-6:]}"
    metadata = {
        "vehicle_id": vehicle["vin"],
        "vin": vehicle["vin"],
        "owner_name": vehicle["owner_name"],
        "manufacturer": vehicle["manufacturer"],
        "vehicle_model": vehicle["vehicle_model"],
        "manufacture_year": vehicle["manufacture_year"],
        "vehicle_status": vehicle_status,
        "file_id": file_stub,
        "correlation_id": f"DEMO-CORR-{batch_suffix}-{item_index:02d}",
        "generated_at": datetime.utcnow().isoformat(),
        "note_theme": note_text,
        "batch_suffix": batch_suffix,
        "event_index": item_index,
    }
    return {
        "note_text": note_text,
        "log_text": "\n".join(log_lines),
        "metadata": metadata,
    }