import os
import json

# Define the data structure for the 3 versions
data_payload = {
    "logs": {
        "sentinel_diag_v1_01.log": "2026-05-03 09:00:15 [SYS] Status: OK. Voltage: 14.1V. Temp: 88C. All systems nominal.",
        "sentinel_diag_v1_02.log": "2026-05-03 10:12:44 [SYS] WARN: Sensor_B lag detected. Latency: 450ms. Retrying...",
        "sentinel_diag_v1_03.log": "2026-05-03 11:05:01 [SYS] CRITICAL: 0x4A2 Voltage Drop. Battery 11.2V. Alt failure suspected.",
        "sentinel_diag_v1_04.log": "2026-05-03 06:30:12 [SYS] INFO: Ambient Temp: -2C. Glow plug pre-heat active.",
        "sentinel_diag_v1_05.log": "2026-05-03 14:22:10 [SYS] ERROR: S3_Upload_Failed. Connection reset by peer."
    },
    "notes": {
        "driver_notes_v2_01.txt": "The car felt sluggish climbing the hill today. Whistling sound past 3000 RPM.",
        "driver_notes_v2_02.txt": "Severe shudder in steering wheel at 120 km/h. Alignment might be off.",
        "driver_notes_v2_03.txt": "Oil changed. Engine sounds quieter and gear shifts are smoother.",
        "driver_notes_v2_04.txt": "AC taking forever to cool the cabin. It is only 22C outside.",
        "driver_notes_v2_05.txt": "Smelled burnt rubber after a long drive on the Autobahn. No lights on dash."
    },
    "metadata": {
        "metadata_v3_01.json": {"unit": "Alpha", "last_service": "2025-10-01", "tire_pressure": {"FL": 32, "FR": 32}},
        "metadata_v3_02.json": {"unit": "Alpha", "modifications": ["S3_Ingestion", "OpenAI_Sync"], "firmware": "v2.1.4"},
        "metadata_v3_03.json": {"unit": "Beta", "region": "North Rhine-Westphalia", "fuel": "Diesel"},
        "metadata_v3_04.json": {"unit": "Gamma", "alert_thresholds": {"max_temp": 110, "min_v": 11.5}},
        "metadata_v3_05.json": {"unit": "Alpha", "events": [{"id": "E101", "type": "Hard Braking"}]}
    }
}

def generate_sentinel_files():
    # Output relative to this script's directory (works on Windows and Linux)
    base_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sentinel_s3_landing")
    os.makedirs(base_dir, exist_ok=True)

    for category, files in data_payload.items():
        cat_path = os.path.join(base_dir, category)
        os.makedirs(cat_path, exist_ok=True)

        for filename, content in files.items():
            file_path = os.path.join(cat_path, filename)
            with open(file_path, "w", encoding="utf-8") as f:
                if filename.endswith(".json"):
                    json.dump(content, f, indent=4)
                else:
                    f.write(content)

    print(f"Success! 15 files created in: {base_dir}")

if __name__ == "__main__":
    generate_sentinel_files()