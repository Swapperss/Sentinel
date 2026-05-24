import os
import json
import random
import sys
import argparse
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from src.ingestion.demo_event_factory import (
    build_unstructured_context,
    expand_fleet_for_distinct_rows,
    get_demo_fleet,
)
from utils.logger import get_logger

def resolve_generation_fleet(fleet, batch_size, distinct_rows):
    resolved_distinct_rows = max(0, int(distinct_rows or 0))
    resolved_batch_size = max(1, int(batch_size or len(fleet)))

    if resolved_distinct_rows > 0:
        target_size = resolved_distinct_rows
    else:
        target_size = resolved_batch_size

    if target_size > len(fleet):
        return expand_fleet_for_distinct_rows(fleet, target_size), target_size

    return [dict(vehicle) for vehicle in fleet[:target_size]], target_size


def generate_sentinel_files(batch_size=5, distinct_rows=None):
    """Generate realistic, dynamic test data files for each vehicle in the batch."""
    logger = get_logger("DATA_GENERATOR")
    fleet = get_demo_fleet()
    rng = random.Random()
    generation_fleet, effective_count = resolve_generation_fleet(fleet, batch_size, distinct_rows)
    
    # Output to data/landing/sentinel_landing/sentinel_s3_landing
    base_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "data", "landing", "sentinel_landing", "sentinel_s3_landing"
    )
    os.makedirs(base_dir, exist_ok=True)
    
    timestamp_suffix = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    logger.info(f"step=generate_sentinel_files base_dir={base_dir} timestamp={timestamp_suffix}")
    
    logs_dir = os.path.join(base_dir, "logs")
    notes_dir = os.path.join(base_dir, "notes")
    metadata_dir = os.path.join(base_dir, "metadata")
    os.makedirs(logs_dir, exist_ok=True)
    os.makedirs(notes_dir, exist_ok=True)
    os.makedirs(metadata_dir, exist_ok=True)

    logger.info(
        "step=generate_sentinel_files mode=%s fleet_size=%s effective_count=%s",
        "distinct" if distinct_rows else "batch",
        len(generation_fleet),
        effective_count,
    )

    for i in range(1, effective_count + 1):
        vehicle = generation_fleet[i - 1]
        context = build_unstructured_context(vehicle, timestamp_suffix, i, rng=rng)

        log_filename = f"sentinel_diag_{timestamp_suffix}_{i:02d}.log"
        log_filepath = os.path.join(logs_dir, log_filename)
        with open(log_filepath, "w", encoding="utf-8") as file_handle:
            file_handle.write(context["log_text"])
        logger.info(f"Generated log file: {log_filename} vin={vehicle['vin']}")

        note_filename = f"driver_notes_{timestamp_suffix}_{i:02d}.txt"
        note_filepath = os.path.join(notes_dir, note_filename)
        with open(note_filepath, "w", encoding="utf-8") as file_handle:
            file_handle.write(context["note_text"])
        logger.info(f"Generated note file: {note_filename} vin={vehicle['vin']}")

        metadata_filename = f"metadata_{timestamp_suffix}_{i:02d}.json"
        metadata_filepath = os.path.join(metadata_dir, metadata_filename)
        with open(metadata_filepath, "w", encoding="utf-8") as file_handle:
            json.dump(context["metadata"], file_handle, indent=4)
        logger.info(
            f"Generated metadata file: {metadata_filename} vehicle_id={context['metadata']['vehicle_id']} file_id={context['metadata']['file_id']}"
        )
    
    logger.info(f"step=generate_sentinel_files status=complete total_files={effective_count * 3} base_dir={base_dir}")

    return {
        "timestamp_suffix": timestamp_suffix,
        "batch_size": effective_count,
        "distinct_rows": distinct_rows,
        "base_dir": base_dir,
        "generated_files": effective_count * 3,
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate Sentinel unstructured logs, notes, and metadata")
    parser.add_argument("--batch-size", type=int, default=5, help="Number of vehicle bundles to generate")
    parser.add_argument(
        "--distinct-rows",
        type=int,
        default=None,
        help="Generate this many distinct VINs instead of reusing the demo fleet",
    )
    args = parser.parse_args()

    generate_sentinel_files(batch_size=args.batch_size, distinct_rows=args.distinct_rows)