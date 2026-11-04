"""
Deterministic Synthetic Telemetry Generator for LOFAR 2.0 Station Health.

Simulates high-velocity, multi-station radio telescope telemetry with realistic
distributions and intentional, cross-column data quality anomalies to stress-test
Netra Validator's streaming Polars and DuckDB out-of-core engines.
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import numpy as np
import polars as pl
import pyarrow.parquet as pq
from benchmarks.generators.base import BaseDatasetGenerator

# Lookup Tables for the LOFAR parameters

# Core (CS), Remote (RS), and International (DE, FR, UK, SE, PL, IE) stations
CORE_STATIONS = [f"CS{i:03d}" for i in range(1, 25)]
REMOTE_STATIONS = [f"RS{i:03d}" for i in range(101, 125)]
INTL_STATIONS = [
    "DE601",
    "DE602",
    "DE603",
    "DE604",
    "DE605",
    "FR606",
    "UK608",
    "SE607",
    "PL610",
    "PL611",
    "PL612",
    "IE613",
]
VALID_STATIONS = np.array(CORE_STATIONS + REMOTE_STATIONS + INTL_STATIONS)

HARDWARE_STATES = np.array(["OK", "DOWN", "SHORT", "FLAT", "SPURIOUS", "OSCILLATING"])
HARDWARE_WEIGHTS = [0.950, 0.010, 0.010, 0.010, 0.010, 0.010]

GPS_STATES = np.array(["LOCKED", "DRIFTING", "LOST_SYNC"])
GPS_WEIGHTS = [0.980, 0.015, 0.005]


def generate_chunk(start_index: int, number_of_rows: int, seed: int) -> pl.DataFrame:
    """
    Generates a single vectorized telemetry batch with engineered contract anomalies.
    """

    rng = np.random.default_rng(seed)

    # 1. Base Temporal Vector (10 Hz polling rate per station)
    base_epoch = np.datetime64("2026-03-01T00:00:00", "ms")
    timestamp_offsets = (start_index + np.arange(number_of_rows, dtype=np.int64)) * 100
    timestamps = base_epoch + timestamp_offsets.astype("timedelta64[ms]")

    # 2. Station & Antenna Metadata
    stations = rng.choice(VALID_STATIONS, size=number_of_rows)
    rcu_ids = rng.integers(0, 192, size=number_of_rows, dtype=np.int32)
    antenna_types = rng.choice(["LBA", "HBA"], size=number_of_rows, p=[0.5, 0.5])

    # 3. Frequency Bands (Conditional on Antenna Type)
    # LBA: 10.0 - 90.0 MHz | HBA: 110.0 - 240.0 MHz
    lba_freqs = rng.uniform(10.0, 90.0, size=number_of_rows)
    hba_freqs = rng.uniform(110.0, 240.0, size=number_of_rows)
    frequencies = np.where(antenna_types == "LBA", lba_freqs, hba_freqs)

    # 4. Status and Hardware States
    hardware_status = rng.choice(HARDWARE_STATES, size=number_of_rows, p=HARDWARE_WEIGHTS)
    gps_sync_status = rng.choice(GPS_STATES, size=number_of_rows, p=GPS_WEIGHTS)

    # 5. Continuous Signals
    # Signal Power normal operational envelope: -120.0 to -40.0 dBm
    signal_power = rng.normal(loc=-75.0, scale=12.0, size=number_of_rows)
    ambient_temp = rng.normal(loc=12.5, scale=6.5, size=number_of_rows)
    packet_drop_rate = rng.exponential(scale=0.005, size=number_of_rows).clip(0.0, 0.05)

    # 6. RFI Detection (True if status is SPURIOUS, otherwise ~1% false-positive noise)
    rfi_detected = (hardware_status == "SPURIOUS") | (
        rng.uniform(0.0, 1.0, size=number_of_rows) < 0.01
    )

    # Inject Contract Violations (Engineered Traps for Validation)

    # Trap 1: Regex Violations (Station ID corruption) ~0.2%
    corrupt_station_mask = rng.uniform(0.0, 1.0, size=number_of_rows) < 0.002
    stations[corrupt_station_mask] = "ERR_STN_999"

    # Trap 2: Out of Bounds RCU (Valid range: 0 - 191) ~0.2%
    corrupt_rcu_mask = rng.uniform(0.0, 1.0, size=number_of_rows) < 0.002
    rcu_ids[corrupt_rcu_mask] = 255

    # Trap 3: Cross-Column Frequency Boundary Violation ~0.5%
    # (HBA antenna operating within LBA band frequencies)
    frequency_anomaly_mask = (rng.uniform(0.0, 1.0, size=number_of_rows) < 0.005) & (
        antenna_types == "HBA"
    )
    frequencies[frequency_anomaly_mask] = 42.0

    # Trap 4: Telemetry Failure Mismatch (SHORT circuit reporting positive power)
    short_anomaly_mask = hardware_status == "SHORT"
    signal_power[short_anomaly_mask] = 0.0  # Physically collapsed dBm level
    # 10% of shorted units suffer reporting glitched high-water power
    signal_power[short_anomaly_mask & (rng.uniform(0, 1, size=number_of_rows) < 0.1)] = 99.9

    # Trap 5: Thermal Malfunction (~0.1% extreme physics violations)
    temp_anomaly_mask = rng.uniform(0.0, 1.0, size=number_of_rows) < 0.001
    ambient_temp[temp_anomaly_mask] = rng.choice([-273.15, 650.0], size=temp_anomaly_mask.sum())

    # Trap 6: Missing Coordinates on Lost Sync (~0.2% Null injection)
    sync_failure_mask = (gps_sync_status == "LOST_SYNC") & (
        rng.uniform(0.0, 1.0, size=number_of_rows) < 0.5
    )
    if sync_failure_mask.any():
        timestamps[sync_failure_mask] = np.datetime64("NaT", "ms")

    # Trap 7: Composite Primary Key Duplication (Network Retransmission)
    # Duplicate first 5 rows across the batch
    if number_of_rows >= 10:
        timestamps[5:10] = timestamps[0:5]
        stations[5:10] = stations[0:5]
        rcu_ids[5:10] = rcu_ids[0:5]

    return pl.DataFrame(
        {
            "timestamp_utc": pl.Series("timestamp_utc", timestamps, dtype=pl.Datetime("ms")),
            "station_id": pl.Series("station_id", stations, dtype=pl.String),
            "rcu_id": pl.Series("rcu_id", rcu_ids, dtype=pl.Int32),
            "antenna_type": pl.Series("antenna_type", antenna_types, dtype=pl.Categorical),
            "frequency_mhz": pl.Series("frequency_mhz", np.round(frequencies, 3), dtype=pl.Float64),
            "hardware_status": pl.Series("hardware_status", hardware_status, dtype=pl.Categorical),
            "signal_power_db": pl.Series(
                "signal_power_db", np.round(signal_power, 2), dtype=pl.Float64
            ),
            "rfi_flag": pl.Series("rfi_flag", rfi_detected, dtype=pl.Boolean),
            "ambient_temp_c": pl.Series(
                "ambient_temp_c", np.round(ambient_temp, 2), dtype=pl.Float64
            ),
            "gps_sync_status": pl.Series("gps_sync_status", gps_sync_status, dtype=pl.Categorical),
            "packet_drop_rate": pl.Series(
                "packet_drop_rate", np.round(packet_drop_rate, 4), dtype=pl.Float64
            ),
            "firmware_version": pl.Series(
                "firmware_version", ["LOFAR2.0-rc4"] * number_of_rows, dtype=pl.String
            ),
        }
    )


def generate_lofar_dataset(
    output_path: Path,
    total_rows: int = 10_000_000,
    chunk_size: int = 2_000_000,
    seed: int = 42,
) -> Path:
    """
    Generates a scaled LOFAR dataset using Polars out-of-core streaming chunks.
    """
    output_path = Path(output_path).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_chunks = max(1, (total_rows + chunk_size - 1) // chunk_size)
    print(f"\n[+] Synthesizing LOFAR Telemetry: {total_rows:,} rows across {total_chunks} chunks")
    print(f"[+] Output Target: {output_path}")

    start_time = time.perf_counter()
    writer = None

    try:
        rows_remaining = total_rows
        current_idx = 0

        for chunk_idx in range(total_chunks):
            current_batch_size = min(rows_remaining, chunk_size)
            chunk_seed = seed + chunk_idx

            t0 = time.perf_counter()
            # Generate the Polars DataFrame
            df_chunk = generate_chunk(current_idx, current_batch_size, chunk_seed)

            # Convert to PyArrow Table
            arrow_table = df_chunk.to_arrow()

            # Initialize writer on the first chunk using the inferred schema
            if writer is None:
                writer = pq.ParquetWriter(output_path, arrow_table.schema, compression="snappy")

            # Stream directly into the final file
            writer.write_table(arrow_table)
            chunk_time = time.perf_counter() - t0

            current_idx += current_batch_size
            rows_remaining -= current_batch_size

            print(
                f"    -> Chunk {chunk_idx + 1:02d}/{total_chunks} "
                f"({current_batch_size:,} rows) written in {chunk_time:.2f}s"
            )
    finally:
        if writer is not None:
            writer.close()

    elapsed = time.perf_counter() - start_time
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    velocity = (total_rows / elapsed) / 1_000_000

    print("\n[✔] LOFAR Dataset Generation Complete!")
    print(f"    - Total Rows:    {total_rows:,}")
    print(f"    - File Size:     {file_size_mb:.2f} MB")
    print(f"    - Elapsed Time:  {elapsed:.2f}s")
    print(f"    - Velocity:      {velocity:.2f}M rows/sec\n")

    return output_path


class LofarTelemetryGenerator(BaseDatasetGenerator):
    """Object-oriented wrapper implementing BaseDatasetGenerator for LOFAR telemetry."""

    def generate(self, destination: Path) -> Path:
        return generate_lofar_dataset(
            output_path=destination,
            total_rows=self.config.total_rows,
            chunk_size=self.config.chunk_size,
            seed=self.config.seed,
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic LOFAR telemetry datasets for benchmarking."
    )
    parser.add_argument(
        "--rows",
        "-n",
        type=int,
        default=10_000_000,
        help="Number of telemetry rows to generate (default: 10,000,000).",
    )
    parser.add_argument(
        "--chunk-size",
        "-c",
        type=int,
        default=2_000_000,
        help="Chunk size for streaming disk synthesis (default: 2,000,000).",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("benchmarks/data/synthetic/lofar_telemetry_10M.parquet"),
        help="Output destination parquet file.",
    )
    parser.add_argument(
        "--seed",
        "-s",
        type=int,
        default=42,
        help="RNG seed for deterministic replication (default: 42).",
    )

    args = parser.parse_args()
    generate_lofar_dataset(
        output_path=args.output,
        total_rows=args.rows,
        chunk_size=args.chunk_size,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
