# Netra Validator Benchmark Suite

The suite includes scripts for dataset generation, hardware/environment fingerprinting, running the benchmarks using isolated subprocess instances for accurate memory tracking (`ru_maxrss`), and results aggregation.

## Quickstart

Run all commands from the repository root using `uv`:

### 1. In-Memory Scaling Run (1 Warmup + 3 Timed Iterations)

Generates the dataset automatically if not already present on disk, runs 1 warmup pass and executes 3 isolated measurement iterations:

```bash
uv run python benchmarks/run.py --scale 100M --warmup 1 --iterations 3
```

### 2. Out-of-Core Envelope Run (Single Cold-Start Pass)

For large out-of-core (`spill_to_disk`) workloads, skip the warmup pass and run a single cold-start iteration to avoid thermal throttling or SSD write-cache saturation:

```bash
uv run python benchmarks/run.py --scale 1B --no-warmup --iterations 1
```

### 3. Pre-Generate Datasets Only

Generate datasets in streaming `2,000,000`-row chunks without running validation:

```bash
uv run python benchmarks/run.py --scale 500M --generate-only
```

You can also run the standalone LOFAR telemetry dataset generation script directly:

```bash
uv run python benchmarks/generators/lofar_telemetry.py \
  --rows 10000000 \
  --chunk-size 2000000 \
  --seed 42 \
  --output benchmarks/data/synthetic/lofar_10M.parquet
```

## CLI Options (`benchmarks/run.py`)

| Flag | Default | Description |
| :--- | :--- | :--- |
| `--suite`, `-s` | `lofar` | Benchmark domain suite (`lofar`) |
| `--scale` | `10M` | Dataset scale (`500K`, `10M`, `50M`, `100M`, `500M`, `1B`, `3B`) |
| `--iterations`, `-i` | `5` | Number of isolated measurement iterations |
| `--warmup`, `-w` | `1` | Number of warmup passes prior to timed runs (`0` to skip) |
| `--no-warmup` | `False` | Verbose/explicit alternate for `--warmup 0` |
| `--chunk-size` | `2M` | Streaming row-group chunk size during Parquet generation |
| `--seed` | `42` | Deterministic RNG seed for data and anomaly injection |
| `--force-generate` | `False` | Overwrite existing Parquet file and re-generate |
| `--generate-only` | `False` | Only synthesize the dataset; skip validation |
| `--target`, `-t` | `netra` | Validation adapter to execute (`netra`) |

## Implementation Notes

1. **Subprocess Memory Isolation (`harness/collector.py`):**
   Every warmup and measurement iteration spawns a fresh Python worker process (`--worker-mode`). This prevents Python Garbage Collector fragmentation, memory retention, or buffer caching from bleeding across iterations, allowing `resource.getrusage(resource.RUSAGE_SELF).ru_maxrss` to record the exact lifetime Peak RSS of each run.
2. **Scratch & Spill Workspace (`targets/netra.py`):**
   Before timing starts, `NetraBenchmarkTarget.setup()` creates a process-isolated `.netra_bench_<pid>/` directory alongside the input dataset for `valid_rows.parquet`, `quarantine_rows.parquet`, and `engine_spill/`. Output partitions are removed outside the timed window after each run, and `teardown()` removes the scratch directory on exit.
3. **Configuring In-Memory vs. Out-of-Core Mode:**
   Toggle `spill_to_disk` in `benchmarks/targets/netra.py` inside `NetraBenchmarkTarget.setup()`:
   - **In-Memory Mode:** `"enabled": False`
   - **Out-of-Core (32 GB Laptop):** `"enabled": True`, `"memory_limit_mb": 16384`, `"chunk_size": 50_000`
   - **Out-of-Core (64 GB Server):** `"enabled": True`, `"memory_limit_mb": 32768`, `"chunk_size": 250_000`

## Synthetic LOFAR Telemetry & Injected Anomalies

`generators/lofar_telemetry.py` generates a 12-column synthetic LOFAR telemetry dataset seeded deterministically per chunk (`seed + chunk_idx`). Each chunk injects **7 deterministic anomaly traps** that trigger **8 failed contract targets** (5 column-level targets in Pass 1 + 3 global SQL targets in Pass 2):

| Trap | Target Column / Rule | Injected Anomaly | Pass |
| :--- | :--- | :--- | :--- |
| **1. Station ID Corruption** | `station_id` (`pattern`) | ~0.2% mutated to `ERR_STN_999` | Pass 1 (Polars) |
| **2. RCU Out-of-Bounds** | `rcu_id` (`maximum: 191`) | ~0.2% set to `255` | Pass 1 (Polars) |
| **3. Thermal Malfunction** | `ambient_temp_c` (`-25.0` to `50.0`) | ~0.1% spiked to `-273.15°C` or `650.0°C` | Pass 1 (Polars) |
| **4. GPS Sync Dropout** | `timestamp_utc` (`required`) | ~50% of `LOST_SYNC` rows set to `null` (`NaT`) | Pass 1 (Polars) |
| **5. Shorted Power Spike** | `signal_power_db` & SQL Check 2 | `SHORT` units set to `0.0 dBm` (10% spiked to `+99.9 dBm`) | Pass 1 & Pass 2 |
| **6. Band Mismatch** | SQL Check 1 (`antenna_type` vs `frequency_mhz`) | ~0.5% of `HBA` antennas assigned `42.0 MHz` (`LBA` band) | Pass 2 (DuckDB) |
| **7. Packet Retransmission** | SQL Check 4 (Composite Primary Key) | First 5 `(timestamp_utc, station_id, rcu_id)` keys duplicated per chunk | Pass 2 (DuckDB) |

*(Note: SQL Check 3, `check_spurious_rfi_correlation`, verifies that all `SPURIOUS` hardware states flag `rfi_flag = true` and is designed to **PASS** with `0` violations).*

## Output Artifacts

Each benchmark run writes a JSON report to `benchmarks/results/latest.json` and archives a copy in `benchmarks/results/history/run_<YYYYMMDD_HHMMSS>_<git_hash>.json` containing:

- `metadata`: UTC timestamp, git commit hash, suite, scale, warmup count, and iteration count.
- `system`: Host OS kernel, CPU model, physical/logical core counts, total RAM, and exact package versions (`netra-validator`, `polars`, `duckdb`, `pyarrow`, `numpy`).
- `dataset`: Resolved path, byte size on disk, and total rows evaluated.
- `statistics`: Aggregated `median`, `mean`, `std`, `min`, and `max` across timed iterations for `wall_time_seconds`, `engine_time_seconds`, `peak_rss_mb`, `throughput_mb_s`, and `row_velocity_m_rows_s`.
- `raw_iterations`: Per-iteration timings, Peak RSS, quarantined row counts, anomaly counts, and thread allocations.
