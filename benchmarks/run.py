"""
Unified Benchmark Orchestration CLI for Netra Validator.

Manages dataset generation, environment verification, isolated subprocess
benchmark execution, statistical compilation, and result persistence.
"""

from __future__ import annotations

import argparse
import datetime
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

# Add project root to path to ensure benchmark modules resolve cleanly
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# isort: split
from benchmarks.generators.base import GeneratorConfig, parse_scale  # noqa: E402
from benchmarks.generators.lofar_telemetry import (  # noqa: E402
    LofarTelemetryGenerator,
)
from benchmarks.harness.collector import run_target_isolated  # noqa: E402
from benchmarks.harness.system import get_system_fingerprint  # noqa: E402

BENCHMARKS_DIR = PROJECT_ROOT / "benchmarks"
DATA_DIR = BENCHMARKS_DIR / "data"
SYNTHETIC_DATA_DIR = DATA_DIR / "synthetic"
CONFIGS_DIR = BENCHMARKS_DIR / "configs"
RESULTS_DIR = BENCHMARKS_DIR / "results"
HISTORY_DIR = RESULTS_DIR / "history"
LATEST_FILE = RESULTS_DIR / "latest.json"


def get_git_commit() -> str:
    """Safely extracts the current short git commit hash."""
    try:
        res = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=str(PROJECT_ROOT),
        )
        return res.stdout.strip()
    except Exception:
        return "unknown"


def resolve_synthetic_dataset_path(suite: str, scale_str: str) -> Path:
    """Computes standard deterministic file paths for generated datasets."""
    return SYNTHETIC_DATA_DIR / f"{suite}_{scale_str.upper()}.parquet"


def resolve_contract_path(suite: str) -> Path:
    """Locates the ODCS YAML contract for the requested suite."""
    contract_path = CONFIGS_DIR / f"{suite}_telemetry_odcs.yaml"
    if not contract_path.exists():
        raise FileNotFoundError(f"Data contract not found for suite '{suite}' at: {contract_path}")
    return contract_path


def handle_generation(suite: str, scale_str: str, chunk_size: int, seed: int, force: bool) -> Path:
    """Coordinates dataset generation or verifies pre-existing files."""
    rows = parse_scale(scale_str)
    dataset_path = resolve_synthetic_dataset_path(suite, scale_str)

    if dataset_path.exists() and not force:
        size_mb = dataset_path.stat().st_size / (1024 * 1024)
        print(f"[i] Dataset exists: {dataset_path} ({size_mb:.2f} MB)")
        print("    (Use --force-generate to overwrite)\n")
        return dataset_path

    if suite == "lofar":
        config = GeneratorConfig(
            total_rows=rows,
            chunk_size=chunk_size,
            seed=seed,
            output_path=dataset_path,
        )
        generator = LofarTelemetryGenerator(config)
        return generator.generate(dataset_path)
    else:
        raise ValueError(f"Unknown benchmark suite: '{suite}'. Available: ['lofar']")


def compute_metric_stats(values: list[float]) -> dict[str, float]:
    """Computes median, mean, standard deviation, min, and max for metric samples."""
    if not values:
        return {"median": 0.0, "mean": 0.0, "std": 0.0, "min": 0.0, "max": 0.0}
    return {
        "median": round(statistics.median(values), 4),
        "mean": round(statistics.mean(values), 4),
        "std": round(statistics.stdev(values) if len(values) > 1 else 0.0, 4),
        "min": round(min(values), 4),
        "max": round(max(values), 4),
    }


def print_summary_table(
    target: str,
    suite: str,
    scale: str,
    stats: dict[str, dict[str, float]],
    sample_iteration: dict[str, Any],
) -> None:
    """Renders a clean, fixed-width ASCII comparison table of benchmark statistics."""
    title = f"BENCHMARK SUMMARY :: [{target.upper()}] on [{suite.upper()}_{scale.upper()}]"
    print("\n" + "=" * 76)
    print(f" {title.center(74)}")
    print("=" * 76)
    print(f" {'Metric':<25} {'Median':>10} {'Mean':>10} {'StdDev':>10} {'Min / Max':>16}")
    print("-" * 76)

    labels = [
        ("Wall Time (s)", "wall_time_seconds", ".2f"),
        ("Engine Time (s)", "engine_time_seconds", ".2f"),
        ("Peak RSS (MB)", "peak_rss_mb", ".1f"),
        ("Throughput (MB/s)", "throughput_mb_s", ".1f"),
        ("Velocity (M rows/s)", "row_velocity_m_rows_s", ".2f"),
    ]

    for label, key, fmt in labels:
        s = stats[key]
        min_max = f"{s['min']:{fmt}} / {s['max']:{fmt}}"
        std_str = f"±{s['std']:{fmt}}"
        print(
            f" {label:<25} {s['median']:>10{fmt}} {s['mean']:>10{fmt}} {std_str:>10} {min_max:>16}"
        )

    print("-" * 76)
    rows_eval = sample_iteration.get("rows_evaluated", 0)
    quarantine = sample_iteration.get("quarantined_rows", 0)
    anomalies = sample_iteration.get("anomalies_detected", 0)
    status_str = "PASSED" if sample_iteration.get("passed") else "FAILED"
    exit_code = sample_iteration.get("extra_meta", {}).get("exit_code", 0)

    print(f" Rows Evaluated: {rows_eval:,} | Quarantined: {quarantine:,} | Anomalies: {anomalies}")
    print(f" Quality Gate Status: {status_str} (Exit Code: {exit_code})")
    print("=" * 76 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="netra-benchmark",
        description="Netra Validator Benchmark Suite Orchestrator.",
    )
    parser.add_argument(
        "--suite",
        "-s",
        type=str,
        default="lofar",
        choices=["lofar"],
        help="Target benchmark suite domain (default: lofar).",
    )
    parser.add_argument(
        "--scale",
        type=str,
        default="10M",
        help="Scale tier to generate/test (e.g., 500K, 10M, 50M, 100M).",
    )
    parser.add_argument(
        "--chunk-size",
        type=str,
        default="2M",
        help="Streaming chunk size for file generation (default: 2M).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="RNG seed for deterministic synthesis (default: 42).",
    )
    parser.add_argument(
        "--force-generate",
        action="store_true",
        help="Re-generate the dataset even if the parquet file already exists.",
    )
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate the dataset; do not run target validation.",
    )
    parser.add_argument(
        "--target",
        "-t",
        type=str,
        default="netra",
        choices=["netra"],
        help="Target validation adapter to evaluate (default: netra).",
    )
    parser.add_argument(
        "--iterations",
        "-i",
        type=int,
        default=5,
        help="Number of measurement iterations (default: 5).",
    )
    parser.add_argument(
        "--warmup",
        "-w",
        type=int,
        nargs="?",
        const=1,
        default=1,
        help="Number of warmup passes before timed iterations (default: 1, set 0 to skip).",
    )
    parser.add_argument(
        "--no-warmup",
        dest="warmup",
        action="store_const",
        const=0,
        help="Skip the warmup pass (equivalent to --warmup 0).",
    )

    args = parser.parse_args()
    chunk_rows = parse_scale(args.chunk_size)

    print("=" * 76)
    print(" NETRA VALIDATOR BENCHMARK HARNESS")
    print(f" Suite: [{args.suite.upper()}]  |  Scale: [{args.scale.upper()}]")
    print("=" * 76)

    # 1. Dataset Generation / Resolution Phase
    t0 = time.perf_counter()
    dataset_file = handle_generation(
        suite=args.suite,
        scale_str=args.scale,
        chunk_size=chunk_rows,
        seed=args.seed,
        force=args.force_generate,
    )

    if args.generate_only:
        print(f"[✔] Dataset prepared at: {dataset_file}")
        print(f"[✔] Total prep duration: {time.perf_counter() - t0:.2f}s")
        return

    # 2. Target Execution Phase
    contract_file = resolve_contract_path(args.suite)
    system_fingerprint = get_system_fingerprint()

    print(
        f"[+] Dataset:  {dataset_file.name} ({dataset_file.stat().st_size / (1024 * 1024):.2f} MB)"
    )
    print(f"[+] Contract: {contract_file.name}")
    print(f"[+] CPU:      {system_fingerprint['cpu_model']} ({system_fingerprint['cpu_cores']})")
    print(f"[+] Memory:   {system_fingerprint['ram_total_gb']} GB RAM")
    print(
        f"[+] Target:   {args.target} "
        f"({args.warmup} warmup, {args.iterations} measurement iterations)"
    )

    # Warmup Run (Cold/Warm separation: Warmup primes kernel page cache)
    if args.warmup > 0:
        for w_idx in range(1, args.warmup + 1):
            label = f"Warmup Pass {w_idx}/{args.warmup}" if args.warmup > 1 else "Warmup Pass"
            print(f"\n[*] Running {label}...", end="", flush=True)
            t_warmup = time.perf_counter()
            warmup_result = run_target_isolated(args.target, dataset_file, contract_file)
            print(
                f" done in {time.perf_counter() - t_warmup:.2f}s "
                f"(Peak RSS: {warmup_result['peak_rss_mb']:.1f} MB)"
            )
    else:
        print("\n[*] Warmup Pass skipped (--warmup 0).")

    # Measurement Iterations (Fresh isolated subprocess per run)
    results: list[dict[str, Any]] = []
    print(f"\n[*] Commencing {args.iterations} isolated measurement iterations:")

    for iteration in range(1, args.iterations + 1):
        print(f"    -> Iteration {iteration:02d}/{args.iterations:02d}: ", end="", flush=True)
        res = run_target_isolated(args.target, dataset_file, contract_file)
        results.append(res)
        print(
            f"Wall: {res['wall_time_seconds']:.2f}s | "
            f"Engine: {res['engine_time_seconds']:.2f}s | "
            f"RSS: {res['peak_rss_mb']:.1f} MB | "
            f"Velocity: {res['row_velocity_m_rows_s']:.2f}M rows/s | "
            f"Gate: {'PASS' if res['passed'] else 'FAIL'}"
        )

    # 3. Statistical Aggregation
    metrics_to_aggregate = [
        "wall_time_seconds",
        "engine_time_seconds",
        "peak_rss_mb",
        "throughput_mb_s",
        "row_velocity_m_rows_s",
    ]
    aggregated_stats = {
        metric: compute_metric_stats([float(r[metric]) for r in results if r[metric] is not None])
        for metric in metrics_to_aggregate
    }

    print_summary_table(
        target=args.target,
        suite=args.suite,
        scale=args.scale,
        stats=aggregated_stats,
        sample_iteration=results[0],
    )

    # 4. Result Artifact Persistence
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    timestamp_utc = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
    git_hash = get_git_commit()

    run_payload = {
        "metadata": {
            "timestamp_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "git_commit": git_hash,
            "suite": args.suite,
            "scale": args.scale,
            "target": args.target,
            "warmup": args.warmup,
            "iterations": args.iterations,
        },
        "system": system_fingerprint,
        "dataset": {
            "path": str(dataset_file),
            "size_bytes": dataset_file.stat().st_size,
            "rows": results[0].get("rows_evaluated", 0),
        },
        "contract": str(contract_file),
        "statistics": aggregated_stats,
        "raw_iterations": results,
    }

    history_file = HISTORY_DIR / f"run_{timestamp_utc}_{git_hash}.json"
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(run_payload, f, indent=2)

    with open(LATEST_FILE, "w", encoding="utf-8") as f:
        json.dump(run_payload, f, indent=2)

    print(f"[✔] Benchmark run saved to:   {history_file.relative_to(PROJECT_ROOT)}")
    print(f"[✔] Latest metrics linked to: {LATEST_FILE.relative_to(PROJECT_ROOT)}\n")


if __name__ == "__main__":
    main()
