"""
Isolated Subprocess Telemetry Collector.

Guarantees pristine memory tracking by executing every benchmark iteration
inside a freshly spawned Python subprocess, preventing GC fragmentation
and cross-iteration cache pollution.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

# Add project root to path for subprocess imports
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
SRC_DIR = PROJECT_ROOT / "src"

# Inject the src/ directory first so Python natively finds netra_validator
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(1, str(PROJECT_ROOT))


def get_peak_rss_mb() -> float:
    """
    Extracts the true OS-level Peak Resident Set Size (RSS) for the current process.
    Handles the unit differences between Linux (Kilobytes) and macOS (Bytes).
    """
    if sys.platform == "win32":
        import psutil  # type: ignore

        return psutil.Process(os.getpid()).memory_info().peak_wset / (1024 * 1024)

    import resource

    rusage = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":
        # macOS returns bytes
        return rusage.ru_maxrss / (1024 * 1024)
    # Linux returns kilobytes
    return rusage.ru_maxrss / 1024


def run_target_isolated(
    target_name: str, dataset_path: Path, contract_path: Path
) -> dict[str, Any]:
    """
    Spawns a fresh Python subprocess to execute the benchmark target.

    Returns:
        The BenchmarkResult parsed from the subprocess JSON output.
    """
    cmd = [
        sys.executable,
        __file__,  # Execute this exact script as the worker
        "--worker-mode",
        "--target",
        target_name,
        "--dataset",
        str(dataset_path),
        "--contract",
        str(contract_path),
    ]

    # --- INJECT OOC FLAG BEFORE SUBPROCESS BOOTS ---
    worker_env = os.environ.copy()
    worker_env["POLARS_FORCE_OOC"] = "1"
    # -----------------------------------------------

    try:
        # Run process, capturing stdout for JSON and stderr for debugging
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
            cwd=str(PROJECT_ROOT),
            env=worker_env,
        )

        # Parse the last line of stdout as the JSON payload
        output_lines = result.stdout.strip().split("\n")
        json_payload = output_lines[-1]

        return json.loads(json_payload)

    except subprocess.CalledProcessError as e:
        print(f"\n[CRITICAL] Subprocess Worker Failed (Exit {e.returncode})")
        print("--- STDOUT ---")
        print(e.stdout)
        print("--- STDERR ---")
        print(e.stderr)
        raise RuntimeError(f"Benchmark worker for '{target_name}' crashed.") from e


# =============================================================================
# SUBPROCESS WORKER EXECUTION
# =============================================================================


def _execute_worker_mode(target_name: str, dataset_path: Path, contract_path: Path) -> None:
    """Executes the target adapter, extracts OS telemetry, and dumps JSON."""

    # 1. Resolve Target Adapter
    if target_name == "netra":
        from benchmarks.targets.netra import NetraBenchmarkTarget

        target_adapter = NetraBenchmarkTarget()
    else:
        raise ValueError(f"Unknown benchmark target adapter: '{target_name}'")

    # 2. Setup (Excluded from timing)
    target_adapter.setup(dataset_path, contract_path)

    try:
        # 3. Execution (Timed inside the adapter)
        result_obj = target_adapter.run_validation()

        # 4. Telemetry Collection (OS-level Peak RSS)
        # We override the 0.0 placeholder set by the adapter with the true OS metric
        result_dict = result_obj if isinstance(result_obj, dict) else result_obj.__dict__.copy()
        result_dict["peak_rss_mb"] = round(get_peak_rss_mb(), 2)

        # 5. Output Payload
        print(json.dumps(result_dict))

    finally:
        # 6. Teardown
        target_adapter.teardown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Isolated Benchmark Worker")
    parser.add_argument("--worker-mode", action="store_true", required=True)
    parser.add_argument("--target", type=str, required=True)
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--contract", type=Path, required=True)
    args = parser.parse_args()

    _execute_worker_mode(args.target, args.dataset, args.contract)
