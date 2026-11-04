"""
Polars Execution Engine (Pass 1).

Executes row-level validation rules using Polars.
Instead of relying on LazyFrame streaming the whole file (which is prone to OOMs on
complex DAGs), this module orchestrates PyArrow micro-batches through an eager/lazy-fused
Polars evaluation loop. It guarantees absolute memory safety, while asynchronously routing
validated and quarantined partitions to disk.
"""

import contextlib
import queue
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import polars as pl
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from netra_validator.config import SpillConfig
from netra_validator.exceptions import ExecutionEngineError

LOWER_ROW_GROUP_SIZE = 10_000
UPPER_ROW_GROUP_SIZE = 10_000_000
DEFAULT_BATCH_SIZE = 2_000_000

# Bounded queue depth enforces backpressure if disk I/O lags behind CPU evaluation
IO_QUEUE_MAXSIZE = 3


@dataclass
class RuleValidationResult:
    """The failure metrics for a specific rule on a column."""

    rule_name: str
    failed_row_count: int


@dataclass
class ColumnValidationResult:
    """The row-level failure metrics for a single column."""

    column_name: str
    failed_row_count: int
    rule_results: list[RuleValidationResult]


@dataclass
class PolarsValidationResult:
    """The aggregate metrics for the Polars Pass streaming execution."""

    total_rows: int
    valid_rows: int
    quarantined_rows: int
    column_results: list[ColumnValidationResult]
    unvalidated_columns: list[str]


@dataclass
class _CompiledExecutionPlan:
    """Pre-compiled static Polars expression DAGs for the micro-batch loop."""

    rule_validity_flags: list[pl.Expr]
    rule_flag_names: list[tuple[str, str, str]]
    column_validity_flags: list[pl.Expr]
    column_flag_names: list[str]
    row_validity_flag: str
    row_flag_expression: pl.Expr
    metric_expressions: list[pl.Expr]
    initial_failed_counts: dict[str, int]


def _mount_pyarrow_dataset(dataset_path: str) -> ds.Dataset:
    """Mounts the physical file via PyArrow for streaming batch extraction."""
    format = Path(dataset_path).suffix.lower()
    try:
        if format == ".csv":
            return ds.dataset(dataset_path, format="csv")
        elif format in {".ipc", ".arrow"}:
            return ds.dataset(dataset_path, format="ipc")
        else:
            # Defaults natively to Parquet for both .parquet files and partitioned directories
            return ds.dataset(dataset_path, format="parquet")
    except Exception as e:
        raise ExecutionEngineError(
            f"Failed to mount dataset via PyArrow {dataset_path}: {e}"
        ) from e


def _parquet_writer_worker(
    task_queue: queue.Queue[pa.Table | None],
    output_path: str | Path,
    error_queue: queue.Queue[Exception],
) -> None:
    """Background thread worker for async Parquet compression and disk I/O."""

    writer = None
    try:
        while True:
            batch = task_queue.get()
            if batch is None:  # Poison pill received; flush and shut down
                break
            if writer is None:
                writer = pq.ParquetWriter(output_path, batch.schema, compression="snappy")
            writer.write_table(batch)
    except Exception as e:
        error_queue.put(e)
    finally:
        if writer is not None:
            writer.close()


def _shutdown_writer_thread(
    worker_thread: threading.Thread | None,
    task_queue: queue.Queue[pa.Table | None],
) -> None:
    """
    Safely flushes and joins a background writer thread without risking a deadlock
    if the worker exited early due to an I/O exception while the queue was full.
    """
    if worker_thread is None:
        return

    while worker_thread.is_alive():
        try:
            task_queue.put(None, timeout=0.1)
            break
        except queue.Full:
            continue

    worker_thread.join()


def _resolve_batch_size(path: str, spill_config: SpillConfig) -> int:
    """
    Determines the PyArrow streaming chunk size. Enforces `spill_config.chunk_size`
    when out-of-core spilling is active, or aligns with physical Parquet Row Groups
    in in-memory mode.
    """
    if spill_config.enabled:
        # Out-of-Core Mode: Strictly clamp micro-batch size to bound active heap
        return spill_config.chunk_size

    # In-Memory Mode: Align batch size with physical Parquet Row Groups to
    # eliminate cross-boundary slicing overhead during reads.
    with contextlib.suppress(Exception):
        if path.lower().endswith(".parquet"):
            pf = pq.ParquetFile(path)
            if pf.metadata.num_row_groups > 0:
                aligned_size = pf.metadata.row_group(0).num_rows
                if LOWER_ROW_GROUP_SIZE <= aligned_size <= UPPER_ROW_GROUP_SIZE:
                    return int(aligned_size)

    return DEFAULT_BATCH_SIZE  # Fall back to DEFAULT_BATCH_SIZE if metadata inspection fails


def _stream_unconstrained_dataset(
    dataset: ds.Dataset,
    output_path: str | Path | None,
    batch_size: int,
    unvalidated_columns: list[str],
) -> PolarsValidationResult:
    """Fast-path passthrough when the contract defines zero row-level rules."""

    total_rows = int(dataset.count_rows())
    if output_path:
        valid_writer: pq.ParquetWriter | None = None
        try:
            for batch in dataset.to_batches(batch_size=batch_size):
                if valid_writer is None:
                    valid_writer = pq.ParquetWriter(output_path, batch.schema, compression="snappy")
                valid_writer.write_batch(batch)
        finally:
            if valid_writer is not None:
                valid_writer.close()

    return PolarsValidationResult(total_rows, total_rows, 0, [], unvalidated_columns)


def _compile_execution_plan(
    expressions: dict[str, dict[str, pl.Expr]],
) -> _CompiledExecutionPlan:
    """Pre-compiles all rule, column, row, and SIMD reduction expressions once."""

    rule_validity_flags: list[pl.Expr] = []
    rule_flag_names: list[tuple[str, str, str]] = []

    for column_name, rules_dict in expressions.items():
        for rule_name, expression in rules_dict.items():
            flag_name = f"_valid_{column_name}_{rule_name}"
            rule_flag_names.append((column_name, rule_name, flag_name))
            rule_validity_flags.append(expression.fill_null(False).alias(flag_name))

    column_validity_flags: list[pl.Expr] = []
    column_flag_names: list[str] = []

    for column_name, rules_dict in expressions.items():
        column_flag_name = f"_valid_{column_name}"
        column_flag_names.append(column_flag_name)
        column_rule_flags = [
            pl.col(f"_valid_{column_name}_{rule_name}") for rule_name in rules_dict
        ]
        column_validity_flags.append(pl.all_horizontal(column_rule_flags).alias(column_flag_name))

    row_validity_flag = "_row_is_valid"
    row_flag_expression = pl.all_horizontal([pl.col(m) for m in column_flag_names]).alias(
        row_validity_flag
    )

    # Pre-compile batch metric reduction expressions
    metric_expressions: list[pl.Expr] = [pl.col(row_validity_flag).sum().alias("batch_valid")]
    for column_flag in column_flag_names:
        metric_expressions.append((~pl.col(column_flag)).sum().alias(column_flag))
    for _, _, flag_name in rule_flag_names:
        metric_expressions.append((~pl.col(flag_name)).sum().alias(flag_name))

    initial_failed_counts: dict[str, int] = dict.fromkeys(column_flag_names, 0)
    for _, _, flag_name in rule_flag_names:
        initial_failed_counts[flag_name] = 0

    return _CompiledExecutionPlan(
        rule_validity_flags=rule_validity_flags,
        rule_flag_names=rule_flag_names,
        column_validity_flags=column_validity_flags,
        column_flag_names=column_flag_names,
        row_validity_flag=row_validity_flag,
        row_flag_expression=row_flag_expression,
        metric_expressions=metric_expressions,
        initial_failed_counts=initial_failed_counts,
    )


def _route_batch_partitions(
    df_eager: pl.DataFrame,
    row_mask: pl.Series,
    batch_valid_count: int,
    valid_queue: queue.Queue[pa.Table | None] | None,
    quarantine_queue: queue.Queue[pa.Table | None] | None,
) -> None:
    """Routes valid and quarantined rows to background Arrow writer queues."""

    batch_rows = df_eager.height

    if valid_queue is not None and batch_valid_count > 0:
        if batch_valid_count == batch_rows:
            # Fast-Path: 100% of batch is valid; skip DataFrame filtering
            valid_queue.put(df_eager.to_arrow())
        else:
            valid_queue.put(df_eager.filter(row_mask).to_arrow())

    batch_quarantined_count = batch_rows - batch_valid_count
    if quarantine_queue is not None and batch_quarantined_count > 0:
        if batch_quarantined_count == batch_rows:
            # Fast-Path: 100% of batch is quarantined; skip DataFrame filtering
            quarantine_queue.put(df_eager.to_arrow())
        else:
            quarantine_queue.put(df_eager.filter(~row_mask).to_arrow())


def _assemble_column_results(
    expressions: dict[str, dict[str, pl.Expr]],
    failed_counts: dict[str, int],
) -> list[ColumnValidationResult]:
    """Maps accumulated failure counts into structured column validation results."""
    column_results: list[ColumnValidationResult] = []

    for column_name, rules_dict in expressions.items():
        column_failed_rows = failed_counts[f"_valid_{column_name}"]
        rule_results = [
            RuleValidationResult(
                rule_name=rule_name,
                failed_row_count=failed_counts[f"_valid_{column_name}_{rule_name}"],
            )
            for rule_name in rules_dict
        ]
        column_results.append(
            ColumnValidationResult(
                column_name=column_name,
                failed_row_count=column_failed_rows,
                rule_results=rule_results,
            )
        )

    return column_results


def execute_polars_rules(
    dataset_path: str | Path,
    expressions: dict[str, dict[str, pl.Expr]],
    output_path: str | Path | None,
    quarantine_path: str | Path | None,
    spill_config: SpillConfig,
) -> PolarsValidationResult:
    """
    Executes row-level constraints using the PyArrow Micro-Batch Sieve pattern.
    """
    path = str(dataset_path)
    batch_size = _resolve_batch_size(path, spill_config)

    dataset = _mount_pyarrow_dataset(path)
    physical_columns = dataset.schema.names
    unvalidated_columns = [col for col in physical_columns if col not in expressions]

    if not expressions:
        return _stream_unconstrained_dataset(dataset, output_path, batch_size, unvalidated_columns)

    # 1. Compile Static Polars Expression Plan

    plan = _compile_execution_plan(expressions)
    failed_counts = plan.initial_failed_counts.copy()
    total_rows = 0
    total_valid = 0

    # 2. Setup Async I/O Queues & Background Workers

    # Maxsize acts as our backpressure. If disk is too slow, CPU waits rather than OOMing.
    valid_queue: queue.Queue[pa.Table | None] = queue.Queue(maxsize=IO_QUEUE_MAXSIZE)
    quarantine_queue: queue.Queue[pa.Table | None] = queue.Queue(maxsize=IO_QUEUE_MAXSIZE)
    error_queue: queue.Queue[Exception] = queue.Queue()

    valid_thread: threading.Thread | None = None
    quarantine_thread: threading.Thread | None = None

    if output_path:
        valid_thread = threading.Thread(
            target=_parquet_writer_worker,
            args=(valid_queue, output_path, error_queue),
            daemon=True,
        )
        valid_thread.start()

    if quarantine_path:
        quarantine_thread = threading.Thread(
            target=_parquet_writer_worker,
            args=(quarantine_queue, quarantine_path, error_queue),
            daemon=True,
        )
        quarantine_thread.start()

    # 3. Execution: The Streaming Async Sieve Loop
    try:
        for batch in dataset.to_batches(batch_size=batch_size):
            # Fail fast if a background I/O thread encountered a disk/write error
            if not error_queue.empty():
                raise ExecutionEngineError(f"Background I/O Error: {error_queue.get()}")

            # Zero-copy Arrow-to-Polars memory pointer handoff
            df_eager = cast(pl.DataFrame, pl.from_arrow(batch))
            total_rows += df_eager.height

            # Execute via .lazy().collect() on the micro-batch so the Polars query
            # optimizer can do its magic and not materialize intermediate DataFrame copies.
            # We compute rule, column, and row validity masks in a separate DataFrame via
            # .select() so physical data columns are never copied or  widened with temporary
            # boolean columns.
            masks_df = (
                df_eager.lazy()
                .select(plan.rule_validity_flags)
                .with_columns(plan.column_validity_flags)
                .with_columns(plan.row_flag_expression)
                .collect()
            )

            # Single-pass boolean reduction across all rule & column flags
            batch_metrics = masks_df.select(plan.metric_expressions).to_dicts()[0]

            batch_valid_count = int(batch_metrics["batch_valid"])
            total_valid += batch_valid_count

            for column_flag in plan.column_flag_names:
                failed_counts[column_flag] += int(batch_metrics[column_flag])
            for _, _, flag_name in plan.rule_flag_names:
                failed_counts[flag_name] += int(batch_metrics[flag_name])

            _route_batch_partitions(
                df_eager=df_eager,
                row_mask=masks_df.get_column(plan.row_validity_flag),
                batch_valid_count=batch_valid_count,
                valid_queue=valid_queue if output_path else None,
                quarantine_queue=quarantine_queue if quarantine_path else None,
            )

    finally:
        # 4. Teardown: Flush Queues and Join Threads
        _shutdown_writer_thread(valid_thread, valid_queue)
        _shutdown_writer_thread(quarantine_thread, quarantine_queue)

        # Final check for I/O errors during shutdown/flush
        if not error_queue.empty():
            raise ExecutionEngineError(f"Background I/O Error: {error_queue.get()}")

    # 5. Assemble Final Validation Telemetry
    return PolarsValidationResult(
        total_rows=total_rows,
        valid_rows=total_valid,
        quarantined_rows=total_rows - total_valid,
        column_results=_assemble_column_results(expressions, failed_counts),
        unvalidated_columns=unvalidated_columns,
    )
