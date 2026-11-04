"""
Main CLI entry point and execution orchestrator for Netra Validator.
"""

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Annotated

import typer

from netra_validator import __version__
from netra_validator.cli.auditor import GlobalTargetChoice, PreFlightAuditor, RunContext
from netra_validator.cli.console import NetraValidatorRenderer
from netra_validator.config import ValidatorConfig
from netra_validator.engine.validator import EngineTelemetry, Validator

validator_app = typer.Typer(
    name="validate",
    help="Execute a data validation run against an ODCS contract.",
    add_completion=False,
)


def _get_total_system_ram_mb() -> float:
    """Extracts total physical memory available on the host OS."""
    if sys.platform == "win32":
        import psutil  # type: ignore # noqa: PLC0415

        return psutil.virtual_memory().total / (1024 * 1024)

    return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / (1024 * 1024)


def _get_peak_process_ram_mb() -> float:
    """Extracts the high-water mark for memory consumption of the current process."""
    if sys.platform == "win32":
        import psutil  # type: ignore # noqa: PLC0415

        return psutil.Process(os.getpid()).memory_info().peak_wset / (1024 * 1024)

    import resource  # noqa: PLC0415

    rusage = resource.getrusage(resource.RUSAGE_SELF)
    return (
        (rusage.ru_maxrss / (1024 * 1024))
        if sys.platform == "darwin"
        else (rusage.ru_maxrss / 1024)
    )


def _handle_interactive_warnings(ui: NetraValidatorRenderer, force: bool) -> None:
    """Handles the blocking prompt loop for warning overrides."""
    if not force:
        ui.set_warning_prompt()
        while True:
            char = typer.getchar()
            if char.lower() in ("y", "\r", "\n"):
                break
            elif char.lower() == "n":
                sys.exit(0)
    ui.set_warning_state()


def _resolve_config_path(explicit_config: str | None) -> Path | None:
    """Resolves the configuration file path via CLI flag, env var, or local directory."""
    if explicit_config:
        return Path(explicit_config).resolve()
    if env_config := os.environ.get("NETRA_CONFIG"):
        return Path(env_config).resolve()
    default_local = Path("netra_config.yaml")
    return default_local.resolve() if default_local.is_file() else None


def _build_configured_validator(
    context: RunContext,
    loaded_config: ValidatorConfig,
    config_source: str,
    fail_on_critical: bool | None,
    fail_on_warnings: bool | None,
) -> Validator:
    """Initializes the Validator instance with the resolved config and CLI overrides."""
    if fail_on_critical is not None:
        loaded_config.pipeline.fail_on_critical = fail_on_critical
    if fail_on_warnings is not None:
        loaded_config.pipeline.fail_on_warnings = fail_on_warnings

    if loaded_config.pipeline.fail_on_critical or loaded_config.pipeline.fail_on_warnings:
        loaded_config.pipeline.quality_gate_active = True

    return Validator(
        contract_path=context.contract_path,
        dataset_path=context.dataset_path,
        output_path=context.valid_path,
        quarantine_path=context.quarantine_path,
        config=loaded_config,
        config_source=config_source,
    )


def _version_callback(value: bool) -> None:
    """Prints the package version and exits."""
    if value:
        sys.stdout.write(f"netra-validator {__version__}\n")
        raise typer.Exit(0)


@validator_app.callback(invoke_without_command=True)
def run_validator(  # noqa: B008, PLR0913, PLR0917
    dataset: str = typer.Argument(..., help="Path to the target dataset (Parquet/CSV)."),
    contract: str = typer.Argument(..., help="Path to the ODCS YAML contract."),
    valid: str = typer.Option(None, "--valid", "-o", help="Path to save the valid rows."),
    quarantine: str = typer.Option(None, "--quarantine", "-q", help="Path to save failing rows."),
    global_target: Annotated[
        GlobalTargetChoice | None,
        typer.Option(
            "--global-target",
            help="Evaluate 'raw' or 'validated' output for the global SQL checks.",
        ),
    ] = None,
    config: Annotated[
        str | None, typer.Option("--config", "-c", help="Path to netra_config.yaml.")
    ] = None,
    force: bool = typer.Option(
        False, "--force", "-y", help="Bypass interactive prompts for warning states."
    ),
    fail_on_critical: Annotated[
        bool | None,
        typer.Option(
            "--fail-on-critical",
            help="Enable Quality Gate and break pipeline on CRITICAL anomalies.",
        ),
    ] = None,
    fail_on_warnings: Annotated[
        bool | None,
        typer.Option(
            "--fail-on-warnings", help="Enable Quality Gate and break pipeline on ANY anomalies."
        ),
    ] = None,
    json_output: Annotated[
        bool,
        typer.Option(
            "--json",
            help="Disable visual CLI output and emit the raw validation report as JSON.",
        ),
    ] = False,
    version: Annotated[
        bool | None,
        typer.Option(
            "--version",
            "-V",
            "-v",
            callback=_version_callback,
            is_eager=True,
            help="Show the netra-validator version and exit.",
        ),
    ] = None,
) -> None:
    """Run a data validation session"""

    # 1. Resolve context
    dataset_relative_path = Path(dataset)
    context = RunContext(
        dataset_path=dataset_relative_path.resolve(),
        contract_path=Path(contract).resolve(),
        valid_path=Path(valid).resolve()
        if valid
        else dataset_relative_path.parent / f"validated_{dataset_relative_path.name}",
        quarantine_path=Path(quarantine).resolve()
        if quarantine
        else dataset_relative_path.parent / f"quarantined_{dataset_relative_path.name}",
        config_path=_resolve_config_path(config),
        global_target=global_target,
        force=force,
    )

    # 2. Evaluate pre-flight
    auditor = PreFlightAuditor(context)
    manifest = auditor.execute()

    # 3A. Headless JSON Execution Mode (Bypasses TUI for piping to jq / files)
    if json_output:
        if manifest.errors:
            for err_type, err_msg in manifest.errors:
                sys.stderr.write(f"[{err_type}] {err_msg}\n")
            sys.exit(1)

        validator = _build_configured_validator(
            context,
            auditor.loaded_config,
            manifest.config_source,
            fail_on_critical,
            fail_on_warnings,
        )
        report = validator.run()
        sys.stdout.write(f"{json.dumps(report, indent=2)}\n")
        pipeline_ctx = report["_meta"]["pipeline_context"]
        sys.exit(pipeline_ctx["exit_code"] if pipeline_ctx is not None else 0)

    # 3B. Interactive Rich TUI Execution Mode
    with NetraValidatorRenderer() as ui:
        ui.set_initializing()
        ui.render_pre_flight_panel(manifest)

        if manifest.errors:
            sys.exit(1)

        if manifest.warnings:
            _handle_interactive_warnings(ui, context.force)

        # 4. Engine execution
        system_total_ram_mb = _get_total_system_ram_mb()
        validator = _build_configured_validator(
            context,
            auditor.loaded_config,
            manifest.config_source,
            fail_on_critical,
            fail_on_warnings,
        )

        telemetry = EngineTelemetry(
            current_phase="BOOT",
            start_time=time.time(),
            polars_threads=validator.polars_threads,
            duckdb_threads=validator.duckdb_threads,
            total_ram_mb=system_total_ram_mb,
        )

        validator.telemetry = telemetry
        ui.set_executing(telemetry)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(validator.run)

            while not future.done():
                ui.tick_execution()
                time.sleep(0.125)

            report = future.result()
            engine_time = time.time() - telemetry.start_time
            file_size_bytes = os.path.getsize(context.dataset_path)
            throughput = (file_size_bytes / engine_time) / (1024**3) if engine_time > 0 else 0.0

            try:
                total_rows = report.get("dataset", {}).get("total_rows_evaluated", 0)
            except (AttributeError, TypeError):
                total_rows = 0

            row_velocity = (total_rows / engine_time) / 1_000_000 if engine_time > 0 else 0.0

            telemetry.engine_time = engine_time
            telemetry.throughput_gb_s = throughput
            telemetry.row_velocity = row_velocity
            telemetry.peak_ram_mb = _get_peak_process_ram_mb()

            # Force the UI to finish the DuckDB 'Wind-Down'
            for _ in range(9):
                ui.tick_execution()
                time.sleep(0.125)

            # 5. Render Final Report
            ui.set_completed(report)

    # Hard terminate with exact pipeline status
    pipeline_ctx = report["_meta"]["pipeline_context"]
    sys.exit(pipeline_ctx["exit_code"] if pipeline_ctx is not None else 0)


@validator_app.command("dummy", hidden=True)
def _dummy() -> None:
    """Hidden command to satisfy Typer's subcommand registry requirements."""
    pass


def register() -> typer.Typer:
    return validator_app


if __name__ == "__main__":
    validator_app()
