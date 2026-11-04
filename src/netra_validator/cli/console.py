"""
State Orchestrator for the Netra Validator CLI.
Connects the living application state to the pure UI components.
"""

import sys
from types import TracebackType

from rich import box
from rich.console import Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from netra_validator import __version__
from netra_validator.cli.components import (
    ErrorStateTuple,
    build_core_dump,
    build_error_section,
    build_execution_section,
    build_pipeline_section,
    build_pre_flight_grid,
    build_pre_flight_section,
    build_report_section,
    build_warning_section,
    console,
    get_frame_styles,
)
from netra_validator.cli.models import ExecutionManifest
from netra_validator.engine.validator import EngineTelemetry
from netra_validator.types import ValidationReport


class NetraValidatorRenderer:
    """
    Rich CLI UI Orchestrator.
    Holds execution state and pushes data to the UI components on a tick loop.
    """

    def __init__(self) -> None:
        self._phase = "init"
        self.pre_flight_status = "init"
        self.telemetry: EngineTelemetry | None = None
        self._pre_flight_grid: Table | None = None
        self.pre_flight_errors: list[tuple[str, str]] = []
        self.pre_flight_warnings: list[str] = []
        self.error_state: ErrorStateTuple | None = None
        self.warning_state: bool = False
        self.report_payload: ValidationReport | None = None

        self._tick = 0
        self._prev_phase = "BOOT"
        self._polars_wind_down_start_tick = -1
        self._duckdb_wind_down_start_tick = -1

        self.live = Live(
            self._build_layout(),
            console=console,
            refresh_per_second=8,
            transient=False,
            auto_refresh=False,
        )

    def _build_layout(self) -> Panel:
        # 1. Resolve master frame styles
        bg_color, border_style = get_frame_styles(
            self.error_state, self.pre_flight_status, self.warning_state
        )

        content = []

        # 2. Attach Pre-Flight payload
        content.extend(
            build_pre_flight_section(
                self.error_state, self.pre_flight_status, self._phase, self._pre_flight_grid
            )
        )

        # 3. Handle fatal diagnostic errors
        if (
            self.pre_flight_status == "pre_flight_failed"
            and self.pre_flight_errors
            and not self.error_state
        ):
            content.extend(build_error_section(self.pre_flight_errors))

        # 4. Handle heuristic warnings
        if (self._phase == "warning_prompt" or self.warning_state) and not self.error_state:
            content.extend(build_warning_section(self.pre_flight_warnings, self._phase))

        # 5. Attach active engine telemetry
        if self._phase in ("executing", "report") and not self.error_state:
            content.extend(
                build_execution_section(
                    self.telemetry,
                    self._tick,
                    self._polars_wind_down_start_tick,
                    self._duckdb_wind_down_start_tick,
                )
            )

        # 6. Attach Final Report Phase
        if self._phase == "report" and self.report_payload:
            content.extend(build_report_section(self.report_payload))
            content.extend(build_pipeline_section(self.report_payload))

        # 7. Handle hard system crashes
        if self.error_state:
            content.extend(build_core_dump(self.error_state))

        term_width = min(console.width or 120, 120)

        # 8. Compile to final output
        return Panel(
            Group(*content),
            title=f"[bold black on {bg_color}] NETRA VALIDATOR [ v{__version__} // BETA ] [/]",
            title_align="left",
            border_style=border_style,
            subtitle="[brand]NetraBase[/brand]",
            subtitle_align="right",
            box=box.SQUARE,
            padding=0,
            expand=True,
            width=term_width,
        )

    def _refresh(self) -> None:
        self.live.update(self._build_layout(), refresh=True)

    def set_initializing(self) -> None:
        self.pre_flight_status = "init"
        self._phase = "init"
        self._refresh()

    def set_warning_prompt(self) -> None:
        self._phase = "warning_prompt"
        self._refresh()

    def set_warning_state(self) -> None:
        self.warning_state = True
        self.pre_flight_status = "pre_flight_completed"
        self._phase = "pre_flight_completed"
        self._refresh()

    def render_pre_flight_panel(self, manifest: ExecutionManifest) -> None:
        self._pre_flight_grid = build_pre_flight_grid(manifest)
        self.pre_flight_errors = manifest.errors
        self.pre_flight_warnings = manifest.warnings
        self.pre_flight_status = (
            "pre_flight_failed" if self.pre_flight_errors else "pre_flight_completed"
        )
        self._phase = self.pre_flight_status
        self._refresh()

    def set_executing(self, telemetry: EngineTelemetry) -> None:
        self._phase = "executing"
        self.telemetry = telemetry
        self._refresh()

    def set_completed(self, report: ValidationReport) -> None:
        self._phase = "report"
        self.report_payload = report
        self._refresh()

    def tick_execution(self) -> None:
        self._tick += 1

        if self.telemetry:
            if self.telemetry.current_phase == "DUCKDB_PASS" and self._prev_phase in (
                "BOOT",
                "POLARS_PASS",
            ):
                self._polars_wind_down_start_tick = self._tick
            elif self.telemetry.current_phase == "ASSEMBLY" and self._prev_phase == "DUCKDB_PASS":
                self._duckdb_wind_down_start_tick = self._tick

            self._prev_phase = self.telemetry.current_phase

        self._refresh()

    def __enter__(self) -> "NetraValidatorRenderer":
        self.live.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        exc_traceback: TracebackType | None,
    ) -> None:
        if exc_type is not None and not issubclass(exc_type, SystemExit):
            self.error_state = (exc_type, exc_value, exc_traceback)
            self._refresh()
            self.live.stop()
            sys.exit(1)

        self.live.stop()
