"""
Pure UI components for the Netra CLI.
Functions map state directly to Rich renderables.
"""

import time
import traceback
from types import TracebackType

from rich import box
from rich.console import Console, Group, RenderableType
from rich.padding import Padding
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text

from netra_validator.cli.models import ExecutionManifest
from netra_validator.cli.theme import NETRA_CLI_THEME
from netra_validator.engine.validator import EngineTelemetry
from netra_validator.types import RuleDiagnostic, ValidationReport

ErrorStateTuple = tuple[type[BaseException] | None, BaseException | None, TracebackType | None]

# 1. Component Constants

TICKS_PER_SECOND = 8
ONE_GB = 1024.0
RAM_USAGE_WARNING_THRESHOLD = 75
RAM_USAGE_CRITICAL_THRESHOLD = 90
THERMAL_DENSITY = 4
SQL_RULE_DISPLAY_LENGTH = 120

console = Console(theme=NETRA_CLI_THEME)
CYBER_BRACKETS = box.Box("┌  ┐\n│  │\n│  │\n│  │\n│  │\n│  │\n│  │\n└  ┘\n")


# 2. Formatting Helpers


def get_slash_indicator(
    is_critical: bool = False,
    is_warning: bool = False,
    split_warning: bool = False,
    split_critical: bool = False,
    unverified_critical: bool = False,
) -> str:

    if unverified_critical:
        return "[critical]/[/critical][muted]/[/muted]"
    if split_critical:
        return "[success]/[/success][critical]/[/critical]"
    if is_critical:
        return "[critical]//[/critical]"
    if split_warning:
        return "[success]/[/success][warning]/[/warning]"
    if is_warning:
        return "[warning]//[/warning]"
    return "[success]//[/success]"


def format_meta_tag(value: str, is_warning: bool = False, is_critical: bool = False) -> str:
    color = "critical" if is_critical else ("warning" if is_warning else "white")
    return f"[{color}]\\[[/{color}] [{color}]{value}[/{color}] [{color}]][/{color}]"


def get_frame_styles(
    error_state: ErrorStateTuple | None, pre_flight_status: str, warning_state: bool
) -> tuple[str, str]:

    if error_state or pre_flight_status == "pre_flight_failed":
        return "#FB0A26", "critical"
    if warning_state:
        return "#FCEE0C", "warning"
    return "#55EAD4", "brand"


def build_divider(semantic_type: str) -> Text:
    terminal_width = min(console.width or 120, 120)
    inner_width = max(terminal_width - 2, 10)
    left_half = inner_width // 2
    right_half = inner_width - left_half

    if semantic_type == "error":
        return Text((">" * left_half) + ("<" * right_half), style="critical")
    if semantic_type == "warning":
        return Text(("<" * left_half) + (">" * right_half), style="warning")
    if semantic_type == "section":
        return Text("-" * inner_width, style="brand")
    return Text(" " * inner_width)


# 3. Phase 1 - Pre-Flight


def build_pre_flight_grid(manifest: ExecutionManifest) -> Table:

    grid = Table.grid(padding=(0, 2), expand=True)
    grid.add_column(style="muted")
    grid.add_column()
    grid.add_column(style="bold white")
    grid.add_column(justify="right")

    grid.add_row(
        "TARGET DATASET",
        get_slash_indicator(is_critical=manifest.dataset_critical_error),
        manifest.dataset_path,
        format_meta_tag(manifest.dataset_meta, is_critical=manifest.dataset_critical_error),
    )
    contract_not_found = manifest.contract_meta == "NOT FOUND"
    grid.add_row(
        "DATA CONTRACT",
        get_slash_indicator(
            unverified_critical=contract_not_found,
            split_critical=manifest.contract_critical and not contract_not_found,
        ),
        manifest.contract_path,
        format_meta_tag(manifest.contract_meta, is_critical=manifest.contract_critical),
    )
    config_not_found = manifest.config_meta == "NOT FOUND"
    grid.add_row(
        "CONFIGURATION",
        get_slash_indicator(
            is_critical=config_not_found,
            split_critical=manifest.config_critical and not config_not_found,
        ),
        manifest.config_source,
        format_meta_tag(manifest.config_meta, is_critical=manifest.config_critical),
    )
    grid.add_row(
        "GLOBAL TARGET",
        get_slash_indicator(),
        manifest.global_target,
        format_meta_tag(manifest.global_target_meta),
    )

    validate_mode_overwrite = manifest.validated_write_mode == "OVERWRITE"
    validated_meta_tags = (
        f"{format_meta_tag(manifest.validated_protocol)} "
        f"{format_meta_tag(manifest.validated_write_mode, is_warning=validate_mode_overwrite)}"
    )
    grid.add_row(
        "VALIDATED PATH",
        get_slash_indicator(split_warning=validate_mode_overwrite),
        manifest.validated_path,
        validated_meta_tags,
    )

    quarantine_mode_overwrite = manifest.quarantine_write_mode == "OVERWRITE"
    quarantine_meta_tags = (
        f"{format_meta_tag(manifest.quarantine_protocol)} "
        f"{format_meta_tag(manifest.quarantine_write_mode, is_warning=quarantine_mode_overwrite)}"
    )
    grid.add_row(
        "QUARANTINE PATH",
        get_slash_indicator(split_warning=quarantine_mode_overwrite),
        manifest.quarantine_path,
        quarantine_meta_tags,
    )

    grid.add_row(
        "ENGINE MODE",
        get_slash_indicator(is_warning=manifest.engine_warning),
        manifest.engine_mode,
        format_meta_tag(manifest.engine_meta, is_warning=manifest.engine_warning),
    )
    return grid


def build_pre_flight_section(
    error_state: ErrorStateTuple | None, pre_flight_status: str, phase: str, grid: Table | None
) -> list[RenderableType]:

    content: list[RenderableType] = []

    header = Text("\n  >> ", style="brand")
    header.append(" PRE FLIGHT CHECKS ", style="brand")

    if error_state:
        header.append("// [ ERROR ]\n", style="critical")
    elif pre_flight_status == "pre_flight_failed":
        header.append("// [ FAILED ]\n", style="critical")
    elif pre_flight_status == "init":
        header.append("// INITIALIZING... █\n", style="white")
    elif phase == "warning_prompt":
        header.append("// [ AWAITING OVERRIDE ]\n", style="warning")
    elif pre_flight_status == "pre_flight_completed":
        header.append("// [ COMPLETED ]\n", style="success")

    content.append(header)
    if grid:
        content.append(Padding(grid, (0, 6, 1, 6)))

    return content


def build_error_section(errors: list[tuple[str, str]]) -> list[RenderableType]:

    content: list[RenderableType] = [build_divider("error")]

    header = Text("\n  >< ", style="critical")
    header.append(" ERROR ", style="critical")
    header.append("// ", style="muted")
    header.append("[ PRE FLIGHT FAILURE ]\n", style="warning")
    content.append(header)

    grid = Table.grid(padding=(0, 2), expand=True)
    grid.add_column(justify="left", no_wrap=True)
    grid.add_column(justify="left", ratio=1, overflow="fold")

    for exception_type, exception_message in errors:
        tag = f"[critical]\\[ {exception_type} ][/critical]"
        message = f"[white]{exception_message}[/white]"
        grid.add_row(tag, message)

    content.append(Padding(grid, (0, 6, 1, 6)))
    return content


def build_warning_section(warnings: list[str], phase: str) -> list[RenderableType]:

    content: list[RenderableType] = [build_divider("warning")]

    header = Text("\n  <> ", style="warning")
    header.append(" WARNING ", style="warning")
    header.append("// ", style="muted")
    header.append("PERFORMANCE & STABILITY RISKS DETECTED\n", style="bold white")
    content.append(header)

    warning_list = [
        Text.from_markup(f"    [warning]\\[ RISK ][/warning] [white]{msg}[/white]")
        for msg in warnings
    ]
    content.append(Padding(Group(*warning_list), (0, 0, 1, 2)))

    if phase == "warning_prompt":
        prompt = Text("\n  >| ", style="brand")
        prompt.append(" USER PROMPT ", style="brand")
        prompt.append("// Proceed with current configuration? [Enter=Yes / n=No] ", style="muted")
        prompt.append("█\n", style="warning")
        content.append(prompt)
    else:
        response = Text("\n  >| ", style="brand")
        response.append(" USER PROMPT ", style="brand")
        response.append("// ", style="muted")
        response.append("[ OVERRIDDEN ]\n", style="warning")
        content.append(response)

    return content


def build_core_dump(error_state: ErrorStateTuple) -> list[RenderableType]:

    content: list[RenderableType] = [build_divider("error")]

    header = Text("\n  >< ", style="critical")
    header.append(" ERROR // ", style="critical")
    header.append("[ CORE DUMP INITIATED ]\n", style="warning")
    content.append(header)

    exception_type, exception_value, exception_traceback = error_state
    traceback_elements: list[RenderableType] = []

    for frame in traceback.extract_tb(exception_traceback):
        markup_line = (
            f"[brand]\\[[/brand] [brand_dim]{frame.filename}[/brand_dim] "
            f"[muted]:[/muted] [accent]{frame.lineno}[/accent] "
            f"[brand]][/brand] [bold white]in {frame.name}[/bold white]"
        )
        traceback_elements.append(Text.from_markup(markup_line))
        if frame.line:
            traceback_elements.append(Text(f"  > {frame.line.strip()}\n", style="muted"))

    exception_name = exception_type.__name__ if exception_type else "Exception"
    msg_markup = f"\n[critical]\\[ {exception_name} ][/critical] [white]{exception_value}[/white]"
    traceback_elements.append(Text.from_markup(msg_markup))

    content.append(Padding(Group(*traceback_elements), (0, 4, 2, 6)))
    return content


# 4. Phase 2 - Engine Execution


def get_thread_cell(
    idx: int, state: str, tick: int, wind_down_start: int, threads_per_tick: int
) -> str:

    if state in ("standby", "sleep") or (
        state == "wind_down" and idx < (tick - wind_down_start) * threads_per_tick
    ):
        return "[inactive]⠿[/inactive]"

    frames = [
        "⠮",
        "⠼",
        "⠫",
        "⠩",
        "⠑",
        "⠄",
        "⠃",
        "⠈",
        "⠡",
        "⠬",
        "⠠",
        "⠤",
        "⠨",
        "⠌",
        "⠴",
        "⠶",
        "⠦",
        "⠱",
        "⠣",
        "⠜",
        "⠹",
    ]
    offset = (idx * 89) % len(frames)
    direction = 1 if (idx % 2) == 0 else -1
    frame = frames[(offset + (tick * direction)) % len(frames)]

    color = "warning" if ((idx * 17 + tick * 13) % 43) < THERMAL_DENSITY else "brand"
    return f"[{color}]{frame}[/{color}]"


def get_core_border(
    state: str, tick: int, wind_down_start: int, total_threads: int, threads_per_tick: int
) -> str:

    if state == "active":
        return "brand"
    if state == "standby":
        return "bold white"
    if state == "sleep":
        return "muted"

    if (tick - wind_down_start) >= TICKS_PER_SECOND or (
        (tick - wind_down_start) * threads_per_tick >= total_threads
    ):
        return "muted"
    return "brand"


def build_vm_core(name: str, state: str, tick: int, wind_down_start: int) -> Panel:

    grid = Table.grid(padding=(0, 0))
    columns, rows = 13, 6  # Odd column count (13 or 11) matches 11-char "{name}_CORE" title
    total_threads = columns * rows
    threads_per_tick = max(1, total_threads // TICKS_PER_SECOND)

    for _ in range(columns):
        grid.add_column()

    for row in range(rows):
        cells = [
            get_thread_cell(row * columns + col, state, tick, wind_down_start, threads_per_tick)
            for col in range(columns)
        ]
        grid.add_row(*cells)

    border = get_core_border(state, tick, wind_down_start, total_threads, threads_per_tick)
    return Panel(
        Padding(grid, (1, 1, 0, 1)),
        title=f"[{border}]{name} CORE[/{border}]",
        border_style=border,
        box=CYBER_BRACKETS,
        expand=False,
    )


def build_engine_cores(phase: str, tick: int, polars_wd: int, duckdb_wd: int) -> Table:

    polars_state, duckdb_state = "active", "standby"
    if phase == "DUCKDB_PASS":
        polars_state = "wind_down" if (tick - polars_wd) < TICKS_PER_SECOND else "sleep"
        duckdb_state = "active"
    elif phase == "ASSEMBLY":
        polars_state = "sleep"
        duckdb_state = "wind_down" if (tick - duckdb_wd) < TICKS_PER_SECOND else "sleep"

    grid = Table.grid(padding=(0, 2))
    grid.add_column()
    grid.add_column()
    grid.add_row(
        build_vm_core("POLARS", polars_state, tick, polars_wd),
        build_vm_core("DUCKDB", duckdb_state, tick, duckdb_wd),
    )
    return grid


def build_performance_metrics(
    telemetry: EngineTelemetry | None, elapsed: float, is_completed: bool
) -> Table:

    tput_val, tput_unit = None, " GB/s"
    if telemetry and telemetry.throughput_gb_s is not None:
        if telemetry.throughput_gb_s < 1.0:
            tput_val, tput_unit = telemetry.throughput_gb_s * 1024, " MB/s"
        else:
            tput_val, tput_unit = telemetry.throughput_gb_s, " GB/s"

    if is_completed and telemetry:
        b_hero, b_ctx = "brand", "bold white"
        lbl_hero = "[brand]ENGINE TIME[/brand]"
        val_time = f"[brand]{telemetry.engine_time:.2f}s[/brand]"
        val_tput = f"[bold white]{tput_val:.2f} {tput_unit.strip()}[/bold white]"
        val_rows = f"[bold white]{telemetry.row_velocity:.2f}M rows/s[/bold white]"
    else:
        b_hero, b_ctx = "muted", "muted"
        lbl_hero = "[white]ENGINE TIME[/white]"
        val_time = f"[bold white]{elapsed:05.2f}s[/bold white]"
        val_tput, val_rows = "[muted]---[/muted]", "[muted]---[/muted]"

    metrics_grid = Table.grid(padding=0, expand=True)
    metrics_grid.add_column(ratio=1)
    metrics_grid.add_column(ratio=2)

    box_left = box.Box("┌─┬┬\n│ ││\n├─┼┼\n│ ││\n├─┼┼\n├─┼┼\n│ ││\n└─┴┴\n")
    tbl_left = Table(
        show_header=False,
        show_lines=False,
        box=box_left,
        border_style=b_hero,
        padding=(0, 2),
        expand=True,
    )
    tbl_left.add_column(justify="center", no_wrap=True)
    tbl_left.add_row(lbl_hero)
    tbl_left.add_row(val_time)

    box_right = box.Box("──┬┐\n  ││\n──┼┤\n  ││\n──┼┤\n──┼┤\n  ││\n──┴┘\n")
    tbl_right = Table(
        show_header=False,
        show_lines=False,
        box=box_right,
        border_style=b_ctx,
        padding=(0, 2),
        expand=True,
    )
    tbl_right.add_column(justify="center", no_wrap=True, ratio=1)
    tbl_right.add_column(justify="center", no_wrap=True, ratio=1)
    tbl_right.add_row("[white]THROUGHPUT[/white]", "[white]ROW VELOCITY[/white]")
    tbl_right.add_row(val_tput, val_rows)

    metrics_grid.add_row(tbl_left, tbl_right)
    return metrics_grid


def build_memory_allocation(
    telemetry: EngineTelemetry | None, is_completed: bool
) -> tuple[Table, Text, Table]:

    ram_val, ram_unit = None, " MB"
    if telemetry and telemetry.peak_ram_mb is not None:
        if telemetry.peak_ram_mb >= ONE_GB:
            ram_val, ram_unit = telemetry.peak_ram_mb / ONE_GB, " GB"
        else:
            ram_val, ram_unit = telemetry.peak_ram_mb, " MB"

    total_gb = (
        (telemetry.total_ram_mb / 1024.0) if telemetry and telemetry.total_ram_mb > 0 else 1.0
    )
    terminal_width = min(console.width or 120, 120)
    bar_width = max(10, terminal_width - 54)

    if is_completed and telemetry and telemetry.peak_ram_mb is not None:
        pct = (telemetry.peak_ram_mb / (total_gb * 1024.0)) * 100
        filled = max(1, int((pct / 100) * bar_width)) if pct > 0 else 0
        empty = bar_width - filled

        color = (
            "brand"
            if pct < RAM_USAGE_WARNING_THRESHOLD
            else "warning"
            if pct < RAM_USAGE_CRITICAL_THRESHOLD
            else "critical"
        )

        bar_str = f"[{color}]{'■' * filled}[/{color}][inactive]{'■' * empty}[/inactive]"
        pct_str = f"{pct:04.1f}%"
        footer_str = f"[{color}]{ram_val:.2f}{ram_unit}[/{color}] / {total_gb:.1f} GB"
    else:
        bar_str = f"[inactive]{'■' * bar_width}[/inactive]"
        pct_str, footer_str = "---%", f"--- / {total_gb:.1f} GB"

    header = Table.grid(expand=True)
    header.add_column(justify="left")
    header.add_column(justify="right")
    title = Text(":: ", style="accent")
    title.append("MEMORY ALLOCATION", style="accent")
    header.add_row(title, f"[bold white][ {pct_str} ][/bold white]")

    footer = Table.grid(expand=True)
    footer.add_column(justify="right")
    footer.add_row(f"[muted]{footer_str}[/muted]")

    ram_usage_bar = Text.from_markup(bar_str)
    ram_usage_bar.no_wrap = True
    ram_usage_bar.overflow = "crop"

    # return header, Text.from_markup(bar_str, overflow="crop"), footer
    return header, ram_usage_bar, footer


def build_execution_section(
    telemetry: EngineTelemetry | None, tick: int, polars_wd: int, duckdb_wd: int
) -> list[RenderableType]:

    content: list[RenderableType] = [build_divider("section")]

    elapsed, phase = 0.0, "BOOT"
    if telemetry and telemetry.start_time > 0:
        elapsed = time.time() - telemetry.start_time
        phase = telemetry.current_phase

    is_completed = telemetry is not None and telemetry.engine_time is not None

    header = Table.grid(expand=True)
    header.add_column(justify="left")
    title = Text("\n  >> ", style="brand")
    title.append(" ENGINE EXECUTION ", style="brand")

    if is_completed:
        title.append("// [ COMPLETED ]\n", style="success")
    elif phase == "BOOT":
        title.append("// [ BOOT ]\n", style="bold white")
    else:
        title.append("// [ RUNNING ]\n", style="warning")

    header.add_row(title)
    content.append(header)

    master = Table.grid(padding=(0, 6), expand=True)
    master.add_column(justify="left")
    master.add_column(justify="left", ratio=1)

    cores_col = build_engine_cores(phase, tick, polars_wd, duckdb_wd)

    # Assembly of Telemetry Column
    telemetry_col = Table.grid(padding=(0, 0), expand=True)
    telemetry_col.add_column(justify="left")
    t_title = Text(":: ", style="accent")
    t_title.append("PERFORMANCE METRICS", style="accent")

    metrics = build_performance_metrics(telemetry, elapsed, is_completed)
    ram_head, ram_bar, ram_foot = build_memory_allocation(telemetry, is_completed)

    telemetry_col.add_row(t_title)
    telemetry_col.add_row(metrics)
    telemetry_col.add_row(Text(""))
    telemetry_col.add_row(ram_head)
    telemetry_col.add_row(ram_bar)
    telemetry_col.add_row(ram_foot)

    master.add_row(cores_col, telemetry_col)
    content.append(Padding(master, (0, 6, 0, 6)))
    content.append(Text(""))

    return content


# 5. Phase 3 - Validation Report


def _resolve_card_header_style(rule: RuleDiagnostic) -> tuple[str, str]:
    """Resolves border color and title markup for a diagnostic card."""

    target = rule["target"]
    status = rule["status"]

    if status == "UNVALIDATED":
        border_color = "#878787"
        return border_color, f"[bold black on {border_color}] {target} [/]"
    if status == "PASSED":
        border_color = "success_dim"
        return border_color, f"[bold {border_color}] {target} [/]"

    border_color = "#FB0A26" if rule["severity"] == "CRITICAL" else "#FCEE0C"
    return border_color, f"[bold black on {border_color}] {target} [/]"


def _build_global_card_content(
    status: str, border_color: str, error_context: dict[str, object]
) -> RenderableType:
    """Builds the inner content for a Global SQL diagnostic card."""

    query_string = str(error_context.get("query", ""))
    actual_value = error_context.get("actual_value")
    error_message = error_context.get("error_message")

    card_header = Table.grid(expand=True)
    card_header.add_column(justify="left", ratio=1)
    card_header.add_column(justify="right", no_wrap=True)

    if status == "PASSED":
        card_header.add_row(f"[{border_color}]PASS[/]", "")
    else:
        if isinstance(actual_value, (int, float)) and not error_message:
            display_value = int(actual_value) if float(actual_value).is_integer() else actual_value
            failed_row_count = f"[white]{display_value:,}[/]"
        else:
            failed_row_count = "[critical]ERROR[/critical]"

        card_header.add_row(f"[{border_color}]FAIL[/]", failed_row_count)

    sql_rule = " ".join(query_string.split())
    if len(sql_rule) > SQL_RULE_DISPLAY_LENGTH:
        sql_rule = sql_rule[:118] + "..."

    sql_snippet = Syntax(
        sql_rule, "sql", theme="monokai", background_color="default", word_wrap=True
    )

    return Group(
        Padding(card_header, (1, 0, 0, 2)),
        Padding(sql_snippet, (1, 0, 0, 2)),
    )


def _format_rule_display_name(raw_rule_name: str) -> str:
    """Formats and escapes brackets in a column rule name for Rich markup."""

    if ": " in raw_rule_name:
        key, val = raw_rule_name.split(": ", 1)
        val_escaped = val.replace("[", "\\[").replace("]", "\\]")
        return f"{key}: [bold white]{val_escaped}[/]"
    return raw_rule_name.replace("[", "\\[").replace("]", "\\]")


def _build_column_card_content(status: str, border_color: str, err_ctx: object) -> RenderableType:
    """Builds the inner tree grid for a Column-level diagnostic card."""

    inner_grid = Table.grid(padding=(0, 1), expand=True)
    inner_grid.add_column(justify="left", no_wrap=True)
    inner_grid.add_column(justify="left", ratio=1, overflow="fold")
    inner_grid.add_column(justify="right", no_wrap=True)

    if status == "UNVALIDATED":
        inner_grid.add_row("", "", "")
        inner_grid.add_row("", "[muted] UNVALIDATED[/muted]", "")
        return inner_grid

    if isinstance(err_ctx, str):
        inner_grid.add_row(f"[{border_color}]  │[/]", "", "")
        inner_grid.add_row("[success_dim]  └─[/]", f"[success_dim]{err_ctx}[/]", "")
        return inner_grid

    if isinstance(err_ctx, list):
        inner_grid.add_row(f"[{border_color}]  │[/]", "", "")
        sorted_rules = sorted(
            err_ctx,
            key=lambda r: (
                r.get("failed_rows", 0) == 0,
                str(r.get("rule_name", "")).lower(),
            ),
        )

        total_rules = len(sorted_rules)
        for idx, err in enumerate(sorted_rules):
            branch = "  └─" if idx == total_rules - 1 else "  ├─"
            display_name = _format_rule_display_name(str(err["rule_name"]))
            failed_rows = int(err["failed_rows"])
            message_color = border_color if failed_rows > 0 else "success_dim"
            outcome_str = (
                f"[white]{failed_rows:,}[/]" if failed_rows > 0 else "[success_dim]PASS[/]"
            )
            inner_grid.add_row(
                f"[{message_color}]{branch}[/]",
                f"[{message_color}]{display_name}[/]",
                outcome_str,
            )

    return inner_grid


def _build_diagnostic_matrix(title: str, rules: list[RuleDiagnostic]) -> list[RenderableType]:
    """Builds the column and global level validation report."""

    if not rules:
        return []

    passed = sum(1 for r in rules if r["status"] == "PASSED")
    failed = sum(1 for r in rules if r["status"] == "FAILED")
    unvalidated = sum(1 for r in rules if r["status"] == "UNVALIDATED")

    total = len(rules)
    entity = "COLUMNS" if "COLUMN" in title else "RULES"

    passed_tag = (
        f"[success]{passed} PASSED[/success]   "
        if passed > 0
        else f"[muted]{passed} PASSED[/muted]   "
    )
    failed_tag = (
        f"[critical]{failed} FAILED[/critical]" if failed > 0 else f"[muted]{failed} FAILED[/muted]"
    )
    summary = f"[brand]\\[ {total} {entity}:[/brand] {passed_tag}{failed_tag}"

    if "COLUMN" in title:
        summary += f"   [muted]{unvalidated} UNVALIDATED[/muted]"
    summary += " [brand]][/brand]"

    header_grid = Table.grid(expand=True)
    header_grid.add_column(justify="left", ratio=1)
    header_grid.add_column(justify="right")
    header_grid.add_row(f"[accent]:: {title}[/accent]", summary)

    cards: list[RenderableType] = []

    for rule in rules:
        status = rule["status"]
        error_context = rule["error_context"]
        border_color, title_markup = _resolve_card_header_style(rule)

        card_content: RenderableType = (
            _build_global_card_content(status, border_color, error_context)
            if isinstance(error_context, dict)
            else _build_column_card_content(status, border_color, error_context)
        )

        cards.append(
            Panel(
                card_content,
                title=title_markup,
                title_align="left",
                border_style=border_color,
                padding=(0, 1),
                box=box.SQUARE,
                expand=True,
            )
        )

    inner_width = min(console.width or 120, 120) - 12
    min_card_width = 32 if "COLUMN" in title else 54
    num_cols = max(1, inner_width // min_card_width)

    masonry = Table.grid(padding=(1, 2), expand=True)
    for _ in range(num_cols):
        masonry.add_column(ratio=1)

    for i in range(0, len(cards), num_cols):
        row_cards: list[RenderableType] = list(cards[i : i + num_cols])
        while len(row_cards) < num_cols:
            row_cards.append(Text(""))
        masonry.add_row(*row_cards)

    return [
        Padding(header_grid, (0, 6, 2, 6)),
        Padding(masonry, (0, 6, 2, 6)),
    ]


def _build_volume_tree(total: int, valid: int, quarantined_rows: int) -> list[RenderableType]:
    """Constructs the centered T-junction volume analysis schematic."""

    usable_width = min(console.width or 120, 120) - 2
    volume_tree_width = min(70, max(20, usable_width - 12))
    center_pad = (usable_width - volume_tree_width) // 2

    center_grid = Table.grid(expand=True)
    center_grid.width = volume_tree_width
    center_grid.add_column(justify="center", ratio=1)
    center_grid.add_row("[white]TOTAL ROWS[/white]")
    center_grid.add_row(f"[bold white][ {total:,} ][/bold white]")
    elements: list[RenderableType] = [Padding(center_grid, (0, center_pad))]

    left_indent, right_indent = 4, 5
    center_idx = volume_tree_width // 2
    left_arm_len = center_idx - left_indent - 1
    right_arm_len = (volume_tree_width - right_indent - 1) - center_idx - 1

    stems = (
        f"[brand]{' ' * center_idx}│[/brand]",
        f"[brand]{' ' * left_indent}┌{'─' * left_arm_len}┴{'─' * right_arm_len}┐[/brand]",
        f"[brand]{' ' * left_indent}│{' ' * (left_arm_len + 1 + right_arm_len)}│[/brand]",
    )
    for markup in stems:
        line_text = Text.from_markup(markup)
        line_text.no_wrap = True
        line_text.overflow = "crop"
        elements.append(Padding(line_text, (0, center_pad)))

    label_grid = Table.grid(expand=True)
    label_grid.width = volume_tree_width
    label_grid.add_column(justify="left", ratio=1)
    label_grid.add_column(justify="right", ratio=1)

    percentage_grid = Table.grid(expand=True)
    percentage_grid.width = volume_tree_width
    percentage_grid.add_column(justify="left", ratio=1)
    percentage_grid.add_column(justify="right", ratio=1)

    if quarantined_rows == 0:
        label_grid.add_row("[success]VALIDATED[/success]", "[muted]QUARANTINED[/muted]")
        label_grid.add_row(f"[success][ {valid:,} ][/success]", "[muted][ 0 ][/muted]")
        bar_string = f"\n[success_dim]{'■' * volume_tree_width}[/]"
        percentage_grid.add_row("[success][ 100.00% ][/success]", "[muted][ 0.00% ][/muted]")
    else:
        label_grid.add_row("[success_dim]VALIDATED[/]", "[critical]QUARANTINED[/critical]")
        label_grid.add_row(
            f"[success_dim][ {valid:,} ][/]", f"[critical][ {quarantined_rows:,} ][/critical]"
        )

        quarantined_pct = (quarantined_rows / total) * 100 if total > 0 else 0.0
        valid_pct = 100.0 - quarantined_pct if total > 0 else 0.0
        empty = max(1, int((quarantined_pct / 100) * volume_tree_width))
        filled = volume_tree_width - empty

        bar_string = f"\n[success_dim]{'■' * filled}[/][critical]{'■' * empty}[/critical]"
        percentage_grid.add_row(
            f"[success_dim][ {valid_pct:0.2f}% ][/]",
            f"[critical][ {quarantined_pct:0.2f}% ][/critical]",
        )

    yield_bar = Text.from_markup(bar_string)
    yield_bar.no_wrap = True
    yield_bar.overflow = "crop"

    elements.extend(
        (
            Padding(label_grid, (0, center_pad)),
            Padding(yield_bar, (0, center_pad)),
            Padding(percentage_grid, (0, center_pad)),
        )
    )
    return elements


def build_report_section(report: ValidationReport) -> list[RenderableType]:
    """Constructs the master report payload and volume analysis T-Junction."""

    content: list[RenderableType] = [build_divider("section")]

    header = Text("\n  >> ", style="brand")
    header.append(" VALIDATION REPORT ", style="brand")
    header.append("// [ GENERATED ]\n", style="success")
    content.append(header)

    dataset = report["dataset"]
    volume_analysis_title = Text(":: VOLUME ANALYSIS\n", style="accent")
    content.append(Padding(volume_analysis_title, (0, 6)))

    content.extend(
        _build_volume_tree(
            total=dataset["total_rows_evaluated"],
            valid=dataset["valid_rows"],
            quarantined_rows=dataset["quarantined_rows"],
        )
    )
    content.append(Text("\n\n"))

    content.extend(_build_diagnostic_matrix("COLUMN DIAGNOSTICS", report["column_level_report"]))
    content.append(Text("\n"))
    content.extend(_build_diagnostic_matrix("GLOBAL DIAGNOSTICS", report["global_level_report"]))

    return content


def build_pipeline_section(report: ValidationReport) -> list[RenderableType]:
    """Generates the final orchestration phase and Circuit Breaker pipeline."""

    content: list[RenderableType] = [build_divider("section")]

    header = Text("\n  >> ", style="brand")
    header.append(" PIPELINE STATUS \n", style="brand")
    content.append(header)

    ctx = report["_meta"]["pipeline_context"]
    mode_active = ctx["quality_gate_active"] if ctx else False
    warn_active = ctx["fail_on_warnings"] if ctx else False
    crit_active = ctx["fail_on_critical"] if ctx else False
    status = ctx["status"] if ctx else "PASSIVE"
    exit_code = ctx["exit_code"] if ctx else 0

    mode_color = "success" if mode_active else "bold white"
    warn_col = "success" if warn_active else "muted"
    crit_col = "success" if crit_active else "muted"

    # 1. The Logic Gate Schematic (Perfect 48-character symmetrical block)
    row1 = f"[{warn_col}]┌─\\[ FAIL ON WARNINGS ][/]    [{crit_col}]\\[ FAIL ON CRITICAL ]─┐[/]"
    row2 = f"[{warn_col}]│[/]{' ' * 46}[{crit_col}]│[/]"
    row3 = row2
    row4 = row2
    row5 = (
        f"[{warn_col}]│[/]{' ' * 15}[{warn_col}]┌[/] "
        f"[brand]QUALITY GATE[/] [{crit_col}]┐[/]{' ' * 15}[{crit_col}]│[/]"
    )
    row6 = (
        f"[{warn_col}]│[/]{' ' * 15}[{warn_col}]│[/]"
        f"{' ' * 14}[{crit_col}]│[/]{' ' * 15}[{crit_col}]│[/]"
    )

    center_text = "    ACTIVE    " if mode_active else "   INACTIVE   "
    row7 = f"[{warn_col}]└{'─' * 15}┤[/][{mode_color}]{center_text}[/][{crit_col}]├{'─' * 15}┘[/]"
    row8 = f"[{warn_col}]│[/]{' ' * 14}[{crit_col}]│[/]{' ' * 15}"
    row9 = f"[{warn_col}]└[/]{' ' * 14}[{crit_col}]┘[/]{' ' * 16}"

    schematic_text = Text.from_markup(
        f"{row1}\n{row2}\n{row3}\n{row4}\n{row5}\n{row6}\n{row7}\n{row8}\n{row9}"
    )
    schematic_layout = Table.grid(expand=True)
    schematic_layout.add_column(justify="center")
    schematic_layout.add_row(schematic_text)

    content.append(Padding(schematic_layout, (0, 0, 1, 0)))

    # 2. The Physical Circuit Breaker
    if status == "PASSED":
        pipe_color = "success"
        status_text = "HEALTHY"
    elif status == "FAILED":
        pipe_color = "critical"
        status_text = "HALTED"
    else:
        pipe_color = "muted"
        status_text = "INACTIVE"

    terminal_width = min(console.width or 120, 120)
    inner_width = max(terminal_width - 12, 20)

    tag = f"[ {status_text} // EXIT_CODE: {exit_code} ]"
    remaining_space = max(0, inner_width - len(tag))
    left_dashes = remaining_space // 2
    right_dashes = remaining_space - left_dashes

    pipe_str = f"[{pipe_color}]{'━' * left_dashes}{tag}{'━' * right_dashes}[/]"
    pipe_text = Text.from_markup(pipe_str)
    pipe_text.no_wrap = True
    pipe_text.overflow = "crop"

    content.append(Padding(pipe_text, (0, 6, 1, 6)))

    return content
