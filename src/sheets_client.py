from __future__ import annotations

from pathlib import Path

import gspread
from gspread.cell import Cell
from gspread.exceptions import WorksheetNotFound
from gspread.spreadsheet import Spreadsheet
from gspread.worksheet import Worksheet


def _header_index_map(headers: list[str]) -> dict[str, int]:
    """Build a 1-based column index map from stripped header names."""
    index_map: dict[str, int] = {}
    duplicates: list[str] = []

    for col, raw_header in enumerate(headers, start=1):
        header = raw_header.strip()
        if not header:
            continue
        if header in index_map:
            duplicates.append(header)
            continue
        index_map[header] = col

    if duplicates:
        dupes = ", ".join(sorted(set(duplicates)))
        raise ValueError(f"Duplicate header names after trimming whitespace: {dupes}")

    return index_map


def _require_headers(headers: list[str], required: list[str]) -> dict[str, int]:
    index_map = _header_index_map(headers)
    missing = [name for name in required if name not in index_map]
    if missing:
        raise ValueError(f"Missing required sheet headers: {', '.join(missing)}")
    return index_map


def open_sheet(sheet_id: str, worksheet_name: str, service_account_json: str) -> Worksheet:
    """Open a worksheet from a Google Sheet using a service account JSON file."""
    creds_path = Path(service_account_json).expanduser()
    if not creds_path.exists():
        raise FileNotFoundError(f"Service account JSON not found: {creds_path}")

    try:
        client = gspread.service_account(filename=str(creds_path))
    except Exception as exc:
        raise RuntimeError(f"Failed to authenticate with service account JSON: {exc}") from exc

    try:
        return client.open_by_key(sheet_id).worksheet(worksheet_name)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to open worksheet '{worksheet_name}' in sheet '{sheet_id}': {exc}"
        ) from exc


def open_spreadsheet(sheet_id: str, service_account_json: str) -> Spreadsheet:
    """Open a spreadsheet by ID using a service account JSON file."""
    creds_path = Path(service_account_json).expanduser()
    if not creds_path.exists():
        raise FileNotFoundError(f"Service account JSON not found: {creds_path}")

    try:
        client = gspread.service_account(filename=str(creds_path))
    except Exception as exc:
        raise RuntimeError(f"Failed to authenticate with service account JSON: {exc}") from exc

    try:
        return client.open_by_key(sheet_id)
    except Exception as exc:
        raise RuntimeError(f"Failed to open sheet '{sheet_id}': {exc}") from exc


def get_headers(ws: Worksheet) -> list[str]:
    """Read row 1 headers and normalize whitespace around header names."""
    return [value.strip() for value in ws.row_values(1)]


def find_first_row_to_evaluate(ws: Worksheet, headers: list[str]) -> int | None:
    """
    Return first 1-based row index where EVALUATE=YES and Decision is blank.
    Returns None when no eligible row exists.
    """
    index_map = _require_headers(headers, ["EVALUATE", "Decision"])
    evaluate_col = index_map["EVALUATE"] - 1
    decision_col = index_map["Decision"] - 1

    rows = ws.get_all_values()
    if len(rows) <= 1:
        return None

    for row_number, row_values in enumerate(rows[1:], start=2):
        evaluate_value = row_values[evaluate_col].strip().upper() if evaluate_col < len(row_values) else ""
        decision_value = row_values[decision_col].strip() if decision_col < len(row_values) else ""
        if evaluate_value == "YES" and decision_value == "":
            return row_number

    return None


def read_row_as_dict(ws: Worksheet, headers: list[str], row: int) -> dict:
    """Read a row into a dict keyed by stripped header names."""
    if row < 1:
        raise ValueError(f"Row number must be >= 1, got: {row}")

    row_values = ws.row_values(row)
    padded = row_values + [""] * max(0, len(headers) - len(row_values))
    return {header: padded[i] if i < len(padded) else "" for i, header in enumerate(headers)}


def write_row_fields(ws: Worksheet, headers: list[str], row: int, updates: dict[str, str]) -> None:
    """Write values to a row by header name."""
    if row < 1:
        raise ValueError(f"Row number must be >= 1, got: {row}")
    if not updates:
        return

    index_map = _header_index_map(headers)
    missing = [field for field in updates if field not in index_map]
    if missing:
        raise ValueError(f"Cannot write unknown header(s): {', '.join(missing)}")

    cells: list[Cell] = []
    for field, value in updates.items():
        col = index_map[field]
        cells.append(Cell(row=row, col=col, value="" if value is None else str(value)))

    cells.sort(key=lambda cell: cell.col)
    ws.update_cells(cells, value_input_option="USER_ENTERED")


def get_or_create_worksheet(
    spreadsheet: Spreadsheet,
    worksheet_name: str,
    required_headers: list[str],
    default_rows: int = 1000,
    default_cols: int = 40,
) -> Worksheet:
    """
    Open worksheet by name or create it if missing.
    Ensures required headers exist in row 1 (missing headers are appended).
    """
    try:
        ws = spreadsheet.worksheet(worksheet_name)
    except WorksheetNotFound:
        ws = spreadsheet.add_worksheet(
            title=worksheet_name,
            rows=str(max(default_rows, 2)),
            cols=str(max(default_cols, len(required_headers) + 5)),
        )
        ws.update(
            range_name="A1",
            values=[required_headers],
            value_input_option="RAW",
        )
        return ws

    headers = get_headers(ws)
    if not any(headers):
        ws.update(
            range_name="A1",
            values=[required_headers],
            value_input_option="RAW",
        )
        return ws

    existing = [h for h in headers if h]
    missing = [h for h in required_headers if h not in existing]
    if missing:
        ws.update(
            range_name="A1",
            values=[existing + missing],
            value_input_option="RAW",
        )
    return ws


def append_row_by_headers(ws: Worksheet, row_data: dict[str, str]) -> None:
    """
    Append a row by worksheet header names.
    Unknown keys are ignored.
    """
    headers = get_headers(ws)
    if not headers:
        raise ValueError(f"Worksheet '{ws.title}' has no header row.")

    values: list[str] = []
    for header in headers:
        values.append(str(row_data.get(header, "")))
    ws.append_row(values, value_input_option="USER_ENTERED")
