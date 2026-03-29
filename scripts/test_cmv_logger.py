# Test script for CMV CSV debug logger
# Validates that _append_csv writes correct headers and rows,
# handles multiple appends, and produces valid CSV files.

import csv
import sys
import tempfile
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from workers.tasks import (
    _append_csv,
    _CMV_INPUT_HEADERS,
    _CMV_OUTPUT_HEADERS,
)

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = ""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  [PASS] {name}")
    else:
        FAIL += 1
        msg = f"  [FAIL] {name}"
        if detail:
            msg += f" — {detail}"
        print(msg)


# ---------------------------------------------------------------------------
# Test 1: Input CSV — headers written on first call, not duplicated on second
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("TEST 1: Input CSV — headers + append behavior")
print("=" * 60)

with tempfile.TemporaryDirectory() as tmp:
    csv_path = Path(tmp) / "cmv_inputs.csv"

    now = datetime.utcnow().isoformat()
    row1 = [
        now, "0xMARKET1", "0xWALLET_A",
        "2026-03-17T21:14:00", "Yes", "0.65", "100.0", "BUY",
        "2026-03-17T22:00:00", "5000.0", "Yes", "2025-06-01T00:00:00",
    ]
    row2 = [
        now, "0xMARKET1", "0xWALLET_B",
        "2026-03-17T21:14:30", "No", "0.35", "200.0", "SELL",
        "2026-03-17T22:00:00", "5000.0", "Yes", "2025-01-15T12:00:00",
    ]

    # First write — should create file with header + 1 row
    _append_csv(csv_path, _CMV_INPUT_HEADERS, [row1])
    check("File created after first write", csv_path.exists())

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        lines = f.readlines()
    check("First write has 2 lines (header + 1 row)", len(lines) == 2,
          f"got {len(lines)}")
    check("Header matches _CMV_INPUT_HEADERS",
          lines[0].strip() == ",".join(_CMV_INPUT_HEADERS))

    # Second write — should append without duplicating header
    _append_csv(csv_path, _CMV_INPUT_HEADERS, [row2])

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        lines = f.readlines()
    check("After second write has 3 lines (header + 2 rows)", len(lines) == 3,
          f"got {len(lines)}")

    # Parse with csv.reader to validate structure
    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        parsed = list(reader)
    check("CSV parseable with csv.reader", len(parsed) == 3)
    check("Header row has correct column count",
          len(parsed[0]) == len(_CMV_INPUT_HEADERS),
          f"expected {len(_CMV_INPUT_HEADERS)}, got {len(parsed[0])}")
    check("Data row 1 has correct column count",
          len(parsed[1]) == len(_CMV_INPUT_HEADERS),
          f"expected {len(_CMV_INPUT_HEADERS)}, got {len(parsed[1])}")
    check("Data row 2 wallet_id is correct",
          parsed[2][2] == "0xWALLET_B",
          f"got {parsed[2][2]}")


# ---------------------------------------------------------------------------
# Test 2: Output CSV — headers and values
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("TEST 2: Output CSV — headers + numeric values")
print("=" * 60)

with tempfile.TemporaryDirectory() as tmp:
    csv_path = Path(tmp) / "cmv_outputs.csv"

    now = datetime.utcnow().isoformat()
    rows = [
        [now, "0xMARKET1", "0xWALLET_A", "0.0386", "2.0288", "-4.9205", "0.000577", "0.0"],
        [now, "0xMARKET1", "0xWALLET_B", "0.0125", "38.8615", "-5.3486", "0.000365", "1.3863"],
    ]

    _append_csv(csv_path, _CMV_OUTPUT_HEADERS, rows)

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        parsed = list(reader)

    check("Output CSV has 2 data rows", len(parsed) == 2, f"got {len(parsed)}")
    check("DictReader keys match output headers",
          list(parsed[0].keys()) == _CMV_OUTPUT_HEADERS,
          f"got {list(parsed[0].keys())}")
    check("delta_t value preserved", parsed[0]["delta_t"] == "0.0386",
          f"got {parsed[0]['delta_t']}")
    check("entropy value preserved", parsed[1]["entropy"] == "1.3863",
          f"got {parsed[1]['entropy']}")
    check("wallet_id preserved", parsed[0]["wallet_id"] == "0xWALLET_A")


# ---------------------------------------------------------------------------
# Test 3: Empty file — header written on first append
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("TEST 3: Empty existing file gets header")
print("=" * 60)

with tempfile.TemporaryDirectory() as tmp:
    csv_path = Path(tmp) / "cmv_outputs.csv"

    # Create empty file (simulates truncated/cleared log)
    csv_path.touch()
    check("Empty file exists", csv_path.exists() and csv_path.stat().st_size == 0)

    now = datetime.utcnow().isoformat()
    _append_csv(csv_path, _CMV_OUTPUT_HEADERS,
                [[now, "0xM", "0xW", "1.0", "2.0", "3.0", "0.001", "0.5"]])

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        lines = f.readlines()
    check("Header written to previously-empty file", len(lines) == 2,
          f"got {len(lines)}")
    check("Header is correct", lines[0].strip() == ",".join(_CMV_OUTPUT_HEADERS))


# ---------------------------------------------------------------------------
# Test 4: Multiple rows in a single call
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("TEST 4: Batch write — multiple rows in one call")
print("=" * 60)

with tempfile.TemporaryDirectory() as tmp:
    csv_path = Path(tmp) / "cmv_inputs.csv"

    now = datetime.utcnow().isoformat()
    batch = []
    for i in range(50):
        batch.append([
            now, f"0xMARKET_{i}", f"0xWALLET_{i}",
            "2026-03-17T21:14:00", "Yes", "0.65", str(100.0 + i), "BUY",
            "2026-03-17T22:00:00", "5000.0", "Yes", "2025-06-01T00:00:00",
        ])

    _append_csv(csv_path, _CMV_INPUT_HEADERS, batch)

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        parsed = list(reader)

    check("Batch of 50 rows written", len(parsed) == 51,  # header + 50
          f"got {len(parsed)}")
    check("All rows have correct column count",
          all(len(r) == len(_CMV_INPUT_HEADERS) for r in parsed))
    check("Last row wallet_id correct", parsed[-1][2] == "0xWALLET_49",
          f"got {parsed[-1][2]}")


# ---------------------------------------------------------------------------
# Test 5: Column count consistency between headers and data
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("TEST 5: Input/Output header counts match expected")
print("=" * 60)

check("Input headers have 12 columns", len(_CMV_INPUT_HEADERS) == 12,
      f"got {len(_CMV_INPUT_HEADERS)}")
check("Output headers have 8 columns", len(_CMV_OUTPUT_HEADERS) == 8,
      f"got {len(_CMV_OUTPUT_HEADERS)}")
check("Input headers contain 'trade_timestamp'", "trade_timestamp" in _CMV_INPUT_HEADERS)
check("Input headers contain 'first_seen_at'", "first_seen_at" in _CMV_INPUT_HEADERS)
check("Output headers contain 'alpha_slippage'", "alpha_slippage" in _CMV_OUTPUT_HEADERS)
check("Output headers contain 'entropy'", "entropy" in _CMV_OUTPUT_HEADERS)


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
TOTAL = PASS + FAIL
print(f"RESULTS: {PASS}/{TOTAL} passed, {FAIL} failed")
print("=" * 60)

if FAIL > 0:
    sys.exit(1)
