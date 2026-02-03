#!/usr/bin/env python3
"""
Dynamic Test Runner - Executes all test_*.py scripts in the scripts/ folder

Usage:
    poetry run python -m scripts.run_all_tests [options]

Options:
    --verbose, -v       Show full output from each test
    --stop-on-fail      Stop execution on first failure
    --filter PATTERN    Only run tests matching pattern (e.g., --filter categories)
    --list              List available tests without running them
    --parallel          Run tests in parallel (experimental)
"""

import sys
import subprocess
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional


@dataclass
class TestResult:
    """Result of a single test execution"""
    name: str
    success: bool
    duration_ms: int
    output: str
    error: Optional[str] = None


def discover_tests(scripts_dir: Path, filter_pattern: Optional[str] = None) -> list[Path]:
    """
    Discover all test_*.py files in the scripts directory.

    Args:
        scripts_dir: Path to scripts directory
        filter_pattern: Optional pattern to filter test names

    Returns:
        List of test file paths
    """
    tests = sorted(scripts_dir.glob("test_*.py"))

    if filter_pattern:
        tests = [t for t in tests if filter_pattern.lower() in t.stem.lower()]

    return tests


def run_single_test(test_path: Path, verbose: bool = False) -> TestResult:
    """
    Execute a single test script in an isolated subprocess.

    Args:
        test_path: Path to the test file
        verbose: Whether to show output in real-time

    Returns:
        TestResult with execution details
    """
    import os

    test_name = test_path.stem
    start_time = time.time()

    try:
        # Run the test as a module with isolated environment
        module_name = f"scripts.{test_name}"

        # Create isolated environment to prevent state sharing between tests
        env = {
            **os.environ,
            # Force fresh imports by disabling pycache
            'PYTHONDONTWRITEBYTECODE': '1',
            # Disable hash randomization for consistent behavior
            'PYTHONHASHSEED': '0',
            # Force UTF-8 encoding for subprocess output (fixes Windows cp1252 issues)
            'PYTHONIOENCODING': 'utf-8',
        }

        result = subprocess.run(
            [sys.executable, "-B", "-u", "-m", module_name],
            capture_output=not verbose,
            text=True,
            encoding='utf-8',
            errors='replace',  # Replace unencodable chars instead of failing
            cwd=test_path.parent.parent,  # Project root
            timeout=300,  # 5 minute timeout per test
            env=env,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        output = ""
        if not verbose:
            output = result.stdout or ""
            if result.stderr:
                output += f"\n[STDERR]\n{result.stderr}"

        return TestResult(
            name=test_name,
            success=result.returncode == 0,
            duration_ms=duration_ms,
            output=output,
            error=result.stderr if result.returncode != 0 else None,
        )

    except subprocess.TimeoutExpired:
        duration_ms = int((time.time() - start_time) * 1000)
        return TestResult(
            name=test_name,
            success=False,
            duration_ms=duration_ms,
            output="",
            error="Test timed out after 5 minutes",
        )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        return TestResult(
            name=test_name,
            success=False,
            duration_ms=duration_ms,
            output="",
            error=str(e),
        )


def print_header():
    """Print test runner header"""
    print()
    print("=" * 70)
    print("  POLYMARKET INSIDER DETECTION - TEST RUNNER")
    print("=" * 70)
    print()


def print_test_list(tests: list[Path]):
    """Print list of discovered tests"""
    print(f"Discovered {len(tests)} test(s):\n")
    for i, test in enumerate(tests, 1):
        print(f"  {i}. {test.stem}")
    print()


def print_result(result: TestResult, index: int, total: int, verbose: bool = False):
    """Print result of a single test"""
    status = "\u2705" if result.success else "\u274c"
    duration = f"{result.duration_ms}ms"

    print(f"  [{index}/{total}] {status} {result.name} ({duration})")

    if not result.success and result.error:
        # Show last few lines of error (usually the most relevant - the actual exception)
        error_lines = result.error.strip().split('\n')
        # Show last 10 lines which typically contain the actual error
        if len(error_lines) > 10:
            print("         ...")
        relevant_lines = error_lines[-10:]
        for line in relevant_lines:
            print(f"         {line}")

    if verbose and result.output:
        print("-" * 50)
        print(result.output)
        print("-" * 50)


def print_summary(results: list[TestResult], total_duration_ms: int):
    """Print test execution summary"""
    passed = sum(1 for r in results if r.success)
    failed = len(results) - passed

    print()
    print("=" * 70)
    print("  SUMMARY")
    print("=" * 70)
    print()
    print(f"  Total:    {len(results)} test(s)")
    print(f"  Passed:   {passed} \u2705")
    print(f"  Failed:   {failed} \u274c")
    print(f"  Duration: {total_duration_ms}ms")
    print()

    if failed > 0:
        print("  Failed tests:")
        for r in results:
            if not r.success:
                print(f"    - {r.name}")
        print()

    if failed == 0:
        print("  \u2705 ALL TESTS PASSED!")
    else:
        print(f"  \u274c {failed} TEST(S) FAILED")

    print()
    print("=" * 70)
    print()


def run_tests_sequential(
    tests: list[Path],
    verbose: bool = False,
    stop_on_fail: bool = False
) -> list[TestResult]:
    """Run tests sequentially"""
    results = []

    for i, test_path in enumerate(tests, 1):
        result = run_single_test(test_path, verbose)
        results.append(result)
        print_result(result, i, len(tests), verbose)

        if stop_on_fail and not result.success:
            print("\n  Stopping on first failure (--stop-on-fail)")
            break

    return results


def run_tests_parallel(tests: list[Path], verbose: bool = False) -> list[TestResult]:
    """Run tests in parallel using ThreadPoolExecutor"""
    results = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_test = {
            executor.submit(run_single_test, test, verbose): test
            for test in tests
        }

        for i, future in enumerate(as_completed(future_to_test), 1):
            result = future.result()
            results.append(result)
            print_result(result, i, len(tests), verbose)

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Run all test scripts in the scripts/ folder"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show full output from each test"
    )
    parser.add_argument(
        "--stop-on-fail",
        action="store_true",
        help="Stop execution on first failure"
    )
    parser.add_argument(
        "--filter",
        type=str,
        default=None,
        help="Only run tests matching pattern"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available tests without running them"
    )
    parser.add_argument(
        "--parallel",
        action="store_true",
        help="Run tests in parallel (experimental)"
    )

    args = parser.parse_args()

    # Find scripts directory
    scripts_dir = Path(__file__).parent

    # Discover tests
    tests = discover_tests(scripts_dir, args.filter)

    if not tests:
        print("\nNo tests found matching criteria.\n")
        return 1

    print_header()

    # List mode - just show tests
    if args.list:
        print_test_list(tests)
        return 0

    # Run tests
    print(f"Running {len(tests)} test(s)...\n")

    start_time = time.time()

    if args.parallel and not args.stop_on_fail:
        results = run_tests_parallel(tests, args.verbose)
    else:
        results = run_tests_sequential(tests, args.verbose, args.stop_on_fail)

    total_duration_ms = int((time.time() - start_time) * 1000)

    # Print summary
    print_summary(results, total_duration_ms)

    # Return exit code
    failed = sum(1 for r in results if not r.success)
    return 1 if failed > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
