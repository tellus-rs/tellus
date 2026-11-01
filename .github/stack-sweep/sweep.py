"""Run a bench across worker-stack shifts, or summarize one job's results.

run:     sweep.py run [--bench messaging|persistence] --preload SO --workdir DIR --out FILE B=BIN C=BIN A=BIN
summary: sweep.py summary FILE
lines:   sweep.py lines [--bench messaging|persistence], the number of result lines of a full run
"""

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time

BENCH_ROWS = {
    "messaging": [
        "flood/unbounded",
        "flood/bounded",
        "ping_pong/pairs/1",
        "ping_pong/pairs/4",
        "fan_out/workers/4",
        "fan_out/workers/16",
        "queued/never/4",
        "queued/never/16",
        "queued/every/4",
        "queued/every/16",
        "timers/none/4",
        "timers/none/16",
        "timers/armed_idle/4",
        "timers/armed_idle/16",
        "timers/restart_each/4",
        "timers/restart_each/16",
    ],
    "persistence": [
        "persist/no_snapshots",
        "persist/snapshots",
        "recover/replay",
        "recover/snapshot",
    ],
}
SHIFTS = list(range(0, 4096, 256))
REPETITIONS = 3
ORDER = ["B", "C", "A"]
WORKER_THREADS = 4


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run")
    run_parser.add_argument("--bench", choices=sorted(BENCH_ROWS), default="messaging")
    run_parser.add_argument("--preload", required=True)
    run_parser.add_argument("--workdir", required=True)
    run_parser.add_argument("--out", required=True)
    run_parser.add_argument("--rows", nargs="*")
    run_parser.add_argument("--shifts", nargs="*", type=int, default=SHIFTS)
    run_parser.add_argument("--repetitions", type=int, default=REPETITIONS)
    run_parser.add_argument("--workers", type=int, default=WORKER_THREADS)
    run_parser.add_argument("binaries", nargs=3)
    summary_parser = commands.add_parser("summary")
    summary_parser.add_argument("results")
    lines_parser = commands.add_parser("lines")
    lines_parser.add_argument("--bench", choices=sorted(BENCH_ROWS), default="messaging")
    args = parser.parse_args()

    if args.command == "run":
        binaries = dict(binary.split("=", 1) for binary in args.binaries)
        if sorted(binaries) != sorted(ORDER):
            sys.exit(f"expected binaries {ORDER}, got {sorted(binaries)}")
        if args.rows is None:
            args.rows = BENCH_ROWS[args.bench]
        run(args, {name: os.path.abspath(path) for name, path in binaries.items()})
    elif args.command == "summary":
        summary(args.results)
    else:
        print(len(BENCH_ROWS[args.bench]) * len(SHIFTS) * REPETITIONS * len(ORDER))


def run(args, binaries):
    preload = os.path.abspath(args.preload)
    workdir = os.path.abspath(args.workdir)
    with open(args.out, "a") as out:
        for repetition in range(args.repetitions):
            order = ORDER[repetition % 3 :] + ORDER[: repetition % 3]
            for shift in args.shifts:
                for row in args.rows:
                    for name in order:
                        median = measure(binaries[name], name, row, shift, preload, workdir, args.workers)
                        line = {
                            "binary": name,
                            "repetition": repetition,
                            "shift": shift,
                            "row": row,
                            "median_ns": median,
                            "timestamp": time.time(),
                        }
                        out.write(json.dumps(line) + "\n")
                        out.flush()


def measure(binary, name, row, shift, preload, workdir, workers):
    home = os.path.join(workdir, name)
    row_dir = os.path.join(home, row)
    shutil.rmtree(row_dir, ignore_errors=True)
    os.makedirs(home, exist_ok=True)
    env = {
        "PATH": "/usr/bin:/bin",
        "HOME": home,
        "CRITERION_HOME": home,
        "LD_PRELOAD": preload,
        "STACK_SHIFT": str(shift),
    }
    started = time.time()
    process = subprocess.run(
        [binary, "--bench", "--noplot", "--warm-up-time", "0.2", "--measurement-time", "0.5",
         "--sample-size", "10", f"^{row}$"],
        cwd=home, env=env, capture_output=True, text=True,
    )
    context = f"{name} {row} shift {shift}"
    if process.returncode != 0:
        sys.exit(f"{context}: exit code {process.returncode}\n{process.stderr}")
    stderr = process.stderr.splitlines()
    if f"stack_shift={shift}" not in stderr:
        sys.exit(f"{context}: stack_shift={shift} missing from stderr\n{process.stderr}")
    threads = [int(line.split("=", 1)[1]) for line in stderr if line.startswith("stack_shift_threads=")]
    if len(threads) != 1 or threads[0] < workers:
        sys.exit(f"{context}: expected stack_shift_threads of at least {workers}\n{process.stderr}")

    estimates = os.path.join(row_dir, "new", "estimates.json")
    benchmark = os.path.join(row_dir, "new", "benchmark.json")
    for path in (estimates, benchmark):
        if not os.path.exists(path) or os.path.getmtime(path) < started:
            sys.exit(f"{context}: {path} was not written by this run")
    with open(benchmark) as file:
        full_id = json.load(file)["full_id"]
    if full_id != row:
        sys.exit(f"{context}: benchmark.json names {full_id}")
    with open(estimates) as file:
        return json.load(file)["median"]["point_estimate"]


def summary(results):
    lines = [json.loads(line) for line in open(results)]
    rows = list(dict.fromkeys(line["row"] for line in lines))
    shifts = sorted({line["shift"] for line in lines})

    print("| Row | Candidate | A/A |")
    print("|---|---|---|")
    for row in rows:
        print(f"| `{row}` | {figure(lines, row, 'C'):+.1%} | {figure(lines, row, 'A'):+.1%} |")

    print()
    print("Candidate against base per shift (median over repetitions):")
    print()
    print("| Row | " + " | ".join(str(shift) for shift in shifts) + " |")
    print("|---|" + "---|" * len(shifts))
    for row in rows:
        cells = [f"{figure(lines, row, 'C', shift):+.0%}" for shift in shifts]
        print(f"| `{row}` | " + " | ".join(cells) + " |")


def figure(lines, row, name, shift=None):
    def median_of(binary):
        return statistics.median(
            line["median_ns"]
            for line in lines
            if line["row"] == row
            and line["binary"] == binary
            and (shift is None or line["shift"] == shift)
        )

    return median_of(name) / median_of("B") - 1


if __name__ == "__main__":
    main()
