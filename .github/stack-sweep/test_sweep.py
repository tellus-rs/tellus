import argparse
import io
import json
import os
import stat
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sweep
from sweep import cluster_rows, figure, listed_rows, parse_list, run, run_cluster, summary, swept_rows

FAKE_BENCH = """#!{python}
import json, os, sys, time
rows = {rows!r}
if sys.argv[1:] == ["--bench", "--list"]:
    print("Gnuplot not found, using plotters backend")
    for row in rows:
        print(row + ": benchmark")
    sys.exit(0)
row = sys.argv[-1][1:-1].replace("\\\\", "")
time.sleep(0.02)
shift = os.environ["STACK_SHIFT"]
print("stack_shift=" + shift, file=sys.stderr)
print("stack_shift_threads={threads}", file=sys.stderr)
directory = os.path.join(os.environ["CRITERION_HOME"], row, "new")
os.makedirs(directory, exist_ok=True)
with open(os.path.join(directory, "benchmark.json"), "w") as file:
    json.dump({{"full_id": row}}, file)
with open(os.path.join(directory, "estimates.json"), "w") as file:
    json.dump({{"median": {{"point_estimate": {median} + int(shift)}}}}, file)
"""

FAKE_CLUSTER = """#!{python}
import os, sys
assert os.environ["TELLUS_REMOTING_ROLE"] == "bench"
shift = os.environ.get("STACK_SHIFT", "0")
print("ADDR 127.0.0.1:1234")
for row, value, unit in {rows!r}:
    print(f"{{row}}: {{value + int(shift)}} {{unit}} (some detail)")
for threads in {threads!r}:
    print("stack_shift=" + shift, file=sys.stderr)
    if threads is not None:
        print(f"stack_shift_threads={{threads}}", file=sys.stderr)
"""


def line(binary, row, median_ns, shift=0):
    return {"binary": binary, "repetition": 0, "shift": shift, "row": row, "median_ns": median_ns}


class Rows(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.directory.cleanup()

    def bench(self, name, rows, threads=4, median=100):
        path = os.path.join(self.directory.name, name)
        with open(path, "w") as file:
            file.write(FAKE_BENCH.format(python=sys.executable, rows=rows, threads=threads, median=median))
        os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)
        return path

    def test_the_list_holds_only_benchmark_lines(self):
        output = "Gnuplot not found\nflood/unbounded: benchmark\n\nping_pong/pairs/1: benchmark\n"
        self.assertEqual(parse_list(output), ["flood/unbounded", "ping_pong/pairs/1"])

    def test_the_rows_come_from_the_bench_in_its_order(self):
        rows = ["b/2", "a/1", "c/3"]
        self.assertEqual(listed_rows(self.bench("B", rows)), rows)

    def test_a_bench_listing_nothing_fails(self):
        with self.assertRaises(SystemExit):
            listed_rows(self.bench("B", []))

    def test_the_swept_rows_are_the_base_rows(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            rows = swept_rows({"B": ["a", "b"], "C": ["a", "b", "new"], "A": ["a", "b"]})
        self.assertEqual(rows, ["a", "b"])
        self.assertIn("new", stderr.getvalue())

    def test_a_row_of_the_base_missing_in_the_candidate_fails(self):
        with self.assertRaises(SystemExit):
            swept_rows({"B": ["a", "b"], "C": ["a"], "A": ["a", "b"]})

    def test_lines_counts_every_listed_row(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            sys.argv = ["sweep.py", "lines", self.bench("B", ["a", "b"])]
            sweep.main()
        self.assertEqual(int(stdout.getvalue()), 2 * len(sweep.SHIFTS) * sweep.REPETITIONS * 3)

    def args(self, rows, shifts=(0, 256), repetitions=3):
        return argparse.Namespace(
            preload="/dev/null", workdir=os.path.join(self.directory.name, "work"),
            out=os.path.join(self.directory.name, "results.jsonl"), rows=rows, shifts=list(shifts),
            repetitions=repetitions, workers=4,
        )

    def test_a_run_measures_every_row_shift_and_binary_with_a_rotating_order(self):
        rows = ["a/1", "b/2"]
        binaries = {name: self.bench(name, rows, median=median) for name, median in (("B", 100), ("C", 110), ("A", 100))}
        args = self.args(rows)
        run(args, binaries)
        lines = [json.loads(line) for line in open(args.out)]
        self.assertEqual(len(lines), 2 * 2 * 3 * 3)
        self.assertEqual([line["binary"] for line in lines[:3]], ["B", "C", "A"])
        self.assertEqual([line["binary"] for line in lines[12:15]], ["C", "A", "B"])
        self.assertEqual((lines[6]["shift"], lines[6]["row"], lines[6]["median_ns"]), (256, "a/1", 356))
        self.assertAlmostEqual(figure(lines, "a/1", "C", 0), 0.1)

    def test_a_bench_with_too_few_shifted_threads_fails(self):
        binaries = {name: self.bench(name, ["a"], threads=2) for name in ("B", "C", "A")}
        with self.assertRaises(SystemExit):
            run(self.args(["a"], shifts=(0,), repetitions=1), binaries)


class Figures(unittest.TestCase):
    def test_the_figure_is_the_median_over_the_base_median(self):
        lines = [line("B", "r", 100), line("B", "r", 300), line("C", "r", 220), line("A", "r", 200)]
        self.assertAlmostEqual(figure(lines, "r", "C"), 0.1)
        self.assertAlmostEqual(figure(lines, "r", "A"), 0.0)

    def test_a_zero_base_median_fails(self):
        with self.assertRaises(SystemExit):
            figure([line("B", "r", 0), line("C", "r", 100)], "r", "C")

    def test_a_missing_row_fails(self):
        with self.assertRaises(SystemExit):
            figure([line("B", "r", 100)], "r", "C")

    def test_the_summary_has_a_row_per_benchmark(self):
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as file:
            for row in ("r", "s"):
                for binary in ("B", "C", "A"):
                    file.write(json.dumps(line(binary, row, 100)) + "\n")
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            summary(file.name)
        os.unlink(file.name)
        self.assertIn("| `r` | +0.0% | +0.0% |", stdout.getvalue())
        self.assertIn("| `s` | +0.0% | +0.0% |", stdout.getvalue())


class Cluster(unittest.TestCase):
    ROWS = [("pipelined", 1000, "round trips/s"), ("serial", 50, "us/round trip"), ("bulk", 500, "MiB/s")]

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()

    def tearDown(self):
        self.directory.cleanup()

    def bench(self, name, rows=ROWS, threads=(10, 10, 12)):
        path = os.path.join(self.directory.name, name)
        with open(path, "w") as file:
            file.write(FAKE_CLUSTER.format(python=sys.executable, rows=rows, threads=list(threads)))
        os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)
        return path

    def args(self, shifts=(0, 256), repetitions=3):
        return argparse.Namespace(
            preload="/dev/null", workdir=os.path.join(self.directory.name, "work"),
            out=os.path.join(self.directory.name, "results.jsonl"), rows=None, shifts=list(shifts),
            repetitions=repetitions, workers=4,
        )

    def test_a_throughput_is_inverted_and_a_latency_scaled_to_nanoseconds(self):
        output = "ADDR 127.0.0.1:1\npipelined: 2000 round trips/s (50000 pings)\nask serial: 61.5 us/ask (2000 asks)\nbulk: 512.0 MiB/s\n"
        self.assertEqual(
            cluster_rows(output, "test"), {"pipelined": 500000.0, "ask serial": 61500.0, "bulk": 1953125.0}
        )

    def test_an_unknown_unit_fails(self):
        with self.assertRaises(SystemExit):
            cluster_rows("serial: 3 ms/round trip\n", "test")

    def test_a_row_printed_twice_fails(self):
        with self.assertRaises(SystemExit):
            cluster_rows("bulk: 1 MiB/s\nbulk: 2 MiB/s\n", "test")

    def test_output_without_a_row_fails(self):
        with self.assertRaises(SystemExit):
            cluster_rows("ADDR 127.0.0.1:1\n", "test")

    def test_a_run_records_every_row_of_every_run_with_a_rotating_order(self):
        binaries = {name: self.bench(name) for name in ("B", "C", "A")}
        args = self.args()
        run_cluster(args, binaries)
        lines = [json.loads(line) for line in open(args.out)]
        self.assertEqual(len(lines), 3 * 2 * 3 * 3)
        self.assertEqual([line["binary"] for line in lines[:9:3]], ["B", "C", "A"])
        self.assertEqual([line["binary"] for line in lines[18:27:3]], ["C", "A", "B"])
        self.assertEqual([line["row"] for line in lines[:3]], ["pipelined", "serial", "bulk"])
        self.assertEqual((lines[10]["shift"], lines[10]["row"], lines[10]["median_ns"]), (256, "serial", 306000.0))

    def test_a_row_of_the_base_missing_in_the_candidate_fails(self):
        binaries = {"B": self.bench("B"), "C": self.bench("C", rows=self.ROWS[:2]), "A": self.bench("A")}
        with self.assertRaises(SystemExit):
            run_cluster(self.args(shifts=(0,), repetitions=1), binaries)

    def test_a_row_only_the_candidate_prints_is_not_swept(self):
        extra = self.ROWS + [("new", 1, "asks/s")]
        binaries = {"B": self.bench("B"), "C": self.bench("C", rows=extra), "A": self.bench("A")}
        args = self.args(shifts=(0,), repetitions=1)
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            run_cluster(args, binaries)
        self.assertNotIn("new", {json.loads(line)["row"] for line in open(args.out)})
        self.assertIn("new", stderr.getvalue())

    def test_a_process_with_too_few_shifted_threads_fails(self):
        binaries = {name: self.bench(name, threads=(10, 2, 12)) for name in ("B", "C", "A")}
        with self.assertRaises(SystemExit):
            run_cluster(self.args(shifts=(0,), repetitions=1), binaries)

    def test_a_process_reporting_no_thread_count_fails(self):
        binaries = {name: self.bench(name, threads=(10, None, 12)) for name in ("B", "C", "A")}
        with self.assertRaises(SystemExit):
            run_cluster(self.args(shifts=(0,), repetitions=1), binaries)

    def test_lines_counts_every_printed_row(self):
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            sys.argv = ["sweep.py", "lines", "--kind", "cluster", self.bench("B")]
            sweep.main()
        self.assertEqual(int(stdout.getvalue()), 3 * len(sweep.SHIFTS) * sweep.CLUSTER_REPETITIONS * 3)


if __name__ == "__main__":
    unittest.main()
