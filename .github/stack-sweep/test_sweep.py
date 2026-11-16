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
from sweep import figure, listed_rows, parse_list, run, summary, swept_rows

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


if __name__ == "__main__":
    unittest.main()
