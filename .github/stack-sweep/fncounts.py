"""Per-function instruction counts of two binaries, names normalized by disasm.py:
fncounts.py BASE CANDIDATE [--select REGEX] [--rename REGEX=REPLACEMENT]... Name collisions are
listed and left out of the counts.
"""

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from disasm import load, renames_from

parser = argparse.ArgumentParser()
parser.add_argument("base")
parser.add_argument("candidate")
parser.add_argument("--select", default=r"tellus|messaging|persistence")
parser.add_argument("--rename", action="append", help="REGEX=REPLACEMENT, applied to the candidate")
args = parser.parse_args()

base, candidate = load(args.base, []), load(args.candidate, renames_from(args.rename))
select = re.compile(args.select)
selected = lambda name: select.search(name) and "drop_glue" not in name
a = {name: len(body) for name, body in base.functions.items() if selected(name)}
b = {name: len(body) for name, body in candidate.functions.items() if selected(name)}
collisions = sorted(set(base.collisions(a)) | set(candidate.collisions(b)))
only_a = sorted(set(a) - set(b))
only_b = sorted(set(b) - set(a))
common = sorted(set(a) & set(b) - set(collisions))
differ = [(name, a[name], b[name]) for name in common if a[name] != b[name]]
print(
    f"functions only in base: {len(only_a)}; only in candidate: {len(only_b)}; "
    f"differing: {len(differ)}; same count: {len(common) - len(differ)}; colliding: {len(collisions)}"
)
for name in only_a:
    print("base only", a[name], name[:170])
for name in only_b:
    print("cand only", b[name], name[:170])
for name, x, y in differ:
    print(f"differ {x} -> {y}", name[:170])
for name in collisions:
    print(f"collision {name[:170]}: base {len(base.raw_names.get(name, ()))}, "
          f"candidate {len(candidate.raw_names.get(name, ()))} functions")
