"""Compose the GitHub release of a crate from its hand-written notes and the generated changelog.

Usage: release_notes.py TAG [CHANGELOG]. Run it inside the repository. TAG is a release tag
`<crate>-v<major>.<minor>.<patch>`. The notes are the Markdown files added to `<crate>/release-notes/`
since the crate's previous tag, oldest first. CHANGELOG is a file holding the generated release body,
which follows the notes. Prints nothing if no note was added or CHANGELOG already holds notes. A TAG
which does not exist yet ends at HEAD, so the output previews the next release.
"""

import re
import subprocess
import sys

TAG = re.compile(r"(?P<crate>.+)-v(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)")
NOTES_HEADING = "## Release notes"
CHANGELOG_HEADING = "## Changelog"


def release_body(tag, changelog):
    if changelog.lstrip().startswith(NOTES_HEADING):
        return ""
    match = parse(tag)
    crate = match["crate"]
    end = tag if exists(tag) else "HEAD"
    previous = previous_tag(crate, version(match))
    notes = [git("show", f"{end}:{path}").strip() for path in added_notes(crate, previous, end)]
    if not notes:
        return ""
    body = f"{NOTES_HEADING}\n\n" + "\n\n".join(notes) + "\n"
    if changelog.strip():
        body += f"\n{CHANGELOG_HEADING}\n\n{changelog.strip()}\n"
    return body


def parse(tag):
    match = TAG.fullmatch(tag)
    if match is None:
        sys.exit(f"not a release tag: {tag}")
    return match


def version(match):
    return (int(match["major"]), int(match["minor"]), int(match["patch"]))


def exists(tag):
    return bool(git("tag", "--list", tag))


def previous_tag(crate, current):
    earlier = []
    for tag in git("tag", "--list", f"{crate}-v*").split():
        match = TAG.fullmatch(tag)
        if match is not None and match["crate"] == crate and version(match) < current:
            earlier.append((version(match), tag))
    return max(earlier)[1] if earlier else None


def added_notes(crate, previous, end):
    directory = f"{crate}/release-notes/"
    revisions = f"{previous}..{end}" if previous else end
    added = git(
        "log", "--no-renames", "--diff-filter=A", "--name-only", "--format=", "--reverse",
        revisions, "--", directory,
    ).split()
    present = set(git("ls-tree", "-r", "--name-only", end, "--", directory).split())
    return [path for path in dict.fromkeys(added) if path.endswith(".md") and path in present]


def git(*args):
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout


def main(args):
    if len(args) not in (1, 2):
        sys.exit(__doc__)
    changelog = ""
    if len(args) == 2:
        with open(args[1], encoding="utf-8") as file:
            changelog = file.read()
    sys.stdout.write(release_body(args[0], changelog))


if __name__ == "__main__":
    main(sys.argv[1:])
