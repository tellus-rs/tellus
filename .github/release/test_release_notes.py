import os
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from release_notes import release_body


class ReleaseBody(unittest.TestCase):
    def setUp(self):
        self.cwd = os.getcwd()
        self.repo = tempfile.TemporaryDirectory()
        os.chdir(self.repo.name)
        self.git("init", "--quiet", "--initial-branch=main")
        self.git("config", "user.name", "Test")
        self.git("config", "user.email", "test@example.com")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        self.commit("tellus/src.rs", "")

    def tearDown(self):
        os.chdir(self.cwd)
        self.repo.cleanup()

    def git(self, *args):
        subprocess.run(["git", *args], check=True, capture_output=True)

    def commit(self, path, content):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as file:
            file.write(content)
        self.git("add", path)
        self.git("commit", "--quiet", "--message", path)

    def test_notes_since_the_previous_tag_precede_the_changelog(self):
        self.commit("tellus/release-notes/old.md", "### Old\n")
        self.git("tag", "tellus-v0.5.3")
        self.commit("tellus/release-notes/b-first.md", "### First\n")
        self.commit("tellus/release-notes/a-second.md", "### Second\n")
        self.git("tag", "tellus-v0.6.0")

        self.assertEqual(
            release_body("tellus-v0.6.0", "### Added\n\n- stashing\n"),
            "## Release notes\n\n### First\n\n### Second\n\n## Changelog\n\n### Added\n\n- stashing\n",
        )

    def test_without_a_changelog_the_body_is_the_notes(self):
        self.git("tag", "tellus-v0.5.3")
        self.commit("tellus/release-notes/first.md", "### First\n")
        self.git("tag", "tellus-v0.6.0")

        self.assertEqual(release_body("tellus-v0.6.0", ""), "## Release notes\n\n### First\n")

    def test_the_previous_tag_is_the_highest_version_below_by_number(self):
        self.git("tag", "tellus-v0.9.0")
        self.commit("tellus/release-notes/first.md", "### First\n")
        self.git("tag", "tellus-v0.10.0")
        self.commit("tellus/release-notes/second.md", "### Second\n")
        self.git("tag", "tellus-v0.11.0")

        self.assertEqual(release_body("tellus-v0.11.0", ""), "## Release notes\n\n### Second\n")

    def test_tags_and_notes_of_other_crates_are_ignored(self):
        self.git("tag", "tellus-v0.5.3")
        self.commit("tellus/release-notes/core.md", "### Core\n")
        self.git("tag", "tellus-persistence-postgres-v0.2.14")
        self.commit("tellus-persistence-postgres/release-notes/store.md", "### Store\n")
        self.git("tag", "tellus-v0.6.0")
        self.git("tag", "tellus-persistence-postgres-v0.3.0")

        self.assertEqual(release_body("tellus-v0.6.0", ""), "## Release notes\n\n### Core\n")
        self.assertEqual(
            release_body("tellus-persistence-postgres-v0.3.0", ""), "## Release notes\n\n### Store\n"
        )

    def test_a_first_release_takes_every_note(self):
        self.commit("tellus/release-notes/first.md", "### First\n")
        self.git("tag", "tellus-v0.1.0")

        self.assertEqual(release_body("tellus-v0.1.0", ""), "## Release notes\n\n### First\n")

    def test_a_tag_which_does_not_exist_yet_previews_head(self):
        self.git("tag", "tellus-v0.5.3")
        self.commit("tellus/release-notes/first.md", "### First\n")

        self.assertEqual(release_body("tellus-v0.6.0", ""), "## Release notes\n\n### First\n")

    def test_an_edited_or_deleted_note_counts_as_it_is_at_the_tag(self):
        self.git("tag", "tellus-v0.5.3")
        self.commit("tellus/release-notes/kept.md", "### Draft\n")
        self.commit("tellus/release-notes/kept.md", "### Kept\n")
        self.commit("tellus/release-notes/dropped.md", "### Dropped\n")
        self.git("rm", "--quiet", "tellus/release-notes/dropped.md")
        self.git("commit", "--quiet", "--message", "drop")
        self.git("tag", "tellus-v0.6.0")

        self.assertEqual(release_body("tellus-v0.6.0", ""), "## Release notes\n\n### Kept\n")

    def test_a_note_edited_after_its_release_is_not_repeated(self):
        self.commit("tellus/release-notes/first.md", "### First\n")
        self.git("tag", "tellus-v0.5.3")
        self.commit("tellus/release-notes/first.md", "### First, corrected\n")
        self.git("tag", "tellus-v0.6.0")

        self.assertEqual(release_body("tellus-v0.6.0", "### Fixed\n"), "")

    def test_only_markdown_files_are_notes(self):
        self.git("tag", "tellus-v0.5.3")
        self.commit("tellus/release-notes/first.md", "### First\n")
        self.commit("tellus/release-notes/README.txt", "not a note\n")
        self.git("tag", "tellus-v0.6.0")

        self.assertEqual(release_body("tellus-v0.6.0", ""), "## Release notes\n\n### First\n")

    def test_a_release_which_holds_notes_already_is_left_alone(self):
        self.git("tag", "tellus-v0.5.3")
        self.commit("tellus/release-notes/first.md", "### First\n")
        self.git("tag", "tellus-v0.6.0")
        body = release_body("tellus-v0.6.0", "### Added\n")

        self.assertEqual(release_body("tellus-v0.6.0", body), "")


if __name__ == "__main__":
    unittest.main()
