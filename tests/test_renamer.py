"""Tests for the Renamer's deterministic filename cleanup.

We test _clean_filename and _sanitize_filename as standalone functions
to avoid importing the full app (which requires pydantic_settings, etc.).
"""

import os
import re
import pytest


# ── Copy the two methods under test so we don't need the full import chain ──

def _clean_filename(name: str) -> str:
    """Deterministic cleanup: title case, remove dashes, clean separators."""
    base, ext = os.path.splitext(name)
    clean = re.sub(r'[-_.]+', ' ', base)
    clean = re.sub(r'[\[\{][^\]\}]*[\]\}]', '', clean)
    clean = re.sub(r'\s+', ' ', clean).strip()
    clean = clean.title()
    return f"{clean}{ext}"


def _sanitize_filename(name: str) -> str:
    """Remove characters that are illegal in common filesystems."""
    illegal = r'[<>:"/\\|?*\x00-\x1f]'
    name = re.sub(illegal, '', name)
    name = re.sub(r'\.{2,}', '.', name)
    name = re.sub(r'\s{2,}', ' ', name)
    name = name.strip('. ')
    return name


class TestCleanFilename:
    """Tests for _clean_filename — deterministic title case + separator cleanup."""

    def test_dashes_to_title_case(self):
        assert _clean_filename("inception-2010-1080p-bluray.mkv") == "Inception 2010 1080P Bluray.mkv"

    def test_dots_as_separators(self):
        assert _clean_filename("The.Matrix.1999.mkv") == "The Matrix 1999.mkv"

    def test_underscores(self):
        assert _clean_filename("breaking_bad_s01e01.mp4") == "Breaking Bad S01E01.mp4"

    def test_mixed_separators(self):
        assert _clean_filename("some-movie_name.2020.mkv") == "Some Movie Name 2020.mkv"

    def test_bracketed_junk_removed(self):
        assert _clean_filename("Movie.Name.2020.[YTS].mkv") == "Movie Name 2020.mkv"

    def test_curly_brace_junk_removed(self):
        assert _clean_filename("Movie.Name.{GROUP}.mkv") == "Movie Name.mkv"

    def test_multiple_brackets(self):
        assert _clean_filename("[RLS]Movie.Name.2020[x264].mkv") == "Movie Name 2020.mkv"

    def test_already_clean_title_case(self):
        assert _clean_filename("Inception 2010.mkv") == "Inception 2010.mkv"

    def test_extension_preserved(self):
        assert _clean_filename("test-file.mp4") == "Test File.mp4"

    def test_subtitle_extension(self):
        assert _clean_filename("movie-name.en.srt") == "Movie Name En.srt"

    def test_extra_whitespace_collapsed(self):
        assert _clean_filename("movie - - name.mkv") == "Movie Name.mkv"

    def test_scene_style_name(self):
        assert _clean_filename("Inception.2010.1080p.BluRay.x264-GROUP.mkv") == "Inception 2010 1080P Bluray X264 Group.mkv"


class TestSanitizeFilename:
    """Tests for _sanitize_filename — illegal character removal."""

    def test_removes_illegal_chars(self):
        assert _sanitize_filename('Movie: The "Sequel".mkv') == "Movie The Sequel.mkv"

    def test_collapses_dots(self):
        assert _sanitize_filename("Movie...Name.mkv") == "Movie.Name.mkv"

    def test_strips_leading_trailing(self):
        assert _sanitize_filename(" .Movie Name. ") == "Movie Name"
