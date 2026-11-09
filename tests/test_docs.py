"""The documentation stays true: its examples run, and its references cover every command, option and public name."""

import argparse
import importlib
import os
import re
import shutil
import subprocess
import sys

import pytest

import deja
from deja import cli

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS = os.path.join(ROOT, "docs")


def page(*parts) -> str:
    with open(os.path.join(DOCS, *parts), encoding="utf-8") as f:
        return f.read()


def test_record_and_replay_example_runs(tmp_path, monkeypatch):
    pytest.importorskip("fastapi")
    monkeypatch.syspath_prepend(os.path.join(DOCS, "examples"))
    for name in ("wallet", "record_and_replay"):
        sys.modules.pop(name, None)
    example = importlib.import_module("record_and_replay")
    recording = example.main(str(tmp_path / "recordings"))
    assert os.path.exists(recording)
    assert not os.path.exists(tmp_path / "recordings" / "replay"), "replay must not save recordings"


def _parser() -> argparse.ArgumentParser:
    seen = {}

    def capture(self, args=None, namespace=None):
        seen["parser"] = self
        raise SystemExit(0)

    original = argparse.ArgumentParser.parse_args
    argparse.ArgumentParser.parse_args = capture
    try:
        with pytest.raises(SystemExit):
            cli.main([])
    finally:
        argparse.ArgumentParser.parse_args = original
    return seen["parser"]


def test_cli_reference_documents_every_command_and_option():
    text = page("reference", "cli.md")
    parser = _parser()
    sub = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
    assert sub.choices, "no subcommands found"
    for name, command in sub.choices.items():
        assert f"## deja {name}" in text, f"docs/reference/cli.md has no section for deja {name}"
        section = text.split(f"## deja {name}", 1)[1].split("\n## ", 1)[0]
        for action in command._actions:
            for option in action.option_strings:
                if option in ("-h", "--help") or option in ("--app", "--deadline"):  # documented once, up top
                    continue
                assert option in section, f"deja {name} {option} is not documented in its section"
    assert "--app APP" in text and "--deadline SECONDS" in text


def test_python_api_reference_covers_the_public_names():
    text = page("reference", "python-api.md")
    for name in deja.__all__:
        if name.startswith("__"):
            continue
        assert re.search(rf"^::: deja\.(\w+\.)?{name}\b", text, re.M), f"deja.{name} is missing from python-api.md"


def test_every_page_is_in_the_navigation():
    with open(os.path.join(ROOT, "mkdocs.yml"), encoding="utf-8") as f:
        nav = f.read()
    for folder, _, files in os.walk(DOCS):
        for file in files:
            if file.endswith(".md"):
                rel = os.path.relpath(os.path.join(folder, file), DOCS).replace(os.sep, "/")
                assert rel in nav, f"{rel} is not in mkdocs.yml's nav"


@pytest.mark.skipif(importlib.util.find_spec("mkdocs") is None, reason="docs extra not installed")
def test_site_builds_without_warnings(tmp_path):
    if importlib.util.find_spec("mkdocstrings") is None or importlib.util.find_spec("material") is None:
        pytest.skip("docs extra not installed")
    env = {**os.environ, "NO_MKDOCS_2_WARNING": "1", "PYTHONIOENCODING": "utf-8"}
    out = subprocess.run(
        [sys.executable, "-m", "mkdocs", "build", "--strict", "-d", str(tmp_path / "site")],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=300,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    shutil.rmtree(tmp_path / "site", ignore_errors=True)
