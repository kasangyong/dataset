import json
import pathlib

import pytest

FIXTURES = pathlib.Path(__file__).parent / "fixtures"
DT = "2026-08-25"


@pytest.fixture(autouse=True)
def isolated_data_root(tmp_path, monkeypatch):
    """Point every storage write at a temp dir so tests never touch data/."""
    monkeypatch.setenv("DATASETS_DATA_ROOT", str(tmp_path / "data"))
    return tmp_path / "data"


@pytest.fixture
def load_fixture():
    """Real captured API payloads. XML sources are handed back as text."""

    def _load(name):
        xml = FIXTURES / f"{name}.xml"
        if xml.exists():
            return xml.read_text(encoding="utf-8")
        return json.loads((FIXTURES / f"{name}.json").read_text(encoding="utf-8"))

    return _load


@pytest.fixture
def fixture_meta():
    def _load(name):
        return json.loads((FIXTURES / f"{name}.meta.json").read_text(encoding="utf-8"))

    return _load
