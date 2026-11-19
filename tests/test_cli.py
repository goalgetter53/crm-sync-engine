from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from crm_sync.cli import main
from tests.conftest import EXAMPLE_MAPPING


@pytest.fixture
def env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, http: TestClient) -> TestClient:
    monkeypatch.setenv("WAREHOUSE_URL", f"sqlite:///{tmp_path / 'wh.db'}")
    monkeypatch.setenv("STATE_URL", f"sqlite:///{tmp_path / 'state.db'}")
    monkeypatch.setenv("HUBSPOT_ACCESS_TOKEN", "test-token")
    assert (
        main(
            ["demo-seed", "--companies", "3", "--contacts", "12", "--deals", "2", "--bad-rows", "1"]
        )
        == 0
    )
    http.headers["Authorization"] = "Bearer test-token"
    return http


def test_validate_ok_and_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["validate", str(EXAMPLE_MAPPING)]) == 0
    assert "companies -> contacts -> deals" in capsys.readouterr().out
    bad = tmp_path / "bad.yaml"
    bad.write_text("version: 1\nobjects: {}\n")
    assert main(["validate", str(bad)]) == 1
    assert "invalid mapping" in capsys.readouterr().err


def test_plan_apply_dlq_replay_flow(env: TestClient, capsys: pytest.CaptureFixture[str]) -> None:
    m = str(EXAMPLE_MAPPING)
    assert main(["plan", m, "--detailed-exitcode"], http=env) == 2
    out = capsys.readouterr().out
    assert "+ create" in out
    assert "Plan: 17 to create" in out

    assert main(["apply", m], http=env) == 0
    out = capsys.readouterr().out
    assert "created=16" in out
    assert "1 operation(s) dead-lettered" in out

    assert main(["dlq", m], http=env) == 0
    dlq = capsys.readouterr().out
    assert "zombie" in dlq
    assert "1 open dead letter(s)" in dlq

    assert main(["replay", m], http=env) == 1  # still broken: data not fixed yet
    assert "0 resolved, 1 still failing" in capsys.readouterr().out


def test_plan_json_output(env: TestClient, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["plan", str(EXAMPLE_MAPPING), "--json"], http=env) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["summary"]["create"] == 17
    assert data["records"][0]["changes"][0]["write_crm"] is True
