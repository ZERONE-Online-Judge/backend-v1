"""Public server health must not disclose competitors' submission activity."""
from datetime import timedelta
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import now_utc
from app.routers import public
from app.settings import settings


client = TestClient(app)


@pytest.mark.parametrize("active_count", [0, 3])
def test_public_status_only_exposes_connected_node_count(monkeypatch, active_count):
    now = now_utc()
    monkeypatch.setattr(public, "now_utc", lambda: now)
    nodes = {
        str(index): SimpleNamespace(last_heartbeat_at=now, running_job_count=7)
        for index in range(active_count)
    }
    nodes["offline"] = SimpleNamespace(
        last_heartbeat_at=now - timedelta(seconds=max(5, settings.judge_node_active_window_seconds) + 1),
        running_job_count=9,
    )
    monkeypatch.setattr(public, "store", SimpleNamespace(
        judge_nodes=nodes,
        judge_jobs={"private-job": SimpleNamespace(status="pending")},
    ))

    response = client.get("/api/public/judge-status")

    assert response.status_code == 200
    assert response.json()["data"] == {"active_node_count": active_count}


def test_workload_changes_do_not_change_public_status(monkeypatch):
    node = SimpleNamespace(last_heartbeat_at=now_utc(), running_job_count=0)
    jobs = {}
    monkeypatch.setattr(public, "store", SimpleNamespace(judge_nodes={"node": node}, judge_jobs=jobs))

    idle = client.get("/api/public/judge-status").json()["data"]
    node.running_job_count = 7
    jobs.update({str(index): SimpleNamespace(status="pending") for index in range(11)})
    busy = client.get("/api/public/judge-status").json()["data"]

    assert idle == busy == {"active_node_count": 1}


def test_public_status_never_reads_submission_queue(monkeypatch):
    class HealthOnlyStore:
        judge_nodes = {}

        @property
        def judge_jobs(self):
            raise AssertionError("Public health must not inspect the submission queue")

    monkeypatch.setattr(public, "store", HealthOnlyStore())
    response = client.get("/api/public/judge-status")
    assert response.status_code == 200
    assert response.json()["data"] == {"active_node_count": 0}
