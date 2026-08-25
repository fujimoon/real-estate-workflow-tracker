"""FastAPI 経由の end-to-end: n8n開始 → ノード → agent呼び出し → 状態変更 → 終了"""

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    # API モジュールの store をテスト用 DB に差し替える
    from workflow_tracker.api import main as api_main
    from workflow_tracker.service.store import TrackerStore

    monkeypatch.setattr(api_main, "store", TrackerStore(db_path=str(tmp_path / "t.db")))
    return TestClient(api_main.app)


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


def test_full_lifecycle(client):
    # 1. n8n ワークフロー開始
    r = client.post(
        "/traces/start",
        json={
            "n8n_workflow_id": "wf-app1",
            "n8n_workflow_name": "反響対応",
            "n8n_execution_id": "exec-100",
        },
    )
    assert r.status_code == 200
    trace_id = r.json()["trace_id"]
    assert trace_id.startswith("WFT-")

    # 2. n8n ノード完了(trace_id ではなく n8n_execution_id で解決)
    r = client.post(
        "/events",
        json={
            "n8n_execution_id": "exec-100",
            "source": "n8n",
            "event_type": "node_finished",
            "node_name": "メール解析(Set)",
            "summary": "反響メールを整形",
        },
    )
    assert r.status_code == 200 and r.json()["seq"] == 2

    # 3. LangGraph エージェント呼び出し(thread_id がマッピングされる)
    r = client.post(
        "/events",
        json={
            "n8n_execution_id": "exec-100",
            "source": "langgraph",
            "event_type": "agent_invoked",
            "app": "app1_inquiry_agent",
            "thread_id": "th-001",
        },
    )
    assert r.status_code == 200

    # 3'. 以後は thread_id だけでイベントを送れる(LangGraph 側は execution_id を持ち回らなくて良い)
    r = client.post(
        "/events",
        json={
            "thread_id_lookup": "th-001",
            "source": "langgraph",
            "event_type": "agent_node_finished",
            "node_name": "classify_intent",
            "thread_id": "th-001",
        },
    )
    assert r.status_code == 200

    # 4. 状態変更(interrupt → 承認待ち)
    r = client.post(
        "/events",
        json={
            "thread_id_lookup": "th-001",
            "source": "langgraph",
            "event_type": "status_changed",
            "thread_id": "th-001",
            "payload": {"status": "waiting_human"},
        },
    )
    assert r.json()["status"] == "waiting_human"

    # LangGraph 終了 → n8n ワークフロー終了
    client.post(
        "/events",
        json={
            "thread_id_lookup": "th-001",
            "source": "langgraph",
            "event_type": "agent_finished",
            "thread_id": "th-001",
        },
    )
    r = client.post(
        "/events",
        json={
            "n8n_execution_id": "exec-100",
            "source": "n8n",
            "event_type": "workflow_finished",
            "summary": "メール送信まで完了",
        },
    )
    assert r.json()["status"] == "succeeded"

    # 詳細: n8n と LangGraph のイベントが1本のタイムラインに並ぶ
    detail = client.get(f"/traces/{trace_id}").json()
    assert detail["n8n_execution_id"] == "exec-100"
    assert detail["apps"] == ["app1_inquiry_agent"]
    assert detail["thread_ids"] == ["th-001"]
    types = [e["event_type"] for e in detail["events"]]
    assert types == [
        "workflow_started",
        "node_finished",
        "agent_invoked",
        "agent_node_finished",
        "status_changed",
        "agent_finished",
        "workflow_finished",
    ]
    sources = {e["source"] for e in detail["events"]}
    assert sources == {"n8n", "langgraph"}

    # タイムライン表示
    tl = client.get(f"/traces/{trace_id}/timeline").json()
    assert len(tl["timeline"]) == 7
    assert tl["status"] == "succeeded"


def test_error_propagation(client):
    client.post("/traces/start", json={"n8n_execution_id": "exec-err"})
    r = client.post(
        "/events",
        json={
            "n8n_execution_id": "exec-err",
            "source": "n8n",
            "event_type": "node_finished",
            "result": "error",
            "node_name": "CRM記録",
            "error_message": "CRM API が 500 を返しました",
        },
    )
    assert r.json()["status"] == "running"  # ノード単発エラーでは failed にしない
    r = client.post(
        "/events",
        json={
            "n8n_execution_id": "exec-err",
            "source": "n8n",
            "event_type": "workflow_finished",
            "result": "error",
            "error_message": "CRM API が 500 を返しました",
        },
    )
    assert r.json()["status"] == "failed"


def test_unknown_trace_404(client):
    r = client.post(
        "/events",
        json={"trace_id": "WFT-00000000-DEADBEEF", "source": "n8n", "event_type": "node_finished"},
    )
    assert r.status_code == 404
    assert client.get("/traces/WFT-00000000-DEADBEEF").status_code == 404
