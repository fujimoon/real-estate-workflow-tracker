"""TrackerClient の fail-safe と送信ボディ"""

import httpx
import pytest

from workflow_tracker.client.tracker_client import TrackerClient


def test_disabled_client_does_not_raise(monkeypatch):
    monkeypatch.delenv("TRACKER_BASE_URL", raising=False)
    client = TrackerClient(app_name="app1_inquiry_agent", base_url="")
    assert not client.enabled
    # トラッカー未設定でも例外を上げない
    assert client.agent_invoked(thread_id="t", n8n_execution_id="e") is None
    client.node_finished(node_name="n", thread_id="t")
    client.agent_finished(thread_id="t")


def test_unreachable_tracker_does_not_raise():
    client = TrackerClient(
        app_name="app1_inquiry_agent",
        base_url="http://127.0.0.1:1",  # 接続不能
        timeout=0.2,
    )
    assert client.agent_invoked(thread_id="t", n8n_execution_id="e") is None
    client.agent_finished(thread_id="t", error="boom")


def test_request_bodies(monkeypatch):
    sent: list[dict] = []

    def fake_post(url, json=None, timeout=None):
        sent.append({"url": url, "body": json})
        return httpx.Response(
            200,
            json={"trace_id": "WFT-1", "seq": 1, "occurred_at": "t", "status": "running"},
            request=httpx.Request("POST", url),
        )

    monkeypatch.setattr(httpx, "post", fake_post)
    client = TrackerClient(app_name="app2_property_rag", base_url="http://tracker:8010")

    trace_id = client.agent_invoked(thread_id="th-9", n8n_execution_id="exec-9")
    assert trace_id == "WFT-1"
    client.node_finished(node_name="hybrid_search", thread_id="th-9", error="index missing")
    client.status_changed(thread_id="th-9", status="waiting_human")
    client.agent_finished(thread_id="th-9")

    assert [b["body"]["event_type"] for b in sent] == [
        "agent_invoked",
        "agent_node_finished",
        "status_changed",
        "agent_finished",
    ]
    assert sent[0]["body"]["app"] == "app2_property_rag"
    assert sent[1]["body"]["result"] == "error"
    assert sent[2]["body"]["payload"] == {"status": "waiting_human"}
    # agent_invoked 以外は thread_id で解決する
    assert all(b["body"].get("thread_id_lookup") == "th-9" for b in sent[1:])
