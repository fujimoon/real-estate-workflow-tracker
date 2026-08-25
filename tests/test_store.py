"""ストアの純ロジック: 採番・マッピング・状態遷移・エラー記録"""

from workflow_tracker.models.trace import (
    EventResult,
    EventSource,
    EventType,
    TraceEvent,
    TraceStatus,
)
from workflow_tracker.service.id_gen import new_trace_id


def _ev(trace_id, **kw):
    base = dict(
        trace_id=trace_id,
        source=EventSource.N8N,
        event_type=EventType.NODE_FINISHED,
    )
    base.update(kw)
    return TraceEvent(**base)


def test_trace_id_format():
    tid = new_trace_id()
    prefix, date_part, rand = tid.split("-")
    assert prefix == "WFT"
    assert len(date_part) == 8 and date_part.isdigit()
    assert len(rand) == 8


def test_trace_ids_are_unique():
    ids = {new_trace_id() for _ in range(200)}
    assert len(ids) == 200


def test_start_trace_records_workflow_started(store):
    trace = store.start_trace(
        n8n_workflow_id="wf-1", n8n_workflow_name="反響対応", n8n_execution_id="exec-1"
    )
    assert trace.status == TraceStatus.RUNNING
    assert trace.event_count == 1
    detail = store.get_trace_detail(trace.trace_id)
    assert detail.events[0].event_type == EventType.WORKFLOW_STARTED
    assert detail.events[0].source == EventSource.N8N


def test_resolve_by_execution_id_and_thread_id(store):
    trace = store.start_trace(n8n_execution_id="exec-42")
    assert store.resolve_trace_id(n8n_execution_id="exec-42") == trace.trace_id
    # agent_invoked で thread_id がマッピングされ、以後 thread_id でも解決できる
    store.append_event(
        _ev(
            trace.trace_id,
            source=EventSource.LANGGRAPH,
            event_type=EventType.AGENT_INVOKED,
            app="app1_inquiry_agent",
            thread_id="th-abc",
        )
    )
    assert store.resolve_trace_id(thread_id="th-abc") == trace.trace_id
    updated = store.get_trace(trace.trace_id)
    assert updated.apps == ["app1_inquiry_agent"]
    assert updated.thread_ids == ["th-abc"]
    assert store.resolve_trace_id(n8n_execution_id="unknown") is None


def test_seq_is_sequential(store):
    trace = store.start_trace(n8n_execution_id="exec-seq")
    for i in range(3):
        ev = store.append_event(_ev(trace.trace_id, node_name=f"node{i}"))
        assert ev.seq == i + 2  # workflow_started が seq=1
    assert store.get_trace(trace.trace_id).event_count == 4


def test_node_error_records_message_but_keeps_running(store):
    """ノード単発のエラーはトレースを failed にしない(n8n がリトライしうる)"""
    trace = store.start_trace(n8n_execution_id="exec-err")
    store.append_event(
        _ev(
            trace.trace_id,
            node_name="メール送信",
            result=EventResult.ERROR,
            error_message="SMTP timeout",
        )
    )
    t = store.get_trace(trace.trace_id)
    assert t.status == TraceStatus.RUNNING
    assert t.error_message == "SMTP timeout"


def test_status_changed_to_waiting_human_and_back(store):
    trace = store.start_trace(n8n_execution_id="exec-hitl")
    store.append_event(
        _ev(
            trace.trace_id,
            event_type=EventType.STATUS_CHANGED,
            payload={"status": "waiting_human"},
        )
    )
    assert store.get_trace(trace.trace_id).status == TraceStatus.WAITING_HUMAN
    store.append_event(
        _ev(
            trace.trace_id,
            event_type=EventType.STATUS_CHANGED,
            payload={"status": "running"},
        )
    )
    assert store.get_trace(trace.trace_id).status == TraceStatus.RUNNING


def test_invalid_status_value_is_ignored(store):
    trace = store.start_trace(n8n_execution_id="exec-bad")
    store.append_event(
        _ev(
            trace.trace_id,
            event_type=EventType.STATUS_CHANGED,
            payload={"status": "not_a_status"},
        )
    )
    assert store.get_trace(trace.trace_id).status == TraceStatus.RUNNING


def test_workflow_finished_ok_sets_succeeded(store):
    trace = store.start_trace(n8n_execution_id="exec-ok")
    store.append_event(_ev(trace.trace_id, event_type=EventType.WORKFLOW_FINISHED))
    t = store.get_trace(trace.trace_id)
    assert t.status == TraceStatus.SUCCEEDED
    assert t.finished_at is not None


def test_workflow_finished_error_sets_failed(store):
    trace = store.start_trace(n8n_execution_id="exec-ng")
    store.append_event(
        _ev(
            trace.trace_id,
            event_type=EventType.WORKFLOW_FINISHED,
            result=EventResult.ERROR,
            error_message="下流APIが500",
        )
    )
    t = store.get_trace(trace.trace_id)
    assert t.status == TraceStatus.FAILED
    assert t.error_message == "下流APIが500"


def test_list_traces_filter(store):
    a = store.start_trace(n8n_execution_id="e-1")
    b = store.start_trace(n8n_execution_id="e-2")
    store.append_event(_ev(b.trace_id, event_type=EventType.WORKFLOW_FINISHED))
    running = store.list_traces(status="running")
    assert [t.trace_id for t in running] == [a.trace_id]
    assert len(store.list_traces()) == 2
