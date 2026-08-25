"""デモ: 1回のワークフロー実行を最初から最後まで記録して、タイムラインを表示する

トラッカーAPIを起動せずに、ストアを直接叩いて動作を確認できる。

    uv run python fixtures/demo_run.py
"""

import time

from workflow_tracker.models.trace import EventResult, EventSource, EventType, TraceEvent
from workflow_tracker.service.store import TrackerStore


def main() -> None:
    store = TrackerStore(db_path="storage/demo.db")

    # 1. n8n ワークフロー開始
    trace = store.start_trace(
        n8n_workflow_id="wf-app1",
        n8n_workflow_name="アプリ1 反響対応",
        n8n_execution_id="exec-demo-001",
        summary="SUUMO 反響メール受信により開始",
    )
    tid = trace.trace_id

    def ev(**kw):
        time.sleep(0.01)  # タイムラインの時刻が単調増加するように
        store.append_event(TraceEvent(trace_id=tid, **kw))

    # 2. n8n ノード完了
    ev(source=EventSource.N8N, event_type=EventType.NODE_FINISHED,
       node_name="メール整形(Set)", summary="反響メールを整形")
    # 3. LangGraph 呼び出し〜内部ノード〜interrupt
    ev(source=EventSource.LANGGRAPH, event_type=EventType.AGENT_INVOKED,
       app="app1_inquiry_agent", thread_id="th-demo-1", summary="反響対応エージェント呼び出し")
    ev(source=EventSource.LANGGRAPH, event_type=EventType.AGENT_NODE_FINISHED,
       node_name="classify_intent", thread_id="th-demo-1", summary="意図分類: complaint_or_complex")
    ev(source=EventSource.LANGGRAPH, event_type=EventType.STATUS_CHANGED,
       thread_id="th-demo-1", summary="interrupt: 営業承認待ち", payload={"status": "waiting_human"})
    # 4. n8n 側で承認 → 再開
    ev(source=EventSource.N8N, event_type=EventType.NODE_FINISHED,
       node_name="Slackで承認依頼", summary="営業担当へ承認依頼を送信")
    ev(source=EventSource.N8N, event_type=EventType.STATUS_CHANGED,
       node_name="承認を待つ(Wait)", summary="承認されたため再開", payload={"status": "running"})
    ev(source=EventSource.LANGGRAPH, event_type=EventType.AGENT_NODE_FINISHED,
       node_name="compliance_check", thread_id="th-demo-1", summary="コンプラ違反 0件")
    ev(source=EventSource.LANGGRAPH, event_type=EventType.AGENT_FINISHED,
       thread_id="th-demo-1", summary="返信文生成まで完了")
    # 5. n8n ノード(エラー→リトライ成功)と終了
    ev(source=EventSource.N8N, event_type=EventType.NODE_FINISHED, result=EventResult.ERROR,
       node_name="CRM記録", error_message="CRM API timeout(1回目)")
    ev(source=EventSource.N8N, event_type=EventType.NODE_FINISHED,
       node_name="CRM記録", summary="リトライで記録成功")
    ev(source=EventSource.N8N, event_type=EventType.WORKFLOW_FINISHED,
       summary="反響対応ワークフロー完了")

    detail = store.get_trace_detail(tid)
    print(f"trace_id : {detail.trace_id}")
    print(f"status   : {detail.status.value}")
    print(f"n8n exec : {detail.n8n_execution_id} / apps: {detail.apps} / threads: {detail.thread_ids}")
    print(f"period   : {detail.started_at} 〜 {detail.finished_at}")
    print("-" * 100)
    for e in detail.events:
        mark = "❌" if e.result == EventResult.ERROR else "・"
        who = {"n8n": "[n8n]", "langgraph": "[LG ]", "tracker": "[trk]"}[e.source.value]
        body = e.summary or e.event_type.value
        if e.node_name:
            body = f"{e.node_name}: {body}"
        if e.error_message:
            body += f" | ERROR: {e.error_message}"
        print(f"{mark} #{e.seq:03d} {e.occurred_at[11:23]} {who} {e.event_type.value:22s} {body}")


if __name__ == "__main__":
    main()
