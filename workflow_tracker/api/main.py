"""real-estate-workflow-tracker の FastAPI

n8n のワークフローと LangChain/LangGraph の処理を、独自採番した trace_id の下に
1本のワークフローとして記録する。

起動:
    uv run uvicorn workflow_tracker.api.main:app --reload --port 8010

記録タイミング(仕様):
    1. n8n ワークフロー開始時         → POST /traces/start
    2. n8n の各ノード処理が済むたび    → POST /events (event_type=node_finished)
    3. LangGraph エージェント呼び出し  → POST /events (event_type=agent_invoked)
       LangGraph 処理の終了            → POST /events (event_type=agent_finished)
    4. ワークフローの状態変更          → POST /events (event_type=status_changed)
    5. n8n ワークフローの終了          → POST /events (event_type=workflow_finished)
"""

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from workflow_tracker.logger import logger
from workflow_tracker.models.trace import (
    EventResult,
    EventSource,
    EventType,
    Trace,
    TraceDetail,
    TraceEvent,
)
from workflow_tracker.service.store import TrackerStore

app = FastAPI(
    title="real-estate-workflow-tracker",
    description="n8n × LangChain/LangGraph 統合ワークフロートレーサ",
    version="0.1.0",
)

store = TrackerStore()


# ----------------------------------------------------------------------
# リクエストモデル
# ----------------------------------------------------------------------
class StartTraceRequest(BaseModel):
    """n8n ワークフロー開始時の記録(最初の呼び出し)"""

    n8n_workflow_id: str | None = Field(default=None, description="n8n のワークフローID")
    n8n_workflow_name: str | None = Field(default=None, description="n8n のワークフロー名")
    n8n_execution_id: str | None = Field(
        default=None, description="n8n の実行ID($execution.id)。以降のイベント解決に使う"
    )
    summary: str = Field(default="", description="人が読める1行サマリ")
    payload: dict = Field(default_factory=dict, description="任意の付帯情報")


class AppendEventRequest(BaseModel):
    """イベントの追記。トレースは trace_id / n8n_execution_id / thread_id のどれでも特定できる"""

    trace_id: str | None = Field(default=None, description="統合トレースID")
    n8n_execution_id: str | None = Field(
        default=None, description="trace_id が手元に無い場合の解決キー(n8n 実行ID)"
    )
    thread_id_lookup: str | None = Field(
        default=None, description="trace_id が手元に無い場合の解決キー(LangGraph thread_id)"
    )

    source: EventSource = Field(description="発生元(n8n / langgraph / tracker)")
    event_type: EventType = Field(description="イベント種別")
    result: EventResult = Field(default=EventResult.OK, description="結果(ok / error)")
    node_name: str | None = Field(default=None, description="n8n / LangGraph のノード名")
    app: str | None = Field(default=None, description="業務アプリ名(例: app1_inquiry_agent)")
    thread_id: str | None = Field(default=None, description="LangGraph の thread_id(マッピング登録)")
    summary: str = Field(default="", description="人が読める1行サマリ")
    error_message: str | None = Field(default=None, description="エラー発生時はその内容")
    payload: dict = Field(default_factory=dict, description="任意の付帯情報")


class AppendEventResponse(BaseModel):
    trace_id: str
    seq: int
    occurred_at: str
    status: str = Field(description="イベント反映後のトレース状態")


# ----------------------------------------------------------------------
# エンドポイント
# ----------------------------------------------------------------------
@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/traces/start", response_model=Trace)
async def start_trace(req: StartTraceRequest) -> Trace:
    """【記録タイミング1】n8n ワークフロー開始。独自の trace_id を採番して返す"""
    return store.start_trace(
        n8n_workflow_id=req.n8n_workflow_id,
        n8n_workflow_name=req.n8n_workflow_name,
        n8n_execution_id=req.n8n_execution_id,
        summary=req.summary,
        payload=req.payload,
    )


@app.post("/events", response_model=AppendEventResponse)
async def append_event(req: AppendEventRequest) -> AppendEventResponse:
    """【記録タイミング2〜5】イベントの追記

    - node_finished      : n8n のノード処理が済むたび(エラー時は result=error + error_message)
    - agent_invoked      : LangGraph エージェントが呼ばれた(app / thread_id がマッピング登録される)
    - agent_node_finished: LangGraph 内部のノードが済むたび(任意)
    - agent_finished     : LangGraph 処理の終了
    - status_changed     : 状態変更(payload.status に waiting_human / running / canceled 等)
    - workflow_finished  : n8n ワークフローの終了(result に応じて succeeded / failed で確定)
    """
    trace_id = store.resolve_trace_id(
        trace_id=req.trace_id,
        n8n_execution_id=req.n8n_execution_id,
        thread_id=req.thread_id_lookup or req.thread_id,
    )
    if trace_id is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "trace を特定できませんでした。trace_id / n8n_execution_id / "
                "thread_id のいずれかを正しく指定してください"
            ),
        )
    event = store.append_event(
        TraceEvent(
            trace_id=trace_id,
            source=req.source,
            event_type=req.event_type,
            result=req.result,
            node_name=req.node_name,
            app=req.app,
            thread_id=req.thread_id,
            summary=req.summary,
            error_message=req.error_message,
            payload=req.payload,
        )
    )
    trace = store.get_trace(trace_id)
    logger.info(
        f"[event] {trace_id} #{event.seq} {req.source.value}/{req.event_type.value}"
        f" result={req.result.value} node={req.node_name or '-'}"
    )
    return AppendEventResponse(
        trace_id=trace_id,
        seq=event.seq,
        occurred_at=event.occurred_at,
        status=trace.status.value,
    )


@app.get("/traces", response_model=list[Trace])
async def list_traces(
    limit: int = Query(default=50, le=500),
    status: str | None = Query(default=None, description="状態で絞り込み"),
) -> list[Trace]:
    """トレースの一覧(新しい順)"""
    return store.list_traces(limit=limit, status=status)


@app.get("/traces/{trace_id}", response_model=TraceDetail)
async def get_trace(trace_id: str) -> TraceDetail:
    """トレース1件の詳細(全イベントのタイムライン付き)"""
    try:
        return store.get_trace_detail(trace_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.get("/traces/{trace_id}/timeline")
async def get_timeline(trace_id: str) -> dict:
    """人が読みやすいタイムライン(テキスト行)を返す"""
    try:
        detail = store.get_trace_detail(trace_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    lines = []
    for e in detail.events:
        mark = "❌" if e.result == EventResult.ERROR else "・"
        who = {"n8n": "[n8n]", "langgraph": "[LG ]", "tracker": "[trk]"}[e.source.value]
        body = e.summary or e.event_type.value
        if e.node_name:
            body = f"{e.node_name}: {body}"
        if e.error_message:
            body += f" | ERROR: {e.error_message}"
        lines.append(f"{mark} #{e.seq:03d} {e.occurred_at} {who} {e.event_type.value:20s} {body}")
    return {
        "trace_id": detail.trace_id,
        "status": detail.status.value,
        "n8n_execution_id": detail.n8n_execution_id,
        "apps": detail.apps,
        "thread_ids": detail.thread_ids,
        "started_at": detail.started_at,
        "finished_at": detail.finished_at,
        "timeline": lines,
    }
