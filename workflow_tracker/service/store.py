"""SQLite ベースの永続化ストア

- traces  : 統合トレース(n8n 実行 ↔ LangGraph 処理のマッピングの親)
- events  : 追記専用のイベント(トレース内で seq を採番)

複数プロセス(n8n からの HTTP と業務アプリからの HTTP)から同時に書かれるため、
書き込みはトランザクション + BEGIN IMMEDIATE で直列化する。
"""

import json
import sqlite3
import threading
from pathlib import Path

from workflow_tracker.logger import logger
from workflow_tracker.models.trace import (
    EventResult,
    EventSource,
    EventType,
    Trace,
    TraceDetail,
    TraceEvent,
    TraceStatus,
    utcnow_iso,
)
from workflow_tracker.service.id_gen import new_trace_id
from workflow_tracker.settings import settings

_SCHEMA = """
CREATE TABLE IF NOT EXISTS traces (
    trace_id          TEXT PRIMARY KEY,
    status            TEXT NOT NULL,
    n8n_workflow_id   TEXT,
    n8n_workflow_name TEXT,
    n8n_execution_id  TEXT,
    apps              TEXT NOT NULL DEFAULT '[]',
    thread_ids        TEXT NOT NULL DEFAULT '[]',
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    error_message     TEXT,
    event_count       INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_traces_n8n_execution ON traces (n8n_execution_id);
CREATE TABLE IF NOT EXISTS events (
    trace_id      TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    occurred_at   TEXT NOT NULL,
    source        TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    result        TEXT NOT NULL,
    node_name     TEXT,
    app           TEXT,
    thread_id     TEXT,
    summary       TEXT NOT NULL DEFAULT '',
    error_message TEXT,
    payload       TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (trace_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_events_thread ON events (thread_id);
"""


class TrackerStore:
    def __init__(self, db_path: str | None = None) -> None:
        path = Path(db_path or settings.TRACKER_DB_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(_SCHEMA)
            self._conn.commit()

    # ------------------------------------------------------------------
    # トレースの開始(n8n ワークフロー開始時に呼ばれる)
    # ------------------------------------------------------------------
    def start_trace(
        self,
        n8n_workflow_id: str | None = None,
        n8n_workflow_name: str | None = None,
        n8n_execution_id: str | None = None,
        summary: str = "",
        payload: dict | None = None,
    ) -> Trace:
        trace = Trace(
            trace_id=new_trace_id(),
            n8n_workflow_id=n8n_workflow_id,
            n8n_workflow_name=n8n_workflow_name,
            n8n_execution_id=n8n_execution_id,
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            self._conn.execute(
                "INSERT INTO traces (trace_id, status, n8n_workflow_id, n8n_workflow_name,"
                " n8n_execution_id, apps, thread_ids, started_at, event_count)"
                " VALUES (?, ?, ?, ?, ?, '[]', '[]', ?, 0)",
                (
                    trace.trace_id,
                    trace.status.value,
                    n8n_workflow_id,
                    n8n_workflow_name,
                    n8n_execution_id,
                    trace.started_at,
                ),
            )
            self._append_event_locked(
                TraceEvent(
                    trace_id=trace.trace_id,
                    source=EventSource.N8N,
                    event_type=EventType.WORKFLOW_STARTED,
                    summary=summary or f"n8n ワークフロー開始: {n8n_workflow_name or n8n_workflow_id or '-'}",
                    payload=payload or {},
                )
            )
            self._conn.commit()
        logger.info(
            f"[start_trace] {trace.trace_id} n8n_execution_id={n8n_execution_id}"
        )
        return self.get_trace(trace.trace_id)

    # ------------------------------------------------------------------
    # トレースの解決(trace_id / n8n_execution_id / thread_id のどれからでも)
    # ------------------------------------------------------------------
    def resolve_trace_id(
        self,
        trace_id: str | None = None,
        n8n_execution_id: str | None = None,
        thread_id: str | None = None,
    ) -> str | None:
        if trace_id:
            row = self._conn.execute(
                "SELECT trace_id FROM traces WHERE trace_id = ?", (trace_id,)
            ).fetchone()
            return row["trace_id"] if row else None
        if n8n_execution_id:
            row = self._conn.execute(
                "SELECT trace_id FROM traces WHERE n8n_execution_id = ?"
                " ORDER BY started_at DESC LIMIT 1",
                (n8n_execution_id,),
            ).fetchone()
            if row:
                return row["trace_id"]
        if thread_id:
            row = self._conn.execute(
                "SELECT trace_id FROM traces WHERE thread_ids LIKE ?"
                " ORDER BY started_at DESC LIMIT 1",
                (f'%"{thread_id}"%',),
            ).fetchone()
            if row:
                return row["trace_id"]
        return None

    # ------------------------------------------------------------------
    # イベントの追記
    # ------------------------------------------------------------------
    def append_event(self, event: TraceEvent) -> TraceEvent:
        """イベントを追記し、トレースの状態・マッピングを自動更新する"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            stored = self._append_event_locked(event)
            self._apply_side_effects_locked(stored)
            self._conn.commit()
        return stored

    def _append_event_locked(self, event: TraceEvent) -> TraceEvent:
        row = self._conn.execute(
            "SELECT event_count FROM traces WHERE trace_id = ?", (event.trace_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"trace not found: {event.trace_id}")
        event.seq = row["event_count"] + 1
        self._conn.execute(
            "INSERT INTO events (trace_id, seq, occurred_at, source, event_type, result,"
            " node_name, app, thread_id, summary, error_message, payload)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event.trace_id,
                event.seq,
                event.occurred_at,
                event.source.value,
                event.event_type.value,
                event.result.value,
                event.node_name,
                event.app,
                event.thread_id,
                event.summary,
                event.error_message,
                json.dumps(event.payload, ensure_ascii=False),
            ),
        )
        self._conn.execute(
            "UPDATE traces SET event_count = ? WHERE trace_id = ?",
            (event.seq, event.trace_id),
        )
        return event

    def _apply_side_effects_locked(self, event: TraceEvent) -> None:
        """イベント種別に応じてトレース本体を自動更新する

        - agent_invoked      : apps / thread_ids にマッピングを追加
        - status_changed     : payload["status"] があればその状態へ
        - workflow_finished  : finished_at を打ち、result に応じて succeeded / failed
        - result=error       : 代表エラーを控える(状態は workflow_finished / status_changed で確定)
        """
        row = self._conn.execute(
            "SELECT apps, thread_ids, status, error_message FROM traces WHERE trace_id = ?",
            (event.trace_id,),
        ).fetchone()
        apps = json.loads(row["apps"])
        thread_ids = json.loads(row["thread_ids"])
        status = row["status"]
        error_message = row["error_message"]

        if event.event_type == EventType.AGENT_INVOKED:
            if event.app and event.app not in apps:
                apps.append(event.app)
            if event.thread_id and event.thread_id not in thread_ids:
                thread_ids.append(event.thread_id)
        if event.thread_id and event.thread_id not in thread_ids:
            thread_ids.append(event.thread_id)

        if event.result == EventResult.ERROR and event.error_message:
            error_message = event.error_message

        finished_at = None
        if event.event_type == EventType.STATUS_CHANGED:
            requested = str(event.payload.get("status", "")).strip()
            if requested in {s.value for s in TraceStatus}:
                status = requested
                if requested in (
                    TraceStatus.SUCCEEDED.value,
                    TraceStatus.FAILED.value,
                    TraceStatus.CANCELED.value,
                ):
                    finished_at = event.occurred_at
        elif event.event_type == EventType.WORKFLOW_FINISHED:
            finished_at = event.occurred_at
            status = (
                TraceStatus.FAILED.value
                if event.result == EventResult.ERROR
                else TraceStatus.SUCCEEDED.value
            )

        self._conn.execute(
            "UPDATE traces SET apps = ?, thread_ids = ?, status = ?, error_message = ?,"
            " finished_at = COALESCE(?, finished_at) WHERE trace_id = ?",
            (
                json.dumps(apps, ensure_ascii=False),
                json.dumps(thread_ids, ensure_ascii=False),
                status,
                error_message,
                finished_at,
                event.trace_id,
            ),
        )

    # ------------------------------------------------------------------
    # 参照
    # ------------------------------------------------------------------
    def get_trace(self, trace_id: str) -> Trace:
        row = self._conn.execute(
            "SELECT * FROM traces WHERE trace_id = ?", (trace_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"trace not found: {trace_id}")
        return self._row_to_trace(row)

    def get_trace_detail(self, trace_id: str) -> TraceDetail:
        trace = self.get_trace(trace_id)
        rows = self._conn.execute(
            "SELECT * FROM events WHERE trace_id = ? ORDER BY seq", (trace_id,)
        ).fetchall()
        events = [
            TraceEvent(
                trace_id=r["trace_id"],
                seq=r["seq"],
                occurred_at=r["occurred_at"],
                source=EventSource(r["source"]),
                event_type=EventType(r["event_type"]),
                result=EventResult(r["result"]),
                node_name=r["node_name"],
                app=r["app"],
                thread_id=r["thread_id"],
                summary=r["summary"],
                error_message=r["error_message"],
                payload=json.loads(r["payload"]),
            )
            for r in rows
        ]
        return TraceDetail(**trace.model_dump(), events=events)

    def list_traces(self, limit: int = 50, status: str | None = None) -> list[Trace]:
        if status:
            rows = self._conn.execute(
                "SELECT * FROM traces WHERE status = ? ORDER BY started_at DESC LIMIT ?",
                (status, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM traces ORDER BY started_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [self._row_to_trace(r) for r in rows]

    @staticmethod
    def _row_to_trace(row: sqlite3.Row) -> Trace:
        return Trace(
            trace_id=row["trace_id"],
            status=TraceStatus(row["status"]),
            n8n_workflow_id=row["n8n_workflow_id"],
            n8n_workflow_name=row["n8n_workflow_name"],
            n8n_execution_id=row["n8n_execution_id"],
            apps=json.loads(row["apps"]),
            thread_ids=json.loads(row["thread_ids"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            error_message=row["error_message"],
            event_count=row["event_count"],
        )
