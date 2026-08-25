"""LangChain/LangGraph 業務アプリ側から使うトラッカークライアント

設計方針: **記録の失敗で業務処理を止めない**。
トラッカーが落ちていても警告ログを出して処理を続行する(fail-safe)。

使い方(業務アプリの FastAPI 内):

    from workflow_tracker.client.tracker_client import TrackerClient

    tracker = TrackerClient(app_name="app1_inquiry_agent")

    # LangGraph エージェントが呼ばれたタイミング
    tracker.agent_invoked(
        n8n_execution_id=payload.execution_id,
        thread_id=thread_id,
        summary="反響対応エージェント呼び出し",
    )
    try:
        result = await graph.ainvoke(...)
        tracker.agent_finished(thread_id=thread_id, summary="返信文生成まで完了")
    except Exception as exc:
        tracker.agent_finished(thread_id=thread_id, error=str(exc))
        raise
"""

import os

import httpx

from workflow_tracker.logger import logger


class TrackerClient:
    def __init__(
        self,
        app_name: str,
        base_url: str | None = None,
        timeout: float | None = None,
    ) -> None:
        self.app_name = app_name
        self.base_url = (base_url or os.getenv("TRACKER_BASE_URL", "")).rstrip("/")
        self.timeout = timeout or float(os.getenv("TRACKER_TIMEOUT_SECONDS", "2.0"))

    @property
    def enabled(self) -> bool:
        return bool(self.base_url)

    # ------------------------------------------------------------------
    def _post_event(self, body: dict) -> dict | None:
        """イベントを送信する。失敗しても例外を上げない(fail-safe)"""
        if not self.enabled:
            logger.warning(
                "[tracker] TRACKER_BASE_URL が未設定のため記録をスキップしました"
            )
            return None
        try:
            resp = httpx.post(
                f"{self.base_url}/events", json=body, timeout=self.timeout
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:  # noqa: BLE001 - 記録失敗で業務を止めない
            logger.warning(f"[tracker] イベント記録に失敗しました(処理は継続): {exc}")
            return None

    # ------------------------------------------------------------------
    # LangGraph 側の記録タイミング
    # ------------------------------------------------------------------
    def agent_invoked(
        self,
        thread_id: str,
        n8n_execution_id: str | None = None,
        trace_id: str | None = None,
        summary: str = "",
        payload: dict | None = None,
    ) -> str | None:
        """LangGraph エージェントが呼ばれたタイミングで記録する

        trace_id が無くても n8n_execution_id から解決される。
        戻り値は解決された trace_id(記録失敗時は None)。
        """
        result = self._post_event(
            {
                "trace_id": trace_id,
                "n8n_execution_id": n8n_execution_id,
                "source": "langgraph",
                "event_type": "agent_invoked",
                "app": self.app_name,
                "thread_id": thread_id,
                "summary": summary or f"{self.app_name} 呼び出し",
                "payload": payload or {},
            }
        )
        return result["trace_id"] if result else None

    def node_finished(
        self,
        node_name: str,
        thread_id: str,
        error: str | None = None,
        summary: str = "",
        payload: dict | None = None,
    ) -> None:
        """LangGraph 内部のノードが済むたびの記録(任意・細粒度)"""
        self._post_event(
            {
                "thread_id_lookup": thread_id,
                "source": "langgraph",
                "event_type": "agent_node_finished",
                "result": "error" if error else "ok",
                "node_name": node_name,
                "app": self.app_name,
                "thread_id": thread_id,
                "summary": summary or f"ノード {node_name} 完了",
                "error_message": error,
                "payload": payload or {},
            }
        )

    def status_changed(
        self,
        thread_id: str,
        status: str,
        summary: str = "",
    ) -> None:
        """状態変更(例: interrupt で waiting_human、承認後に running へ戻す)"""
        self._post_event(
            {
                "thread_id_lookup": thread_id,
                "source": "langgraph",
                "event_type": "status_changed",
                "app": self.app_name,
                "thread_id": thread_id,
                "summary": summary or f"状態変更: {status}",
                "payload": {"status": status},
            }
        )

    def agent_finished(
        self,
        thread_id: str,
        error: str | None = None,
        summary: str = "",
        payload: dict | None = None,
    ) -> None:
        """LangGraph 処理の終了タイミングで記録する"""
        self._post_event(
            {
                "thread_id_lookup": thread_id,
                "source": "langgraph",
                "event_type": "agent_finished",
                "result": "error" if error else "ok",
                "app": self.app_name,
                "thread_id": thread_id,
                "summary": summary or f"{self.app_name} 終了",
                "error_message": error,
                "payload": payload or {},
            }
        )
