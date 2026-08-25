"""統合トレースのデータモデル

n8n のワークフロー実行と LangChain/LangGraph の処理を、独自採番した
1本の trace_id の下に「イベントの追記」として記録していく。
"""

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


def utcnow_iso() -> str:
    """記録は常に UTC の ISO8601 で持つ(表示時にタイムゾーン変換する)"""
    return datetime.now(timezone.utc).isoformat()


class EventSource(str, Enum):
    """イベントの発生元レイヤー"""

    N8N = "n8n"
    LANGGRAPH = "langgraph"
    TRACKER = "tracker"  # トラッカー自身による補完(自動ステータス変更など)


class EventType(str, Enum):
    """記録タイミング(仕様で求められた5種 + 内部処理の細分)"""

    # 1. n8n ワークフロー開始(最初の呼び出しタイミング)
    WORKFLOW_STARTED = "workflow_started"
    # 2. n8n のノード処理が済むたび
    NODE_FINISHED = "node_finished"
    # 3. LangChain/LangGraph エージェントが呼ばれたタイミング
    AGENT_INVOKED = "agent_invoked"
    #    (任意)LangGraph 内部のノードが済むたび
    AGENT_NODE_FINISHED = "agent_node_finished"
    #    LangGraph 処理の終了
    AGENT_FINISHED = "agent_finished"
    # 4. ワークフローの状態変更(interrupt による承認待ちなど)
    STATUS_CHANGED = "status_changed"
    # 5. n8n ワークフローの終了
    WORKFLOW_FINISHED = "workflow_finished"


class EventResult(str, Enum):
    """イベント単位の結果"""

    OK = "ok"
    ERROR = "error"


class TraceStatus(str, Enum):
    """統合トレース全体の状態"""

    RUNNING = "running"
    WAITING_HUMAN = "waiting_human"  # interrupt / Slack 承認待ちなど
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class TraceEvent(BaseModel):
    """追記されるイベント1件"""

    trace_id: str = Field(description="独自採番の統合トレースID")
    seq: int = Field(default=0, description="トレース内の通し番号(採番はストアが行う)")
    occurred_at: str = Field(default_factory=utcnow_iso, description="発生時刻(UTC ISO8601)")
    source: EventSource = Field(description="発生元(n8n / langgraph / tracker)")
    event_type: EventType = Field(description="イベント種別")
    result: EventResult = Field(default=EventResult.OK, description="結果(ok / error)")
    # n8n 側の文脈
    node_name: str | None = Field(default=None, description="n8n のノード名 / LangGraph のノード名")
    # LangGraph 側の文脈
    app: str | None = Field(default=None, description="呼ばれた業務アプリ(例: app1_inquiry_agent)")
    thread_id: str | None = Field(default=None, description="LangGraph の thread_id")
    # 内容
    summary: str = Field(default="", description="人が読める1行サマリ")
    error_message: str | None = Field(default=None, description="エラー発生時の内容")
    payload: dict = Field(default_factory=dict, description="任意の付帯情報(入出力の抜粋など)")


class Trace(BaseModel):
    """統合トレース(n8n 実行と LangGraph 処理のマッピングの親)"""

    trace_id: str = Field(description="独自採番の統合トレースID(例: WFT-20260825-1A2B3C4D)")
    status: TraceStatus = Field(default=TraceStatus.RUNNING)
    # --- n8n 側の識別子 ---
    n8n_workflow_id: str | None = Field(default=None, description="n8n のワークフローID")
    n8n_workflow_name: str | None = Field(default=None, description="n8n のワークフロー名")
    n8n_execution_id: str | None = Field(default=None, description="n8n の実行ID($execution.id)")
    # --- LangChain/LangGraph 側の識別子(1実行で複数エージェントを呼ぶ場合がある) ---
    apps: list[str] = Field(default_factory=list, description="呼ばれた業務アプリの一覧")
    thread_ids: list[str] = Field(default_factory=list, description="LangGraph の thread_id 一覧")
    # --- 時刻 ---
    started_at: str = Field(default_factory=utcnow_iso)
    finished_at: str | None = Field(default=None)
    # --- 結果 ---
    error_message: str | None = Field(default=None, description="失敗時の代表エラー")
    event_count: int = Field(default=0, description="記録済みイベント数")


class TraceDetail(Trace):
    """トレース + 全イベント(タイムライン)"""

    events: list[TraceEvent] = Field(default_factory=list)
