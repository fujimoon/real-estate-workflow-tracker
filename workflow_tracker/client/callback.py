"""LangGraph のノード完了を自動記録する callback ハンドラ

業務アプリのグラフ呼び出しに渡すだけで、LangGraph の各ノード
(chain 単位)の完了を agent_node_finished として記録できる。

    from workflow_tracker.client.callback import TrackerCallbackHandler

    handler = TrackerCallbackHandler(tracker, thread_id=thread_id)
    graph.invoke(inputs, config={"callbacks": [handler], ...})

記録の失敗で業務処理は止めない(TrackerClient 側で fail-safe)。
"""

from typing import Any
from uuid import UUID

from langchain_core.callbacks import BaseCallbackHandler

from workflow_tracker.client.tracker_client import TrackerClient

# LangGraph 内部の細かすぎる実行単位は記録しない
_IGNORED_NAMES = {"LangGraph", "RunnableSequence", "ChannelWrite", "ChannelRead", "_write"}


class TrackerCallbackHandler(BaseCallbackHandler):
    def __init__(self, client: TrackerClient, thread_id: str) -> None:
        self.client = client
        self.thread_id = thread_id
        # run_id → ノード名(on_chain_end でノード名を引くため)
        self._names: dict[UUID, str] = {}

    @staticmethod
    def _node_name(serialized: dict | None, kwargs: dict) -> str | None:
        name = (kwargs.get("name") or (serialized or {}).get("name") or "").strip()
        if not name or name in _IGNORED_NAMES or name.startswith("__"):
            return None
        return name

    def on_chain_start(
        self, serialized: dict[str, Any], inputs: dict[str, Any], *, run_id: UUID, **kwargs: Any
    ) -> None:
        name = self._node_name(serialized, kwargs)
        if name:
            self._names[run_id] = name

    def on_chain_end(self, outputs: dict[str, Any], *, run_id: UUID, **kwargs: Any) -> None:
        name = self._names.pop(run_id, None)
        if name:
            self.client.node_finished(node_name=name, thread_id=self.thread_id)

    def on_chain_error(self, error: BaseException, *, run_id: UUID, **kwargs: Any) -> None:
        name = self._names.pop(run_id, None)
        if name:
            self.client.node_finished(
                node_name=name, thread_id=self.thread_id, error=str(error)
            )
