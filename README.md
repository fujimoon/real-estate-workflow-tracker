# real-estate-workflow-tracker

n8n のワークフロー実行と LangChain/LangGraph の処理を、**独自採番した1つのトレースID
(`WFT-YYYYMMDD-XXXXXXXX`)の下にマッピングし、あたかも1本のワークフローであるかのように
記録していく**統合トレーサ。

[real-estate-dx-agents](https://github.com/fujimoon/real-estate-dx-agents) の6アプリ
(n8n → FastAPI → LangGraph という構成)の運用を想定している。

## 何を解決するか

n8n の実行ログと LangGraph のトレースは別々のシステムに残るため、
「この反響メールの対応は、いまどこで何をしていて、どこで失敗したのか」を追うには
両方を見比べる必要がある。本トラッカーは n8n の `execution_id` と LangGraph の
`thread_id` を独自の `trace_id` に紐付け、**両レイヤーのイベントを1本の時系列タイムライン**
として記録する。

```
・ #001 [n8n] workflow_started    SUUMO 反響メール受信により開始
・ #002 [n8n] node_finished       メール整形(Set): 反響メールを整形
・ #003 [LG ] agent_invoked       反響対応エージェント呼び出し
・ #004 [LG ] agent_node_finished classify_intent: 意図分類: complaint_or_complex
・ #005 [LG ] status_changed      interrupt: 営業承認待ち          ← 状態: waiting_human
・ #006 [n8n] node_finished       Slackで承認依頼
・ #007 [n8n] status_changed      承認されたため再開               ← 状態: running
・ #008 [LG ] agent_node_finished compliance_check: コンプラ違反 0件
・ #009 [LG ] agent_finished      返信文生成まで完了
❌ #010 [n8n] node_finished       CRM記録 | ERROR: CRM API timeout(1回目)
・ #011 [n8n] node_finished       CRM記録: リトライで記録成功
・ #012 [n8n] workflow_finished   反響対応ワークフロー完了          ← 状態: succeeded
```

## 記録タイミング(仕様)

| # | タイミング | イベント種別 | 呼び出し |
|---|---|---|---|
| 1 | **n8n ワークフロー開始時(最初の呼び出し)** | `workflow_started` | `POST /traces/start` — 独自の `trace_id` を採番して返す |
| 2 | **n8n のノード処理が済むたび** | `node_finished` | `POST /events` — 時刻と結果を記録。エラー時は `result=error` + `error_message` |
| 3 | **LangChain/LangGraph エージェントが呼ばれたとき** | `agent_invoked` | `POST /events` — `app` と `thread_id` が trace にマッピングされる |
| 3' | (任意)LangGraph 内部のノードが済むたび | `agent_node_finished` | `POST /events` または `TrackerCallbackHandler` で自動記録 |
| 3'' | LangGraph 処理の終了 | `agent_finished` | `POST /events` |
| 4 | **ワークフローの状態が変更されたとき** | `status_changed` | `POST /events` — `payload.status` で trace の状態が変わる(`waiting_human` / `running` / `canceled` など) |
| 5 | **n8n ワークフローの終了** | `workflow_finished` | `POST /events` — `result` に応じて `succeeded` / `failed` で確定 |

すべてのイベントは UTC の発生時刻・通し番号(`seq`)・発生元(`n8n` / `langgraph`)付きで
追記され、後から改変されない(append-only)。

## IDマッピングの仕組み

```
trace_id (独自採番: WFT-20260825-1A2B3C4D)
 ├─ n8n_workflow_id   : n8n のワークフローID($workflow.id)
 ├─ n8n_execution_id  : n8n の実行ID($execution.id)      ← n8n 側のイベント解決キー
 └─ thread_ids[]      : LangGraph の thread_id(複数可)   ← LangGraph 側のイベント解決キー
```

イベント送信時に `trace_id` を知らなくてもよい。`POST /events` は
`trace_id` / `n8n_execution_id` / `thread_id_lookup` の**どれからでも trace を解決**する。

- n8n 側: 開始時に `trace_id` を受け取るが、以降のノードは `$execution.id` を送るだけでよい
- LangGraph 側: 最初の `agent_invoked` を `n8n_execution_id` で送ると `thread_id` が
  マッピングされ、以後は `thread_id` だけで送れる(業務アプリは既に `execution_id` /
  `thread_id` を受け渡す設計になっているため、追加の持ち回りは不要)

## 状態遷移

```
running ──(status_changed: waiting_human)──> waiting_human
   ^                                             │
   └──────(status_changed: running)──────────────┘
running ──(workflow_finished result=ok)────> succeeded
running ──(workflow_finished result=error)─> failed
running ──(status_changed: canceled)───────> canceled
```

ノード単発のエラー(`node_finished` の `result=error`)は**トレースを failed にしない**
(n8n がリトライしうるため)。エラー内容は記録され、代表エラーとして trace に控えられる。
最終的な成否は `workflow_finished`(または明示的な `status_changed`)で確定する。

## セットアップと起動

```bash
uv sync
cp .env.sample .env    # APIキーは不要。DBパス等を変えたい場合のみ編集
uv run uvicorn workflow_tracker.api.main:app --reload --port 8010
```

永続化は SQLite(既定: `storage/tracker.db`)。テスト・デモは外部依存なしで動く。

```bash
uv run pytest                        # 18テスト(APIキー・ネットワーク不要)
uv run python fixtures/demo_run.py   # 1実行分を最初から最後まで記録してタイムライン表示
```

## API 仕様

### `POST /traces/start` — 【タイミング1】n8n ワークフロー開始

```json
// リクエスト
{
  "n8n_workflow_id": "wf-app1",
  "n8n_workflow_name": "アプリ1 反響対応",
  "n8n_execution_id": "12345",
  "summary": "反響メール受信により開始"
}
// レスポンス(trace_id が採番される)
{ "trace_id": "WFT-20260825-1A2B3C4D", "status": "running", "started_at": "...", ... }
```

### `POST /events` — 【タイミング2〜5】イベント追記

```json
// n8n ノード完了(エラーの例)
{
  "n8n_execution_id": "12345",
  "source": "n8n",
  "event_type": "node_finished",
  "result": "error",
  "node_name": "CRM記録",
  "error_message": "CRM API が 500 を返しました"
}

// LangGraph エージェント呼び出し(thread_id がマッピングされる)
{
  "n8n_execution_id": "12345",
  "source": "langgraph",
  "event_type": "agent_invoked",
  "app": "app1_inquiry_agent",
  "thread_id": "th-001"
}

// 状態変更(interrupt で承認待ちへ)
{
  "thread_id_lookup": "th-001",
  "source": "langgraph",
  "event_type": "status_changed",
  "payload": { "status": "waiting_human" }
}

// n8n ワークフロー終了
{ "n8n_execution_id": "12345", "source": "n8n", "event_type": "workflow_finished" }
```

レスポンスは常に `{ "trace_id", "seq", "occurred_at", "status" }`(反映後のトレース状態)。

### 参照系

| エンドポイント | 内容 |
|---|---|
| `GET /traces` | トレース一覧(新しい順、`?status=running` で絞り込み) |
| `GET /traces/{trace_id}` | 詳細 + 全イベント(タイムライン) |
| `GET /traces/{trace_id}/timeline` | 人が読みやすいテキスト行のタイムライン |
| `GET /healthz` | ヘルスチェック |

## n8n への組み込み

[n8n/workflow_with_tracker.json](n8n/workflow_with_tracker.json) が実例
(アプリ1 反響対応にトラッカー呼び出しを織り込んだもの)。要点:

1. トリガーの直後に HTTP Request で `POST /traces/start`。
   ボディに `$workflow.id` / `$workflow.name` / `$execution.id` を渡す
2. 記録したいノードの後ろに HTTP Request で `POST /events`(`event_type=node_finished`)。
   `$execution.id` を送るだけで trace が解決される
3. 承認待ち(Wait)の前後で `status_changed`(`waiting_human` → `running`)
4. 末尾で `workflow_finished`。**Error Workflow(Error Trigger)からも
   `workflow_finished` + `result=error` を送る**と、異常終了も failed で確定する

## LangGraph 業務アプリへの組み込み

`workflow_tracker.client.TrackerClient` を使う。**記録の失敗で業務処理は止めない**
(トラッカーが落ちていても警告ログのみで継続する fail-safe 設計)。

```python
from workflow_tracker.client.tracker_client import TrackerClient
from workflow_tracker.client.callback import TrackerCallbackHandler

tracker = TrackerClient(app_name="app1_inquiry_agent")  # TRACKER_BASE_URL を参照

@app.post("/agent/inquiry")
async def run_agent(payload: InquiryRequest):
    thread_id = payload.thread_id or str(uuid4())

    # 【タイミング3】エージェントが呼ばれた(execution_id → trace 解決 & thread_id 登録)
    tracker.agent_invoked(
        thread_id=thread_id,
        n8n_execution_id=payload.execution_id,
        summary="反響対応エージェント呼び出し",
    )
    try:
        result = await graph.ainvoke(
            inputs,
            config={
                "configurable": {"thread_id": thread_id},
                # 【タイミング3'】各ノードの完了を自動記録(任意)
                "callbacks": [TrackerCallbackHandler(tracker, thread_id=thread_id)],
            },
        )
        if result.get("interrupted"):
            # 【タイミング4】interrupt による承認待ち
            tracker.status_changed(thread_id=thread_id, status="waiting_human",
                                   summary="送信前承認待ち(interrupt)")
        else:
            tracker.agent_finished(thread_id=thread_id, summary="返信文生成まで完了")
        return result
    except Exception as exc:
        # LangGraph 側の失敗も1本のタイムラインに残る
        tracker.agent_finished(thread_id=thread_id, error=str(exc))
        raise
```

依存の追加は `httpx` のみ(このリポジトリを `uv add --editable` するか、
`client/` の2ファイルを業務アプリにコピーしてもよい。標準ライブラリ + httpx で完結する)。

## ディレクトリ構成

```
real-estate-workflow-tracker/
├── workflow_tracker/
│   ├── settings.py / logger.py
│   ├── models/trace.py        # Trace / TraceEvent / 状態・イベント種別の enum
│   ├── service/
│   │   ├── id_gen.py          # 独自トレースIDの採番(WFT-YYYYMMDD-8hex)
│   │   └── store.py           # SQLite ストア(採番・マッピング・状態遷移)
│   ├── api/main.py            # FastAPI(port 8010)
│   └── client/
│       ├── tracker_client.py  # 業務アプリ用クライアント(fail-safe)
│       └── callback.py        # LangGraph ノード完了の自動記録ハンドラ
├── n8n/workflow_with_tracker.json  # n8n 組み込み実例(アプリ1)
├── fixtures/demo_run.py            # 1実行分のデモ
└── tests/                          # 18テスト(外部依存なし)
```

## 本番化に向けた TODO

- SQLite → Postgres 化(複数レプリカでの書き込み集中に備える)
- LangSmith / Langfuse との突合キー連携(`trace_id` を LangSmith の `metadata` に載せる)
- タイムラインの Web UI(現在は `GET /traces/{id}/timeline` のテキスト)
- 保持期間・アーカイブポリシー(append-only のためデータは増え続ける)
- 認証(現在は認証なし。社内ネットワーク内での利用を想定)
