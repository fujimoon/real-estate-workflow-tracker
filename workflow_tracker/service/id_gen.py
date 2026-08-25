"""独自トレースIDの採番

形式: <PREFIX>-<YYYYMMDD>-<8桁hex>
例  : WFT-20260825-1A2B3C4D

n8n の execution_id とも LangGraph の thread_id とも別に採番し、
両者をこのIDの下にマッピングする。
"""

import secrets
from datetime import datetime, timezone

from workflow_tracker.settings import settings


def new_trace_id() -> str:
    date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
    random_part = secrets.token_hex(settings.id.random_hex_digits // 2).upper()
    return f"{settings.id.prefix}-{date_part}-{random_part}"
