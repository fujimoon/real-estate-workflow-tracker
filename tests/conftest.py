import pytest

from workflow_tracker.service.store import TrackerStore


@pytest.fixture()
def store(tmp_path) -> TrackerStore:
    """テスト毎に独立した SQLite を使う"""
    return TrackerStore(db_path=str(tmp_path / "tracker.db"))
