"""
Tests for history_list and history_search MCP tools.
"""

import json
from unittest.mock import patch

import pytest

from tools.history import HistoryListTool, HistorySearchTool
from utils.storage_backend import SQLiteStorage


@pytest.fixture
def storage(tmp_path):
    """Create a SQLiteStorage with test data."""
    db_path = str(tmp_path / "history_test.db")
    s = SQLiteStorage(db_path=db_path)

    # Populate test data
    s.save_thread(
        {
            "thread_id": "h-1",
            "tool_name": "consensus",
            "initial_context": {"step": "Evaluate Python vs Rust for CLI"},
            "status": "inactive",
            "created_at": "2026-03-20T10:00:00+00:00",
            "last_updated_at": "2026-03-20T10:05:00+00:00",
        }
    )
    s.append_turn(
        "h-1",
        {
            "role": "assistant",
            "content": "Python offers rapid development and rich ecosystem",
            "timestamp": "2026-03-20T10:01:00+00:00",
            "model_name": "gemini-2.5-flash",
            "model_provider": "google",
            "tool_name": "consensus",
            "files": ["/src/main.py"],
        },
        0,
    )
    s.append_turn(
        "h-1",
        {
            "role": "assistant",
            "content": "Rust compiles to single binary with excellent performance",
            "timestamp": "2026-03-20T10:02:00+00:00",
            "model_name": "gpt-4o",
            "model_provider": "openai",
            "tool_name": "consensus",
        },
        1,
    )

    s.save_thread(
        {
            "thread_id": "h-2",
            "tool_name": "consensus",
            "initial_context": {"step": "Review authentication middleware"},
            "status": "active",
            "created_at": "2026-03-22T10:00:00+00:00",
            "last_updated_at": "2026-03-22T10:05:00+00:00",
        }
    )
    s.append_turn(
        "h-2",
        {
            "role": "assistant",
            "content": "Auth middleware has a token validation issue",
            "timestamp": "2026-03-22T10:01:00+00:00",
            "model_name": "gpt-4o",
            "model_provider": "openai",
            "tool_name": "consensus",
            "files": ["/auth/middleware.py"],
        },
        0,
    )

    yield s
    s.shutdown()


@pytest.fixture
def mock_storage(storage):
    """Patch _get_storage to return our test storage."""
    with patch("tools.history._get_storage", return_value=storage):
        yield storage


class TestHistoryListTool:
    @pytest.fixture(autouse=True)
    def setup(self, mock_storage):
        self.tool = HistoryListTool()

    def test_requires_no_model(self):
        assert self.tool.requires_model() is False

    def test_get_name(self):
        assert self.tool.get_name() == "history_list"

    @pytest.mark.asyncio
    async def test_list_all_threads(self):
        result = await self.tool.execute({})
        output = json.loads(result[0].text)
        assert output["status"] == "success"
        data = json.loads(output["content"])
        assert data["total_count"] == 2
        assert len(data["threads"]) == 2

    @pytest.mark.asyncio
    async def test_list_with_model_filter(self):
        result = await self.tool.execute({"model_name": "gemini-2.5-flash"})
        output = json.loads(result[0].text)
        data = json.loads(output["content"])
        assert data["total_count"] == 1
        assert data["threads"][0]["thread_id"] == "h-1"

    @pytest.mark.asyncio
    async def test_list_with_date_filter(self):
        result = await self.tool.execute({"after": "2026-03-21T00:00:00+00:00"})
        output = json.loads(result[0].text)
        data = json.loads(output["content"])
        assert data["total_count"] == 1
        assert data["threads"][0]["thread_id"] == "h-2"

    @pytest.mark.asyncio
    async def test_list_pagination(self):
        result = await self.tool.execute({"limit": 1, "offset": 0})
        output = json.loads(result[0].text)
        data = json.loads(output["content"])
        assert data["total_count"] == 2
        assert len(data["threads"]) == 1
        assert data["has_more"] is True

    @pytest.mark.asyncio
    async def test_list_limit_capped_at_100(self):
        result = await self.tool.execute({"limit": 999})
        output = json.loads(result[0].text)
        data = json.loads(output["content"])
        assert data["limit"] == 100


class TestHistorySearchTool:
    @pytest.fixture(autouse=True)
    def setup(self, mock_storage):
        self.tool = HistorySearchTool()

    def test_requires_no_model(self):
        assert self.tool.requires_model() is False

    def test_get_name(self):
        assert self.tool.get_name() == "history_search"

    @pytest.mark.asyncio
    async def test_search_by_content(self):
        result = await self.tool.execute({"query": "single binary"})
        output = json.loads(result[0].text)
        assert output["status"] == "success"
        data = json.loads(output["content"])
        assert data["total_turn_matches"] >= 1
        assert any("gpt-4o" == m["model_name"] for m in data["turn_matches"])

    @pytest.mark.asyncio
    async def test_search_with_model_filter(self):
        result = await self.tool.execute(
            {
                "query": "rapid development",
                "model_name": "gemini-2.5-flash",
            }
        )
        output = json.loads(result[0].text)
        data = json.loads(output["content"])
        assert data["total_turn_matches"] >= 1

    @pytest.mark.asyncio
    async def test_search_with_file_path(self):
        result = await self.tool.execute(
            {
                "query": "token validation",
                "file_path": "/auth/middleware.py",
            }
        )
        output = json.loads(result[0].text)
        data = json.loads(output["content"])
        assert "file_matches" in data
        assert len(data["file_matches"]) >= 1

    @pytest.mark.asyncio
    async def test_search_requires_query(self):
        result = await self.tool.execute({"query": ""})
        output = json.loads(result[0].text)
        assert output["status"] == "error"

    @pytest.mark.asyncio
    async def test_search_no_results(self):
        result = await self.tool.execute({"query": "zzz_nonexistent_term_zzz"})
        output = json.loads(result[0].text)
        data = json.loads(output["content"])
        assert data["total_turn_matches"] == 0


class TestHistoryToolsWithInMemoryStorage:
    """History tools should return error when InMemoryStorage is active."""

    @pytest.mark.asyncio
    async def test_list_returns_error_with_memory_storage(self):
        from utils.storage_backend import InMemoryStorage

        mem = InMemoryStorage()
        with patch("tools.history._get_storage", return_value=mem):
            tool = HistoryListTool()
            result = await tool.execute({})
            output = json.loads(result[0].text)
            assert output["status"] == "error"
            assert "SQLite" in output["content"]
        mem.shutdown()

    @pytest.mark.asyncio
    async def test_search_returns_error_with_memory_storage(self):
        from utils.storage_backend import InMemoryStorage

        mem = InMemoryStorage()
        with patch("tools.history._get_storage", return_value=mem):
            tool = HistorySearchTool()
            result = await tool.execute({"query": "test"})
            output = json.loads(result[0].text)
            assert output["status"] == "error"
            assert "SQLite" in output["content"]
        mem.shutdown()
