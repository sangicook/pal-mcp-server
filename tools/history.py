"""
History tools — query conversation history stored in SQLite.

Provides two MCP tools:
- history_list: List/filter past conversation threads
- history_search: Search turn content by text, model, provider, or file

Both tools require SQLite storage (the default). They do not call AI models.
"""

import json
import logging
from typing import Any, Optional

from mcp.types import TextContent

from tools.models import ToolModelCategory, ToolOutput
from tools.shared.base_models import ToolRequest
from tools.shared.base_tool import BaseTool

logger = logging.getLogger(__name__)


def _get_sqlite_storage():
    """Get SQLiteStorage instance or None if not available."""
    from utils.storage_backend import SQLiteStorage

    storage = _get_storage()
    if isinstance(storage, SQLiteStorage):
        return storage
    return None


def _get_storage():
    """Get the storage backend. Separated for testability."""
    from utils.storage_backend import get_storage_backend

    return get_storage_backend()


class HistoryListTool(BaseTool):
    """List and filter past conversation threads."""

    def get_name(self) -> str:
        return "history_list"

    def get_description(self) -> str:
        return (
            "List past conversation threads with optional filters. "
            "Filter by tool, model, date range. Supports pagination."
        )

    def get_input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "tool_name": {
                    "type": "string",
                    "description": "Filter by tool name (e.g., 'consensus')",
                },
                "model_name": {
                    "type": "string",
                    "description": "Filter by model used in turns (e.g., 'gemini-2.5-flash')",
                },
                "after": {
                    "type": "string",
                    "description": "ISO date — only threads created after this date",
                },
                "before": {
                    "type": "string",
                    "description": "ISO date — only threads created before this date",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results to return (default 20, max 100)",
                    "minimum": 1,
                    "maximum": 100,
                },
                "offset": {
                    "type": "integer",
                    "description": "Pagination offset (default 0)",
                    "minimum": 0,
                },
            },
            "required": [],
            "additionalProperties": False,
        }

    def get_annotations(self) -> Optional[dict[str, Any]]:
        return {"readOnlyHint": True}

    def get_system_prompt(self) -> str:
        return ""

    def get_request_model(self):
        return ToolRequest

    def requires_model(self) -> bool:
        return False

    async def prepare_prompt(self, request) -> str:
        return ""

    def format_response(self, response: str, request, model_info=None) -> str:
        return response

    async def execute(self, arguments: dict[str, Any]) -> list[TextContent]:
        storage = _get_sqlite_storage()
        if storage is None:
            output = ToolOutput(
                status="error",
                content="History queries require SQLite storage (the default). "
                "Set PAL_STORAGE_BACKEND=sqlite or remove the env var.",
                content_type="text",
            )
            return [TextContent(type="text", text=output.model_dump_json())]

        try:
            limit = min(arguments.get("limit", 20), 100)
            offset = arguments.get("offset", 0)

            threads, total_count = storage.list_threads(
                tool_name=arguments.get("tool_name"),
                model_name=arguments.get("model_name"),
                after=arguments.get("after"),
                before=arguments.get("before"),
                limit=limit,
                offset=offset,
            )

            result = {
                "threads": threads,
                "total_count": total_count,
                "has_more": (offset + limit) < total_count,
                "limit": limit,
                "offset": offset,
            }

            output = ToolOutput(
                status="success",
                content=json.dumps(result, indent=2, ensure_ascii=False),
                content_type="text",
                metadata={
                    "tool_name": self.get_name(),
                    "total_count": total_count,
                    "returned": len(threads),
                },
            )
            return [TextContent(type="text", text=output.model_dump_json())]

        except Exception as e:
            logger.error(f"Error in history_list: {e}")
            output = ToolOutput(
                status="error",
                content=f"Error querying history: {e}",
                content_type="text",
            )
            return [TextContent(type="text", text=output.model_dump_json())]

    def get_model_category(self) -> ToolModelCategory:
        return ToolModelCategory.FAST_RESPONSE


class HistorySearchTool(BaseTool):
    """Search conversation turn content."""

    def get_name(self) -> str:
        return "history_search"

    def get_description(self) -> str:
        return (
            "Search past conversation content by text, model, provider, or file path. "
            "Returns matching turns with thread context."
        )

    def get_input_schema(self) -> dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Search text to find in conversation content (required)",
                },
                "model_name": {
                    "type": "string",
                    "description": "Filter by model name (e.g., 'gpt-4o')",
                },
                "model_provider": {
                    "type": "string",
                    "description": "Filter by provider (e.g., 'google', 'openai')",
                },
                "tool_name": {
                    "type": "string",
                    "description": "Filter by tool name (e.g., 'consensus')",
                },
                "file_path": {
                    "type": "string",
                    "description": "Find threads that reference this file path",
                },
                "limit": {
                    "type": "integer",
                    "description": "Max results (default 10, max 50)",
                    "minimum": 1,
                    "maximum": 50,
                },
            },
            "required": ["query"],
            "additionalProperties": False,
        }

    def get_annotations(self) -> Optional[dict[str, Any]]:
        return {"readOnlyHint": True}

    def get_system_prompt(self) -> str:
        return ""

    def get_request_model(self):
        return ToolRequest

    def requires_model(self) -> bool:
        return False

    async def prepare_prompt(self, request) -> str:
        return ""

    def format_response(self, response: str, request, model_info=None) -> str:
        return response

    async def execute(self, arguments: dict[str, Any]) -> list[TextContent]:
        storage = _get_sqlite_storage()
        if storage is None:
            output = ToolOutput(
                status="error",
                content="History queries require SQLite storage (the default). "
                "Set PAL_STORAGE_BACKEND=sqlite or remove the env var.",
                content_type="text",
            )
            return [TextContent(type="text", text=output.model_dump_json())]

        query = arguments.get("query", "")
        if not query:
            output = ToolOutput(
                status="error",
                content="The 'query' field is required for history_search.",
                content_type="text",
            )
            return [TextContent(type="text", text=output.model_dump_json())]

        try:
            limit = min(arguments.get("limit", 10), 50)
            results = []

            # Text search on turn content
            turn_results = storage.search_turns(
                query=query,
                model_name=arguments.get("model_name"),
                model_provider=arguments.get("model_provider"),
                tool_name=arguments.get("tool_name"),
                limit=limit,
            )
            results.extend(turn_results)

            # File path search (additive)
            file_path = arguments.get("file_path")
            if file_path:
                file_results = storage.find_threads_by_file(file_path)
                # Add file results as a separate section
                result_data = {
                    "turn_matches": results,
                    "file_matches": file_results,
                    "total_turn_matches": len(results),
                    "total_file_matches": len(file_results),
                }
            else:
                result_data = {
                    "turn_matches": results,
                    "total_turn_matches": len(results),
                }

            output = ToolOutput(
                status="success",
                content=json.dumps(result_data, indent=2, ensure_ascii=False),
                content_type="text",
                metadata={
                    "tool_name": self.get_name(),
                    "query": query,
                    "total_matches": len(results),
                },
            )
            return [TextContent(type="text", text=output.model_dump_json())]

        except Exception as e:
            logger.error(f"Error in history_search: {e}")
            output = ToolOutput(
                status="error",
                content=f"Error searching history: {e}",
                content_type="text",
            )
            return [TextContent(type="text", text=output.model_dump_json())]

    def get_model_category(self) -> ToolModelCategory:
        return ToolModelCategory.FAST_RESPONSE
