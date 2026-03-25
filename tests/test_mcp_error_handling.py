import json
from types import SimpleNamespace

import pytest
from mcp.types import CallToolRequest, CallToolRequestParams

from providers.registry import ModelProviderRegistry
from server import server as mcp_server


def _install_dummy_provider(monkeypatch):
    """Ensure preflight model checks succeed without real provider configuration."""

    class DummyProvider:
        def get_provider_type(self):
            return SimpleNamespace(value="dummy")

        def get_capabilities(self, model_name):
            return SimpleNamespace(
                supports_extended_thinking=False,
                allow_code_generation=False,
                supports_images=False,
                context_window=1_000_000,
                max_image_size_mb=10,
            )

    monkeypatch.setattr(
        ModelProviderRegistry,
        "get_provider_for_model",
        classmethod(lambda cls, model_name: DummyProvider()),
    )
    monkeypatch.setattr(
        ModelProviderRegistry,
        "get_available_models",
        classmethod(lambda cls, respect_restrictions=False: {"gemini-2.5-flash": None}),
    )


@pytest.mark.asyncio
async def test_unknown_tool_returns_error_content(monkeypatch):
    """Ensure calling an unknown tool returns an error message in the response content."""

    _install_dummy_provider(monkeypatch)

    handler = mcp_server.request_handlers[CallToolRequest]

    request = CallToolRequest(params=CallToolRequestParams(name="nonexistent_tool", arguments={"prompt": "test"}))

    server_result = await handler(request)

    assert server_result.root.content, "Expected error response content"
    payload = server_result.root.content[0].text
    assert "Unknown tool: nonexistent_tool" in payload
