"""Tests for per-thread cost tracking: extraction, formatting, and viewer helpers."""

from unittest.mock import MagicMock

import pytest

from providers.openai_compatible import OpenAICompatibleProvider
from providers.shared.provider_type import ProviderType
from viewer.app import _extract_turn_cost, _extract_turn_tokens, fmtcost_filter


# Concrete subclass so we can test _extract_usage without hitting ABC errors
class _TestableProvider(OpenAICompatibleProvider):
    def get_provider_type(self) -> ProviderType:
        return ProviderType.OPENROUTER


# ---------------------------------------------------------------------------
# _extract_usage — cost from OpenAI-compatible responses
# ---------------------------------------------------------------------------


class TestExtractUsageWithCost:
    """Test cost extraction from API responses."""

    def _make_provider(self):
        """Create a minimal provider instance for testing _extract_usage."""
        return object.__new__(_TestableProvider)

    def test_extract_usage_with_cost(self):
        """OpenRouter-style response with usage.cost as a direct attribute."""
        provider = self._make_provider()
        response = MagicMock()
        response.usage.prompt_tokens = 100
        response.usage.completion_tokens = 50
        response.usage.total_tokens = 150
        response.usage.cost = 0.0042

        result = provider._extract_usage(response)

        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50
        assert result["total_tokens"] == 150
        assert result["cost"] == pytest.approx(0.0042)

    def test_extract_usage_with_cost_in_model_extra(self):
        """Pydantic v2 fallback: cost in model_extra dict."""
        provider = self._make_provider()
        response = MagicMock()
        response.usage.prompt_tokens = 200
        response.usage.completion_tokens = 80
        response.usage.total_tokens = 280
        # Simulate no direct 'cost' attribute — getattr returns None
        response.usage.cost = None
        response.usage.model_extra = {"cost": 0.0123}

        result = provider._extract_usage(response)

        assert result["cost"] == pytest.approx(0.0123)

    def test_extract_usage_without_cost(self):
        """Standard OpenAI response: no cost key present."""
        provider = self._make_provider()
        response = MagicMock()
        response.usage.prompt_tokens = 50
        response.usage.completion_tokens = 25
        response.usage.total_tokens = 75
        response.usage.cost = None
        response.usage.model_extra = None

        result = provider._extract_usage(response)

        assert "cost" not in result
        assert result["total_tokens"] == 75

    def test_extract_usage_with_invalid_cost(self):
        """Non-numeric cost is silently skipped."""
        provider = self._make_provider()
        response = MagicMock()
        response.usage.prompt_tokens = 10
        response.usage.completion_tokens = 5
        response.usage.total_tokens = 15
        response.usage.cost = "not-a-number"
        response.usage.model_extra = None

        result = provider._extract_usage(response)

        assert "cost" not in result
        assert result["total_tokens"] == 15


# ---------------------------------------------------------------------------
# fmtcost Jinja filter
# ---------------------------------------------------------------------------


class TestFmtcostFilter:
    """Test the fmtcost Jinja filter at various magnitudes."""

    def test_sub_cent(self):
        assert fmtcost_filter(0.0042) == "$0.0042"

    def test_sub_dollar(self):
        assert fmtcost_filter(0.035) == "$0.035"

    def test_dollar_plus(self):
        assert fmtcost_filter(1.5) == "$1.50"

    def test_zero(self):
        assert fmtcost_filter(0) == "$0.0000"

    def test_none(self):
        assert fmtcost_filter(None) == ""

    def test_string_numeric(self):
        assert fmtcost_filter("0.15") == "$0.150"

    def test_non_numeric_string(self):
        assert fmtcost_filter("abc") == ""


# ---------------------------------------------------------------------------
# Viewer helpers — _extract_turn_cost / _extract_turn_tokens
# ---------------------------------------------------------------------------


class TestExtractTurnCost:
    """Test viewer helper for extracting cost from turn metadata."""

    def test_with_cost(self):
        metadata = {"usage": {"cost": 0.0042, "total_tokens": 150}}
        assert _extract_turn_cost(metadata) == pytest.approx(0.0042)

    def test_missing_usage_key(self):
        assert _extract_turn_cost({"stance": "for"}) is None

    def test_none_metadata(self):
        assert _extract_turn_cost(None) is None

    def test_empty_dict(self):
        assert _extract_turn_cost({}) is None

    def test_no_cost_in_usage(self):
        metadata = {"usage": {"total_tokens": 100}}
        assert _extract_turn_cost(metadata) is None

    def test_invalid_cost_type(self):
        metadata = {"usage": {"cost": "bad"}}
        assert _extract_turn_cost(metadata) is None


class TestExtractTurnTokens:
    """Test viewer helper for extracting tokens from turn metadata."""

    def test_with_tokens(self):
        metadata = {"usage": {"input_tokens": 100, "output_tokens": 50, "total_tokens": 150}}
        result = _extract_turn_tokens(metadata)
        assert result["input_tokens"] == 100
        assert result["output_tokens"] == 50
        assert result["total_tokens"] == 150

    def test_none_metadata(self):
        result = _extract_turn_tokens(None)
        assert result == {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}

    def test_empty_usage(self):
        result = _extract_turn_tokens({"usage": {}})
        assert result["total_tokens"] == 0
