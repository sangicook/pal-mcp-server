"""Tests for DISABLED_TOOLS environment variable functionality."""

import logging
import os
from unittest.mock import patch

import pytest

from server import (
    apply_tool_filter,
    parse_disabled_tools_env,
    validate_disabled_tools,
)


# Mock the tool classes since we're testing the filtering logic
class MockTool:
    def __init__(self, name):
        self.name = name


class TestDisabledTools:
    """Test suite for DISABLED_TOOLS functionality."""

    def test_parse_disabled_tools_empty(self):
        """Empty string returns empty set (no tools disabled)."""
        with patch.dict(os.environ, {"DISABLED_TOOLS": ""}):
            assert parse_disabled_tools_env() == set()

    def test_parse_disabled_tools_not_set(self):
        """Unset variable returns empty set."""
        with patch.dict(os.environ, {}, clear=True):
            # Ensure DISABLED_TOOLS is not in environment
            if "DISABLED_TOOLS" in os.environ:
                del os.environ["DISABLED_TOOLS"]
            assert parse_disabled_tools_env() == set()

    def test_parse_disabled_tools_single(self):
        """Single tool name parsed correctly."""
        with patch.dict(os.environ, {"DISABLED_TOOLS": "consensus"}):
            assert parse_disabled_tools_env() == {"consensus"}

    def test_parse_disabled_tools_multiple(self):
        """Multiple tools with spaces parsed correctly."""
        with patch.dict(os.environ, {"DISABLED_TOOLS": "consensus, listmodels, version"}):
            assert parse_disabled_tools_env() == {"consensus", "listmodels", "version"}

    def test_parse_disabled_tools_extra_spaces(self):
        """Extra spaces and empty items handled correctly."""
        with patch.dict(os.environ, {"DISABLED_TOOLS": " consensus , , listmodels ,  "}):
            assert parse_disabled_tools_env() == {"consensus", "listmodels"}

    def test_parse_disabled_tools_duplicates(self):
        """Duplicate entries handled correctly (set removes duplicates)."""
        with patch.dict(os.environ, {"DISABLED_TOOLS": "consensus,listmodels,consensus"}):
            assert parse_disabled_tools_env() == {"consensus", "listmodels"}

    def test_tool_filtering_logic(self):
        """Test the complete filtering logic using the actual server functions."""
        # Simulate ALL_TOOLS
        ALL_TOOLS = {
            "consensus": MockTool("consensus"),
            "version": MockTool("version"),
            "listmodels": MockTool("listmodels"),
        }

        # Test case 1: No tools disabled
        disabled_tools = set()
        enabled_tools = apply_tool_filter(ALL_TOOLS, disabled_tools)

        assert len(enabled_tools) == 3  # All tools included
        assert set(enabled_tools.keys()) == set(ALL_TOOLS.keys())

        # Test case 2: Disable consensus
        disabled_tools = {"consensus"}
        enabled_tools = apply_tool_filter(ALL_TOOLS, disabled_tools)

        assert len(enabled_tools) == 2  # version, listmodels
        assert "consensus" not in enabled_tools
        assert "version" in enabled_tools
        assert "listmodels" in enabled_tools

        # Test case 3: Attempt to disable essential tools
        disabled_tools = {"version", "consensus"}
        enabled_tools = apply_tool_filter(ALL_TOOLS, disabled_tools)

        assert "version" in enabled_tools  # Essential tool not disabled
        assert "consensus" not in enabled_tools  # Regular tool disabled
        assert "listmodels" in enabled_tools  # Essential tool included

    def test_unknown_tools_warning(self, caplog):
        """Test that unknown tool names generate appropriate warnings."""
        ALL_TOOLS = {
            "consensus": MockTool("consensus"),
            "version": MockTool("version"),
            "listmodels": MockTool("listmodels"),
        }
        disabled_tools = {"consensus", "unknown_tool", "another_unknown"}

        with caplog.at_level(logging.WARNING):
            validate_disabled_tools(disabled_tools, ALL_TOOLS)
            assert "Unknown tools in DISABLED_TOOLS: ['another_unknown', 'unknown_tool']" in caplog.text

    def test_essential_tools_warning(self, caplog):
        """Test warning when trying to disable essential tools."""
        ALL_TOOLS = {
            "consensus": MockTool("consensus"),
            "version": MockTool("version"),
            "listmodels": MockTool("listmodels"),
        }
        disabled_tools = {"version", "consensus"}

        with caplog.at_level(logging.WARNING):
            validate_disabled_tools(disabled_tools, ALL_TOOLS)
            assert "Cannot disable essential tools: ['version']" in caplog.text

    @pytest.mark.parametrize(
        "env_value,expected",
        [
            ("", set()),  # Empty string
            ("   ", set()),  # Only spaces
            (",,,", set()),  # Only commas
            ("consensus", {"consensus"}),  # Single tool
            ("consensus,listmodels", {"consensus", "listmodels"}),  # Multiple tools
            ("consensus, listmodels, version", {"consensus", "listmodels", "version"}),  # With spaces
            ("consensus,listmodels,consensus", {"consensus", "listmodels"}),  # Duplicates
        ],
    )
    def test_parse_disabled_tools_parametrized(self, env_value, expected):
        """Parametrized tests for various input formats."""
        with patch.dict(os.environ, {"DISABLED_TOOLS": env_value}):
            assert parse_disabled_tools_env() == expected
