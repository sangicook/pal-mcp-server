"""
Test model metadata preservation during conversation continuation.

This test verifies that when using continuation_id without specifying a model,
the system correctly retrieves and uses the model from the previous conversation
turn instead of defaulting to DEFAULT_MODEL or the custom provider's default.

Bug: https://github.com/BeehiveInnovations/pal-mcp-server/issues/111
"""

from unittest.mock import MagicMock, patch

import pytest

from server import reconstruct_thread_context
from utils.conversation_memory import add_turn, create_thread, get_thread
from utils.model_context import ModelContext


class TestModelMetadataContinuation:
    """Test model metadata preservation during conversation continuation."""

    @pytest.mark.asyncio
    async def test_no_previous_assistant_turn_defaults(self):
        """Test behavior when there's no previous assistant turn."""
        # Save and set DEFAULT_MODEL for test
        import importlib
        import os

        original_default = os.environ.get("DEFAULT_MODEL", "")
        os.environ["DEFAULT_MODEL"] = "auto"
        import config
        import utils.model_context

        importlib.reload(config)
        importlib.reload(utils.model_context)

        try:
            thread_id = create_thread("consensus", {"prompt": "test"})

            # Only add user turns
            add_turn(thread_id, "user", "First question")
            add_turn(thread_id, "user", "Second question")

            arguments = {"continuation_id": thread_id}

            # Mock dependencies
            with patch("utils.model_context.ModelContext.calculate_token_allocation") as mock_calc:
                mock_calc.return_value = MagicMock(
                    total_tokens=200000,
                    content_tokens=160000,
                    response_tokens=40000,
                    file_tokens=64000,
                    history_tokens=64000,
                )

                with patch("utils.conversation_memory.build_conversation_history") as mock_build:
                    mock_build.return_value = ("=== CONVERSATION HISTORY ===\n", 1000)

                    # Call the actual function
                    enhanced_args = await reconstruct_thread_context(arguments)

                    # Should not have set a model
                    assert enhanced_args.get("model") is None

                    # ModelContext should use DEFAULT_MODEL
                    model_context = ModelContext.from_arguments(enhanced_args)
                    from config import DEFAULT_MODEL

                    assert model_context.model_name == DEFAULT_MODEL
        finally:
            # Restore original value
            if original_default:
                os.environ["DEFAULT_MODEL"] = original_default
            else:
                os.environ.pop("DEFAULT_MODEL", None)
            importlib.reload(config)
            importlib.reload(utils.model_context)

    @pytest.mark.asyncio
    async def test_explicit_model_overrides_previous_turn(self):
        """Test that explicitly specifying a model overrides the previous turn's model."""
        thread_id = create_thread("consensus", {"prompt": "test"})
        add_turn(thread_id, "assistant", "Response", model_name="gemini-2.5-flash", model_provider="google")

        arguments = {"continuation_id": thread_id, "model": "o3"}  # Explicitly specified

        # Mock dependencies
        with patch("utils.model_context.ModelContext.calculate_token_allocation") as mock_calc:
            mock_calc.return_value = MagicMock(
                total_tokens=200000,
                content_tokens=160000,
                response_tokens=40000,
                file_tokens=64000,
                history_tokens=64000,
            )

            with patch("utils.conversation_memory.build_conversation_history") as mock_build:
                mock_build.return_value = ("=== CONVERSATION HISTORY ===\n", 1000)

                # Call the actual function
                enhanced_args = await reconstruct_thread_context(arguments)

                # Should keep the explicit model
                assert enhanced_args.get("model") == "o3"

    @pytest.mark.asyncio
    async def test_thread_chain_model_preservation(self):
        """Test model preservation across thread chains (parent-child relationships)."""
        # Create parent thread
        parent_id = create_thread("consensus", {"prompt": "analyze"})
        add_turn(parent_id, "assistant", "Analysis", model_name="gemini-2.5-pro", model_provider="google")

        # Create child thread using a simple tool instead of workflow tool
        child_id = create_thread("consensus", {"prompt": "review"}, parent_thread_id=parent_id)

        # Child thread should be able to access parent's model through chain traversal
        # NOTE: Current implementation only checks current thread (not parent threads)
        context = get_thread(child_id)
        assert context.parent_thread_id == parent_id

        arguments = {"continuation_id": child_id}

        # Mock dependencies
        with patch("utils.model_context.ModelContext.calculate_token_allocation") as mock_calc:
            mock_calc.return_value = MagicMock(
                total_tokens=200000,
                content_tokens=160000,
                response_tokens=40000,
                file_tokens=64000,
                history_tokens=64000,
            )

            with patch("utils.conversation_memory.build_conversation_history") as mock_build:
                mock_build.return_value = ("=== CONVERSATION HISTORY ===\n", 1000)

                # Call the actual function
                enhanced_args = await reconstruct_thread_context(arguments)

                # No turns in child thread yet, so model should not be set
                assert enhanced_args.get("model") is None
