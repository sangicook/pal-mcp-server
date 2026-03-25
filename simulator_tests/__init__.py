"""
Communication Simulator Tests Package

This package contains individual test modules for the PAL MCP Communication Simulator.
Each test is in its own file for better organization and maintainability.
"""

from .base_test import BaseSimulatorTest
from .test_consensus_conversation import TestConsensusConversation
from .test_consensus_three_models import TestConsensusThreeModels
from .test_consensus_workflow_accurate import TestConsensusWorkflowAccurate
from .test_conversation_chain_validation import ConversationChainValidationTest

# Test registry for dynamic loading
TEST_REGISTRY = {
    "consensus_conversation": TestConsensusConversation,
    "consensus_workflow_accurate": TestConsensusWorkflowAccurate,
    "consensus_three_models": TestConsensusThreeModels,
    "conversation_chain_validation": ConversationChainValidationTest,
}

__all__ = [
    "BaseSimulatorTest",
    "TestConsensusConversation",
    "TestConsensusWorkflowAccurate",
    "TestConsensusThreeModels",
    "ConversationChainValidationTest",
    "TEST_REGISTRY",
]
