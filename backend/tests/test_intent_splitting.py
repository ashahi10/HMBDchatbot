import pytest
import json
from unittest.mock import MagicMock, AsyncMock

# Import the wrapper instead of direct imports
try:
    # First attempt to import directly from our test wrapper
    from tests.test_wrapper import LangChainPipeline, Intent, MultiLLMService
except ImportError:
    # If that fails, try a relative import
    from .test_wrapper import LangChainPipeline, Intent, MultiLLMService

# Test the intent splitting functionality
class TestIntentSplitting:
    
    @pytest.mark.asyncio
    async def test_split_intents_single(self):
        """Test that a single intent question is correctly identified"""
        # Mock the LLM service
        llm_service = MagicMock(spec=MultiLLMService)
        llm_service.provider = "groq"
        
        # Create a mock LLM chain response
        intent_response = json.dumps({
            "intents": [
                {"intent_text": "What is glucose?", "intent_type": "GetBasicInfo", "confidence": 0.95}
            ]
        })
        
        # Create a mock ainvoke method
        async_mock = AsyncMock(return_value=intent_response)
        llm_service.get_langchain_llm.return_value = MagicMock()
        
        # Create pipeline instance
        pipeline = LangChainPipeline(
            llm_service=llm_service,
            neo4j_connection=MagicMock(),
            neo4j_schema_text="",
            hmdb_client=MagicMock()
        )
        
        # Replace the intent_splitting_chain with a mock
        pipeline.intent_splitting_chain = MagicMock()
        pipeline.intent_splitting_chain.ainvoke = async_mock
        
        # Test single intent query
        question = "What is glucose?"
        intents = await pipeline._split_intents(question)
        
        # Assertions
        assert len(intents) == 1
        assert intents[0].intent_text == "What is glucose?"
        assert intents[0].intent_type == "GetBasicInfo"
        assert intents[0].confidence == 0.95
    
    @pytest.mark.asyncio
    async def test_split_intents_multiple(self):
        """Test that a multi-intent question is correctly identified"""
        # Mock the LLM service
        llm_service = MagicMock(spec=MultiLLMService)
        llm_service.provider = "groq"
        
        # Create a mock LLM chain response for multiple intents
        intent_response = json.dumps({
            "intents": [
                {"intent_text": "What is glucose", "intent_type": "GetBasicInfo", "confidence": 0.95},
                {"intent_text": "what pathways is it involved in", "intent_type": "GetPathways", "confidence": 0.95}
            ]
        })
        
        # Create a mock ainvoke method
        async_mock = AsyncMock(return_value=intent_response)
        llm_service.get_langchain_llm.return_value = MagicMock()
        
        # Create pipeline instance
        pipeline = LangChainPipeline(
            llm_service=llm_service,
            neo4j_connection=MagicMock(),
            neo4j_schema_text="",
            hmdb_client=MagicMock()
        )
        
        # Replace the intent_splitting_chain with a mock
        pipeline.intent_splitting_chain = MagicMock()
        pipeline.intent_splitting_chain.ainvoke = async_mock
        
        # Test multi-intent query
        question = "What is glucose and what pathways is it involved in?"
        intents = await pipeline._split_intents(question)
        
        # Assertions
        assert len(intents) == 2
        assert intents[0].intent_text == "What is glucose"
        assert intents[0].intent_type == "GetBasicInfo"
        assert intents[1].intent_text == "what pathways is it involved in"
        assert intents[1].intent_type == "GetPathways"
    
    @pytest.mark.asyncio
    async def test_intent_error_handling(self):
        """Test that errors in intent splitting are handled gracefully"""
        # Mock the LLM service
        llm_service = MagicMock(spec=MultiLLMService)
        llm_service.provider = "groq"
        
        # Create a mock LLM chain response that will cause an error
        async_mock = AsyncMock(side_effect=Exception("Test error"))
        llm_service.get_langchain_llm.return_value = MagicMock()
        
        # Create pipeline instance
        pipeline = LangChainPipeline(
            llm_service=llm_service,
            neo4j_connection=MagicMock(),
            neo4j_schema_text="",
            hmdb_client=MagicMock()
        )
        
        # Replace the intent_splitting_chain with a mock
        pipeline.intent_splitting_chain = MagicMock()
        pipeline.intent_splitting_chain.ainvoke = async_mock
        
        # Test error handling
        question = "What is glucose?"
        intents = await pipeline._split_intents(question)
        
        # Assertions - should fall back to single intent
        assert len(intents) == 1
        assert intents[0].intent_text == question
        assert intents[0].intent_type == "GetBasicInfo"
        assert intents[0].confidence == 1.0 