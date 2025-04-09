import pytest
import asyncio
from typing import List, Dict, Any
from pydantic import BaseModel, Field

from backend.pipeline.langchain_pipeline import LangChainPipeline, Intent
from backend.services.llm_service import MultiLLMService, LLMProvider

# Mock LLM service for testing
class MockLLMService(MultiLLMService):
    def __init__(self):
        super().__init__(
            provider=LLMProvider.GROQ,
            api_key="test_key",
            query_generator_model_name="test_model",
            query_summarizer_model="test_model"
        )
    
    def get_langchain_llm(self, streaming=False, temperature=0.7, max_tokens=2048):
        # Create a mock LLM class that will be returned by get_langchain_llm
        class MockLLM:
            async def astream(self, inputs):
                # Return a test response based on the prompt
                yield "This is an aggregated response for testing purposes."
                
        return MockLLM()

# Test IntentList model
class TestIntent(Intent):
    intent_text: str = "Test intent"
    intent_type: str = "GetBasicInfo"
    confidence: float = 0.95

@pytest.mark.asyncio
async def test_combine_sub_intent_results():
    """Test the _combine_sub_intent_results method"""
    
    # Create a mock LLMService
    llm_service = MockLLMService()
    
    # Create a test LangChainPipeline with minimal dependencies
    pipeline = LangChainPipeline(
        llm_service=llm_service,
        neo4j_connection=None,
        neo4j_schema_text="Test schema",
        hmdb_client=None
    )
    
    # Create test intents
    intent1 = TestIntent(
        intent_text="What is glucose?",
        intent_type="GetBasicInfo",
        confidence=0.95
    )
    
    intent2 = TestIntent(
        intent_text="What pathways is it involved in?",
        intent_type="GetPathways",
        confidence=0.90
    )
    
    # Create test results to combine
    test_results = [
        {
            "intent": intent1,
            "text_accumulator": ["Glucose is a simple sugar and important carbohydrate in biology."],
            "original_question": "What is glucose and what pathways is it involved in?"
        },
        {
            "intent": intent2,
            "text_accumulator": ["Glucose is involved in glycolysis and the pentose phosphate pathway."],
            "original_question": "What is glucose and what pathways is it involved in?"
        }
    ]
    
    # Call the method
    result = await pipeline._combine_sub_intent_results(test_results)
    
    # Assertions
    assert result is not None
    assert isinstance(result, str)
    assert len(result) > 0
    
    # Check for key content from both intents
    assert "Glucose" in result
    assert "simple sugar" in result
    assert "glycolysis" in result
    assert "pentose phosphate" in result
    
    # Check that it's formatted as a cohesive response
    # Either it has no section headers (single narrative) or properly formatted markdown
    assert "##" in result or (".\n\n" in result and len(result.split("\n\n")) >= 2)

@pytest.mark.asyncio
async def test_combine_single_intent_result():
    """Test the _combine_sub_intent_results method with a single result"""
    
    # Create a mock LLMService
    llm_service = MockLLMService()
    
    # Create a test LangChainPipeline with minimal dependencies
    pipeline = LangChainPipeline(
        llm_service=llm_service,
        neo4j_connection=None,
        neo4j_schema_text="Test schema",
        hmdb_client=None
    )
    
    # Create test intent
    intent1 = TestIntent(
        intent_text="What is glucose?",
        intent_type="GetBasicInfo",
        confidence=0.95
    )
    
    # Create a single test result
    test_results = [
        {
            "intent": intent1,
            "text_accumulator": ["Glucose is a simple sugar and important carbohydrate in biology."],
            "original_question": "What is glucose?"
        }
    ]
    
    # Call the method
    result = await pipeline._combine_sub_intent_results(test_results)
    
    # Assertions
    assert result is not None
    assert isinstance(result, str)
    assert len(result) > 0
    
    # With a single result, it should directly return the accumulated text
    assert result == "Glucose is a simple sugar and important carbohydrate in biology."

@pytest.mark.asyncio
async def test_empty_results():
    """Test the _combine_sub_intent_results method with empty results"""
    
    # Create a mock LLMService
    llm_service = MockLLMService()
    
    # Create a test LangChainPipeline with minimal dependencies
    pipeline = LangChainPipeline(
        llm_service=llm_service,
        neo4j_connection=None,
        neo4j_schema_text="Test schema",
        hmdb_client=None
    )
    
    # Call the method with empty results
    result = await pipeline._combine_sub_intent_results([])
    
    # Assertions
    assert result is not None
    assert isinstance(result, str)
    assert "couldn't process" in result 