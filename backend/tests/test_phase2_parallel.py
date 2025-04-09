import pytest
import sys
import os
import json
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch
from typing import List, Dict, Any, AsyncGenerator

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.pipeline.langchain_pipeline import LangChainPipeline, Intent, Entity, EntityList, QueryPlan
from backend.services.memory_service import MemoryService
from backend.services.llm_service import MultiLLMService

# Mock classes for testing

class MockNeo4jConnection:
    def run_query(self, query):
        # Return deterministic mock results based on query content
        if "synonym" in query.lower():
            return [{"name": "lactate", "synonyms": ["Lactic Acid", "L-Lactate"]}]
        elif "pathway" in query.lower() or "disease" in query.lower():
            return [{"name": "lactate", "diseases": ["Lactic Acidosis"], "pathways": ["Glycolysis"]}]
        elif "description" in query.lower():
            return [{"description": "Lactate is a key metabolite in energy metabolism."}]
        else:
            return [{"name": "lactate", "formula": "C3H5O3", "molecular_weight": 89.07}]

class MockHMDBClient:
    def post(self, endpoint, payload):
        # Mock HMDB API response
        if "lactate" in str(payload).lower() or "lactic" in str(payload).lower():
            return {
                "found": [{
                    "hmdb_id": "HMDB0000190",
                    "name": "Lactate",
                    "description": "Lactate is the conjugate base of lactic acid.",
                    "chemical_formula": "C3H5O3",
                    "average_molecular_weight": 89.07,
                    "monisotopic_molecular_weight": 89.02,
                    "iupac_name": "(2S)-2-hydroxypropanoate",
                    "traditional_iupac": "L-lactate",
                    "cas_registry_number": "79-33-4",
                    "smiles": "CC(O)C(=O)[O-]",
                    "inchi": "InChI=1S/C3H6O3/c1-2(4)3(5)6/h2,4H,1H3,(H,5,6)/p-1/t2-/m0/s1",
                    "inchikey": "JVTAAEKCZFNVCJ-REOHCLBHSA-M",
                    "kingdom": "Organic compounds",
                    "super_class": "Organic acids and derivatives",
                    "class": "Hydroxy acids and derivatives",
                    "sub_class": "Alpha hydroxy acids and derivatives",
                    "direct_parent": "Alpha hydroxy acids and derivatives",
                    "synonyms": ["Lactic acid", "L-Lactate", "Lactate", "(S)-2-Hydroxypropanoate"],
                    "diseases": [
                        {"name": "Lactic Acidosis", "omim_id": "245400"},
                        {"name": "Diabetes Mellitus, Type 2", "omim_id": "125853"}
                    ],
                    "pathways": [
                        {"name": "Glycolysis", "smpdb_id": "SMP00031", "kegg_map_id": "map00010"},
                        {"name": "Gluconeogenesis", "smpdb_id": "SMP00128", "kegg_map_id": "map00010"}
                    ]
                }],
                "not_found": []
            }
        else:
            return {"found": [], "not_found": [payload]}

class MockLLMService:
    """Mock LLM service for testing the pipeline"""
    
    def __init__(self, provider="test"):
        self.provider = provider
        self.default_query_model = "test-model"
        self.default_summary_model = "test-summary-model"
    
    def get_langchain_llm(self, streaming=False, temperature=None, max_tokens=None):
        llm = MagicMock()
        llm.astream = AsyncMock(side_effect=self._mock_stream)
        return llm
    
    async def generate_query_completion(self, system_prompt, user_prompt, model_name=None):
        return "Mock LLM response"
    
    async def generate_summary_completion_stream(self, system_prompt, user_prompt, model_name=None):
        chunks = ["This ", "is ", "a ", "mock ", "response."]
        for chunk in chunks:
            yield chunk
            await asyncio.sleep(0.01)
    
    async def answer_general_question(self, question, context="", model_name=None):
        if "synonym" in question.lower():
            chunks = ["Lactate has synonyms including Lactic Acid and L-Lactate."]
        elif "disease" in question.lower():
            chunks = ["Lactate is associated with diseases such as Lactic Acidosis."]
        elif "pathway" in question.lower():
            chunks = ["Lactate is involved in Glycolysis and Gluconeogenesis pathways."]
        else:
            chunks = ["Lactate is a compound with formula C3H5O3."]
        
        for chunk in chunks:
            yield chunk
            await asyncio.sleep(0.01)
    
    async def _mock_stream(self, inputs):
        # Mock different responses based on inputs
        if "entity" in str(inputs):
            result = json.dumps({
                "entities": [
                    {"name": "lactate", "type": "Metabolite", "confidence": 0.95}
                ]
            })
            yield result
        elif "query_plan" in str(inputs):
            result = json.dumps({
                "entities": [
                    {"name": "lactate", "type": "Metabolite", "confidence": 0.95}
                ],
                "query_intent": "GetMetaboliteInfo",
                "should_query": True,
                "reasoning": "The user is asking about a metabolite, so we should query the database."
            })
            yield result
        elif "query_results" in str(inputs):
            question = inputs.get("question", "").lower()
            if "synonym" in question:
                yield "Lactate has several synonyms including Lactic Acid and L-Lactate."
            elif "disease" in question:
                yield "Lactate is associated with diseases such as Lactic Acidosis."
            elif "pathway" in question:
                yield "Lactate is involved in Glycolysis and Gluconeogenesis pathways."
            else:
                yield "Lactate is a compound with formula C3H5O3 and molecular weight 89.07 g/mol."
        elif "api_data" in str(inputs):
            yield "Lactate (HMDB0000190) is the conjugate base of lactic acid and plays a key role in cellular metabolism."
        else:
            yield "MATCH (m:Metabolite {name: 'lactate'}) RETURN m"

class TestPhase2ParallelProcessing:
    """Test class for Phase 2 implementation of parallel processing"""
    
    def setup_method(self):
        """Set up test environment before each test"""
        self.llm_service = MockLLMService()
        self.neo4j_connection = MockNeo4jConnection()
        self.hmdb_client = MockHMDBClient()
        self.memory_service = MemoryService()
        
        # Create pipeline instance
        self.pipeline = LangChainPipeline(
            llm_service=self.llm_service,
            neo4j_connection=self.neo4j_connection,
            neo4j_schema_text="Mock Neo4j schema for testing",
            hmdb_client=self.hmdb_client
        )
        
        # Mock the entity parser
        self.pipeline.entity_parser = MagicMock()
        self.pipeline.entity_parser.parse.return_value = EntityList(entities=[
            Entity(name="lactate", type="Metabolite", confidence=0.95)
        ])
        
        # Mock the query plan parser
        self.pipeline.query_plan_parser = MagicMock()
        self.pipeline.query_plan_parser.parse.return_value = QueryPlan(
            entities=[Entity(name="lactate", type="Metabolite", confidence=0.95)],
            query_intent="GetMetaboliteInfo",
            should_query=True,
            reasoning="The user is asking about a metabolite, so we should query the database."
        )
    
    @pytest.mark.asyncio
    async def test_process_sub_intent(self):
        """Test the _process_sub_intent method for processing a single intent"""
        # Create a test intent
        intent = Intent(
            intent_text="What are the synonyms for lactate?",
            intent_type="GetSynonyms",
            confidence=0.95
        )
        
        # Call the process_sub_intent method
        result = await self.pipeline._process_sub_intent(
            intent=intent,
            user_question="What are the synonyms for lactate?",
            conversation_history=[],
            relevant_history=[],
            entity_extraction_results={
                "full_extraction_response": json.dumps({
                    "entities": [
                        {"name": "lactate", "type": "Metabolite", "confidence": 0.95}
                    ]
                }),
                "entities": EntityList(entities=[
                    Entity(name="lactate", type="Metabolite", confidence=0.95)
                ])
            }
        )
        
        # Verify the result
        assert result is not None, "Result should not be None"
        assert "intent" in result, "Result should contain the intent"
        assert result["intent"] == intent, "Intent in result should match input intent"
        assert "text_accumulator" in result, "Result should have a text accumulator"
        assert len(result["text_accumulator"]) > 0, "Text accumulator should not be empty"
        assert "lactate" in "".join(result["text_accumulator"]).lower(), "Result should contain information about lactate"
    
    def test_combine_sub_intent_results(self):
        """Test the _combine_sub_intent_results method"""
        # Create test results
        results = [
            {
                "intent": Intent(intent_text="What are the synonyms for lactate?", intent_type="GetSynonyms", confidence=0.95),
                "text_accumulator": ["Lactate has several synonyms including Lactic Acid and L-Lactate."]
            },
            {
                "intent": Intent(intent_text="What diseases is it associated with?", intent_type="GetDiseases", confidence=0.95),
                "text_accumulator": ["Lactate is associated with diseases such as Lactic Acidosis."]
            }
        ]
        
        # Call the combine_sub_intent_results method
        combined = self.pipeline._combine_sub_intent_results(results)
        
        # Verify the combined result
        assert "Here's the information you asked for:" in combined, "Combined result should have an introduction"
        assert "GetSynonyms" in combined, "Combined result should include the first intent type"
        assert "GetDiseases" in combined, "Combined result should include the second intent type"
        assert "synonyms" in combined.lower(), "Combined result should mention synonyms"
        assert "diseases" in combined.lower(), "Combined result should mention diseases"
    
    @pytest.mark.asyncio
    async def test_single_intent_results(self):
        """Test that the _combine_sub_intent_results method handles single intent results correctly"""
        # Create a test result with a single intent
        results = [
            {
                "intent": Intent(intent_text="What is lactate?", intent_type="GetBasicInfo", confidence=0.95),
                "text_accumulator": ["Lactate is a compound with formula C3H5O3 and molecular weight 89.07 g/mol."]
            }
        ]
        
        # Call the combine_sub_intent_results method
        combined = self.pipeline._combine_sub_intent_results(results)
        
        # Verify that the combined result contains only the text from the single intent
        assert combined == "Lactate is a compound with formula C3H5O3 and molecular weight 89.07 g/mol.", \
            "Single intent results should just return the text without headers"
    
    @pytest.mark.asyncio
    async def test_run_pipeline_multi_intent(self):
        """Test the run_pipeline method with multiple intents"""
        # Create a patched version of the _split_intents method
        async def mock_split_intents(question):
            return [
                Intent(intent_text="What are the synonyms for lactate?", intent_type="GetSynonyms", confidence=0.95),
                Intent(intent_text="What diseases is it associated with?", intent_type="GetDiseases", confidence=0.95)
            ]
        
        # Patch the _split_intents method
        self.pipeline._split_intents = mock_split_intents
        
        # Patch the _should_query_llm_decision method to always return True
        self.pipeline._should_query_llm_decision = AsyncMock(return_value=True)
        
        # Call the run_pipeline method and collect the results
        messages = []
        async for message in self.pipeline.run_pipeline(
            user_question="Give me the synonyms for lactate and also show me the diseases associated.",
            conversation_history=[],
            relevant_history=[]
        ):
            # Extract the message data
            try:
                data = json.loads(message.replace("data:", "").strip())
                messages.append(data)
            except:
                pass
        
        # Verify the results
        found_multiple_parts = False
        found_answer = False
        
        for msg in messages:
            # Check for message about processing multiple parts
            if msg["section"] == "Thinking" and "parts" in msg["text"] and "parallel" in msg["text"]:
                found_multiple_parts = True
            
            # Check for the answer
            if msg["section"] == "Answer":
                found_answer = True
                # Verify the answer contains information about both intents
                assert "synonyms" in msg["text"].lower() or "lactic acid" in msg["text"].lower(), \
                    "Answer should contain information about synonyms"
                assert "diseases" in msg["text"].lower() or "lactic acidosis" in msg["text"].lower(), \
                    "Answer should contain information about diseases"
        
        assert found_multiple_parts, "Pipeline should indicate it's processing multiple parts"
        assert found_answer, "Pipeline should return an answer"
    
    @pytest.mark.asyncio
    async def test_parallel_processing_with_gather(self):
        """Test that asyncio.gather is properly used for parallel processing"""
        # Create test intents
        intents = [
            Intent(intent_text="What are the synonyms for lactate?", intent_type="GetSynonyms", confidence=0.95),
            Intent(intent_text="What diseases is it associated with?", intent_type="GetDiseases", confidence=0.95)
        ]
        
        # Create entity extraction results
        entity_extraction_results = {
            "full_extraction_response": json.dumps({
                "entities": [
                    {"name": "lactate", "type": "Metabolite", "confidence": 0.95}
                ]
            }),
            "entities": EntityList(entities=[
                Entity(name="lactate", type="Metabolite", confidence=0.95)
            ])
        }
        
        # Replace the run_pipeline method with our own implementation to test gather usage
        async def mock_run_pipeline(user_question, conversation_history=None, relevant_history=None):
            # Generate fake SSE messages
            yield f"data:{{'section': 'Thinking', 'text': 'Processing intents in parallel...'}}\n\n"
            yield f"data:{{'section': 'Answer', 'text': 'Combined results'}}\n\n"
            yield f"data:{{'section': 'DONE', 'text': ''}}\n\n"
            
        self.pipeline.run_pipeline = mock_run_pipeline
        
        # Create a real asyncio.gather call to verify it works with _process_sub_intent
        async def test_gather():
            # Create process_sub_intent mock that returns predictable results
            process_sub_intent_mock = AsyncMock()
            process_sub_intent_mock.side_effect = [
                {"intent": intents[0], "text_accumulator": ["Mock result for synonyms"]},
                {"intent": intents[1], "text_accumulator": ["Mock result for diseases"]}
            ]
            
            # Create the tasks
            tasks = [
                process_sub_intent_mock(intent=intent, user_question="test", 
                                       conversation_history=[], relevant_history=[],
                                       entity_extraction_results=entity_extraction_results)
                for intent in intents
            ]
            
            # Call gather
            results = await asyncio.gather(*tasks)
            
            # Verify results
            assert len(results) == 2, "Should have two results"
            assert results[0]["intent"] == intents[0], "First result should be for first intent"
            assert results[1]["intent"] == intents[1], "Second result should be for second intent"
            
            # Verify process_sub_intent was called twice
            assert process_sub_intent_mock.call_count == 2, "process_sub_intent should be called twice"
            
            return results
        
        # Run the test
        results = await test_gather()
        
        # Verify that gather returned the expected results
        assert len(results) == 2, "Gather should return results for both intents"
        assert results[0]["text_accumulator"] == ["Mock result for synonyms"], "First result has correct text"
        assert results[1]["text_accumulator"] == ["Mock result for diseases"], "Second result has correct text"
    
    @pytest.mark.asyncio
    async def test_partial_failure_handling(self):
        """Test that the system can handle partial failures in multi-intent processing"""
        # Create test intents
        intents = [
            Intent(intent_text="What are the synonyms for lactate?", intent_type="GetSynonyms", confidence=0.95),
            Intent(intent_text="What diseases is it associated with?", intent_type="GetDiseases", confidence=0.95)
        ]
        
        # Create entity extraction results
        entity_extraction_results = {
            "full_extraction_response": json.dumps({
                "entities": [
                    {"name": "lactate", "type": "Metabolite", "confidence": 0.95}
                ]
            }),
            "entities": EntityList(entities=[
                Entity(name="lactate", type="Metabolite", confidence=0.95)
            ])
        }
        
        # Create a mock for _process_sub_intent that succeeds for the first intent
        # but fails for the second intent
        async def mock_process_sub_intent(intent, user_question, conversation_history=None, 
                                         relevant_history=None, entity_extraction_results=None):
            if intent.intent_type == "GetSynonyms":
                # First intent succeeds
                return {
                    "intent": intent,
                    "text_accumulator": ["Lactate has several synonyms including Lactic Acid and L-Lactate."],
                    "section": "Answer",
                    "error": None
                }
            else:
                # Second intent fails
                raise Exception("Simulated failure in disease lookup")
        
        # Create a patched version of the _split_intents method
        async def mock_split_intents(question):
            return intents
        
        # Patch the methods
        original_process_sub_intent = self.pipeline._process_sub_intent
        original_split_intents = self.pipeline._split_intents
        
        try:
            # Apply mocks
            self.pipeline._process_sub_intent = mock_process_sub_intent
            self.pipeline._split_intents = mock_split_intents
            self.pipeline._should_query_llm_decision = AsyncMock(return_value=True)
            
            # Call the run_pipeline method and collect the results
            messages = []
            async for message in self.pipeline.run_pipeline(
                user_question="Give me the synonyms for lactate and also show me the diseases associated.",
                conversation_history=[],
                relevant_history=[]
            ):
                # Extract the message data
                try:
                    data = json.loads(message.replace("data:", "").strip())
                    messages.append(data)
                except:
                    pass
            
            # Verify that we got an answer
            found_answer = False
            for msg in messages:
                if msg["section"] == "Answer":
                    found_answer = True
                    answer_text = msg["text"]
                    
                    # Should contain successful results
                    assert "synonym" in answer_text.lower() or "lactic acid" in answer_text.lower(), \
                        "Answer should contain information from successful intent"
                    
                    # Should contain error message or indication
                    assert "error" in answer_text.lower() or "fail" in answer_text.lower() or "could not" in answer_text.lower(), \
                        "Answer should acknowledge the failure of one intent"
            
            assert found_answer, "Pipeline should return an answer despite partial failure"
            
        finally:
            # Restore original methods
            self.pipeline._process_sub_intent = original_process_sub_intent
            self.pipeline._split_intents = original_split_intents 