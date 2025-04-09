"""
Mock classes for testing without importing from the main codebase
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Any
import json

# Create mock Intent class
class Intent(BaseModel):
    intent_text: str = Field(..., description="The specific text portion of the original question for this intent")
    intent_type: str = Field(..., description="The intent category")
    confidence: float = Field(..., description="Confidence score (0-1)")

# Create mock LangChainPipeline class
class LangChainPipeline:
    def __init__(self, llm_service, neo4j_connection, neo4j_schema_text, hmdb_client=None):
        self.llm_service = llm_service
        self.neo4j_connection = neo4j_connection
        self.neo4j_schema_text = neo4j_schema_text
        self.hmdb_client = hmdb_client
        self.intent_splitting_chain = None

    async def _split_intents(self, question: str) -> List[Intent]:
        """
        Implementation of _split_intents that mimics the real behavior 
        but is simpler for testing purposes
        """
        try:
            # Call the mocked intent_splitting_chain
            if self.intent_splitting_chain:
                result = await self.intent_splitting_chain.ainvoke({"question": question})
                
                # Clean the result if it contains markdown code blocks
                BAD_RESPONSES = ["```", "json", "```json"]
                cleaned_result = result
                for bad_prefix in BAD_RESPONSES:
                    if cleaned_result.startswith(bad_prefix):
                        cleaned_result = cleaned_result[len(bad_prefix):].strip()
                for bad_suffix in ["`", "```"]:
                    if cleaned_result.endswith(bad_suffix):
                        cleaned_result = cleaned_result[:-len(bad_suffix)].strip()
                
                # Parse the JSON result
                intents_data = json.loads(cleaned_result)
                
                # Validate the structure
                if "intents" not in intents_data or not isinstance(intents_data["intents"], list):
                    return [Intent(intent_text=question, intent_type="GetBasicInfo", confidence=1.0)]
                
                # Convert dict to Intent objects
                intents = []
                for intent_dict in intents_data["intents"]:
                    intent = Intent(
                        intent_text=intent_dict.get("intent_text", ""),
                        intent_type=intent_dict.get("intent_type", "GetBasicInfo"),
                        confidence=intent_dict.get("confidence", 1.0)
                    )
                    intents.append(intent)
                
                return intents
        except Exception as e:
            # Fall back to treating as a single intent
            return [Intent(intent_text=question, intent_type="GetBasicInfo", confidence=1.0)]
        
        # Default return if no chain is set up
        return [Intent(intent_text=question, intent_type="GetBasicInfo", confidence=1.0)]

# Create mock MultiLLMService class
class MultiLLMService:
    def __init__(self, **kwargs):
        self.provider = kwargs.get("provider", "mock")
        self.default_query_model = None
        self.default_summary_model = None

    def get_langchain_llm(self, streaming=False):
        """Mock method"""
        return None 