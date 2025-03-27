from typing import List, AsyncGenerator, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.runnables import RunnableSequence
from pydantic import BaseModel, Field

from pipeline.config import PipelineConfig
from pipeline.model_manager import ModelManager
from pipeline.entity_manager import EntityManager, Entity, EntityList
from pipeline.stream_processor import StreamProcessor
from pipeline.chain_manager import ChainManager
from pipeline.query_manager import QueryManager


class PipelineStage(Enum):
    ENTITY_EXTRACTION = "entity_extraction"
    ENTITY_MATCHING = "entity_matching"
    QUERY_PLANNING = "query_planning"
    QUERY_GENERATION = "query_generation"
    QUERY_EXECUTION = "query_execution"
    RESULT_PROCESSING = "result_processing"
    SUFFICIENCY_EVALUATION = "sufficiency_evaluation"
    SUMMARY_GENERATION = "summary_generation"

@dataclass
class PipelineState:
    user_question: str
    entities: Optional[EntityList] = None
    query_plan: Optional[Any] = None
    query_response: Optional[str] = None
    neo4j_results: Optional[List[Dict[str, Any]]] = None
    current_stage: Optional[PipelineStage] = None
    error: Optional[Exception] = None

class QueryPlan(BaseModel):
    entities: List[Entity] = Field(..., description="List of extracted entities that match the schema")
    query_intent: str = Field(..., description="Intent of the query")
    should_query: bool = Field(..., description="Whether a database query is needed")
    reasoning: str = Field(..., description="Explanation of the decision")
    nodes_and_relationships: Dict[str, List[str]] = Field(
        ...,
        description="Specifies the node labels and relationship types the query should use. 'nodes' is a list of node labels, 'relationships' is a list of relationship types, 'properties' is a list of properties"
    )

class SufficiencyPlan(BaseModel):
    entities: List[Entity] = Field(..., description="List of extracted entities that match the schema")
    query_intent: str = Field(..., description="Intent of the query")
    should_retry_query: bool = Field(..., description="Whether a database query is needed")
    reasoning: str = Field(..., description="Explanation of the decision")
    nodes_and_relationships: Dict[str, List[str]] = Field(
        ...,
        description="Specifies the node labels and relationship types the query should use. 'nodes' is a list of node labels, 'relationships' is a list of relationship types, 'properties' is a list of properties"
    )


class LangChainPipeline:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.state = PipelineState(user_question="")
        
        
        self.model_manager = ModelManager(config)
        self.entity_manager = EntityManager(config)
        self.chain_manager = ChainManager(config, self.model_manager)

        self._initialize_chains()


        self.query_manager = QueryManager(config, self.retry_chain)

        self.entity_parser = PydanticOutputParser(pydantic_object=EntityList)
        self.query_plan_parser = PydanticOutputParser(pydantic_object=QueryPlan)
        self.sufficiency_plan_parser = PydanticOutputParser(pydantic_object=SufficiencyPlan)

    def _initialize_chains(self) -> None:
        chains = self.chain_manager.initialize_chains()
        self.entity_chain = chains["entity_chain"]
        self.query_plan_chain = chains["query_plan_chain"]
        self.query_chain = chains["query_chain"]
        self.summary_chain = chains["summary_chain"]
        self.other_chain = chains["other_chain"]
        self.retry_chain = chains["retry_chain"]
        self.sufficiency_chain = chains["sufficiency_chain"]

    async def _process_stage( self, stage: PipelineStage, chain: RunnableSequence, inputs: Dict[str, Any], section: str ) -> AsyncGenerator[str, None]:
        self.state.current_stage = stage
        accumulator: List[str] = []
        async for message in StreamProcessor.process_stream(chain, section, inputs, accumulator):
            yield message
        yield StreamProcessor.format_message(section, "".join(accumulator))

    async def _extract_entities(self) -> AsyncGenerator[str, None]:
        inputs = { "question": self.state.user_question, "schema": self.config.neo4j_schema_text }
        accumulator: List[str] = []
        async for message in StreamProcessor.process_stream(self.entity_chain, "Extracting entities", inputs, accumulator):
            yield message
        response = "".join(accumulator)
        
        try:
            self.state.entities = self.entity_parser.parse(response)
        except Exception as e:
            self.state.error = e
            self.state.entities = EntityList(entities=[])
            yield StreamProcessor.format_message("Error", f"Failed to parse entities: {e}")

    async def _match_entities(self) -> AsyncGenerator[str, None]:
        if not self.state.entities:
            yield StreamProcessor.format_message("Warning", "No entities to match")
            return

        for entity in self.state.entities.entities:
            if entity.type == "Metabolite":
                entity.name = self.entity_manager.match_metabolite(entity.name)
            elif entity.type == "Protein":
                entity.name = self.entity_manager.match_protein(entity.name)
            elif entity.type == "Disease":
                entity.name = self.entity_manager.match_disease(entity.name)
            yield StreamProcessor.format_message("Entity Matching", f"Matched {entity.type}: {entity.name}")

    async def _create_query_plan(self) -> AsyncGenerator[str, None]:
        inputs = { "question": self.state.user_question, "entities": self.state.entities, "schema": self.config.neo4j_schema_text }
        accumulator: List[str] = []
        async for message in StreamProcessor.process_stream(self.query_plan_chain, "Query planning", inputs, accumulator):
            yield message
        response = "".join(accumulator)
        
        try:
            self.state.query_plan = self.query_plan_parser.parse(response)
        except Exception as e:
            self.state.error = e
            yield StreamProcessor.format_message("Error", f"Failed to parse query plan: {e}")

    async def _generate_query(self) -> AsyncGenerator[str, None]:
        inputs = { "query_plan": self.state.query_plan, "schema": self.config.neo4j_schema_text}
        accumulator: List[str] = []
        async for message in StreamProcessor.process_stream(self.query_chain, "Query execution", inputs, accumulator):
            yield message
        self.state.query_response = "".join(accumulator)

    async def _execute_query(self) -> AsyncGenerator[str, None]:
        async for message in self.query_manager.execute_query( self.state.query_plan, self.state.query_response ):
            yield message

        async for message in self.query_manager.handle_empty_results( self.state.query_plan, self.state.query_response, self.query_manager.get_current_results()):
            yield message

        self.state.neo4j_results = self.query_manager.get_current_results()
        yield StreamProcessor.format_message("Results", f"Query results: {self.state.neo4j_results}")

    async def _process_results(self) -> AsyncGenerator[str, None]:
        if not self.state.neo4j_results:
            yield StreamProcessor.format_message("Warning", "No results to process")
            return

        if len(self.state.neo4j_results) > 0:
            metabolites = [
                entity.name for entity in self.state.entities.entities
                if entity.type == "Metabolite"
            ]
            descriptions = self.entity_manager.get_metabolite_descriptions(metabolites)
            current_results = self.state.neo4j_results
            self.state.neo4j_results.extend(descriptions)
            if len(self.state.neo4j_results) > len(current_results):
                yield StreamProcessor.format_message(
                    "Results", f"Processed results: {self.state.neo4j_results}"
                )

    async def _evaluate_sufficiency(self) -> AsyncGenerator[str, None]:
        retry_count = 0
        max_retries = 3
        
        while retry_count <= max_retries:
            inputs = { 
                "neo4j_results": self.state.neo4j_results, 
                "question": self.state.user_question, 
                "schema": self.config.neo4j_schema_text,
                "current_query": self.state.query_response
            }
            accumulator: List[str] = []
            async for message in StreamProcessor.process_stream(self.sufficiency_chain, "Sufficiency", inputs, accumulator):
                yield message
            
            response = "".join(accumulator)
            try:
                sufficiency_plan = self.sufficiency_plan_parser.parse(response)
                if not sufficiency_plan.should_retry_query:
                    break
                    
                retry_count += 1
                if retry_count > max_retries:
                    yield StreamProcessor.format_message("Error", f"Query results still insufficient after {max_retries} additions")
                    break
                    
                yield StreamProcessor.format_message("Query Addition", f"Attempt {retry_count} of {max_retries}: Adding additional query components...")
                
                if hasattr(sufficiency_plan, 'query_addition') and sufficiency_plan.query_addition:
                    query_parts = self.state.query_response.split("RETURN")
                    if len(query_parts) == 2:
                        self.state.query_response = f"{query_parts[0]}{sufficiency_plan.query_addition} RETURN{query_parts[1]}"
                    else:
                        self.state.query_response = f"{self.state.query_response} {sufficiency_plan.query_addition}"
                    
                    # Execute the modified query
                    async for message in self._execute_query():
                        yield message
                    async for message in self._process_results():
                        yield message
                else:
                    yield StreamProcessor.format_message("Error", "No query addition provided in sufficiency plan")
                    break
                    
            except Exception as e:
                retry_count += 1
                if retry_count > max_retries:
                    yield StreamProcessor.format_message("Error", f"Failed to evaluate sufficiency after {max_retries} attempts: {str(e)}")
                    break
                    
                yield StreamProcessor.format_message("Retry", f"Attempt {retry_count} of {max_retries}: Failed to parse sufficiency evaluation")

    async def _generate_summary(self) -> AsyncGenerator[str, None]:
        inputs = { "query_results": self.state.neo4j_results, "question": self.state.user_question}
        async for message in StreamProcessor.process_stream( self.summary_chain, "Summary", inputs, []):
            yield message

    async def _handle_non_query_response(self) -> AsyncGenerator[str, None]:
        inputs = {"question": self.state.user_question}
        async for message in StreamProcessor.process_stream( self.other_chain, "Summary", inputs, []):
            yield message
    
    async def run_pipeline(self, user_question: str) -> AsyncGenerator[str, None]:
        try:
            self.state = PipelineState(user_question=user_question)

            async for message in self._extract_entities():
                yield message
            async for message in self._match_entities():
                yield message
            
            async for message in self._create_query_plan():
                yield message
            
            if self.state.query_plan and self.state.query_plan.should_query:
                async for message in self._generate_query():
                    yield message
                async for message in self._execute_query():
                        yield message
                
                async for message in self._process_results():
                        yield message
                async for message in self._evaluate_sufficiency():
                    yield message
                async for message in self._generate_summary():
                    yield message
            else:
                async for message in self._handle_non_query_response():
                    yield message
                
        except Exception as error:
            self.state.error = error
            yield StreamProcessor.format_message("Error", f"Error in pipeline: {error}")
