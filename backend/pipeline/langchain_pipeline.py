import json
from typing import List, AsyncGenerator, Optional, Any, Dict

from langchain_core.runnables import RunnableSequence, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser, PydanticOutputParser
from pydantic import BaseModel, Field

from services.llm_service import MultiLLMService
from pipeline.prompts import entity_prompt, query_plan_prompt, query_prompt, summary_prompt

BAD_RESPONSES = ["```", "json", "```json", "```cypher", "```cypher\n", "```", "cy", "pher"]

class Entity(BaseModel):
    name: str = Field(..., description="The name of the entity")
    type: str = Field(..., description="The entity category")
    confidence: float = Field(..., description="Confidence score (0-1)")

class EntityList(BaseModel):
    entities: List[Entity] = Field(..., description="List of extracted entities")

class QueryPlan(BaseModel):
    entities: List[Entity] = Field(..., description="List of extracted entities")
    query_intent: str = Field(..., description="Intent of the query")
    should_query: bool = Field(..., description="Whether a database query is needed")
    reasoning: str = Field(..., description="Explanation of the decision")

class LangChainPipeline:
    def __init__(self, llm_service: MultiLLMService, neo4j_connection, neo4j_schema_text: str):
        self.llm_service = llm_service
        self.neo4j_connection = neo4j_connection
        self.neo4j_schema_text = neo4j_schema_text

        self.entity_parser = PydanticOutputParser(pydantic_object=EntityList)
        self.query_plan_parser = PydanticOutputParser(pydantic_object=QueryPlan)


        self.entity_chain = self._create_chain( {"question": lambda x: x["question"], "schema": lambda _: self.neo4j_schema_text}, entity_prompt, streaming=True, parser=self.entity_parser)
        self.query_plan_chain = self._create_chain( {"question": lambda x: x["question"], "entities": lambda x: x["entities"], "schema": lambda _: self.neo4j_schema_text}, query_plan_prompt, streaming=True, parser=self.query_plan_parser)
        self.query_chain = self._create_chain( {"query_plan": lambda x: x["query_plan"], "schema": lambda _: self.neo4j_schema_text}, query_prompt, streaming=True, parser=None)
        self.summary_chain = self._create_chain( {"query_results": lambda x: x["query_results"], "question": lambda x: x["question"]}, summary_prompt, streaming=True, parser=None)

    def _create_chain(self, assignment_funcs: dict, chain_prompt, streaming: bool, parser: Optional[PydanticOutputParser] = None) -> RunnableSequence:
        return ( RunnablePassthrough.assign(**assignment_funcs) | chain_prompt | self.llm_service.get_langchain_llm(streaming=streaming) | StrOutputParser() )

    def _format_message(self, section: str, text: str) -> str:
        message = {"section": section, "text": text}
        return f"data:{json.dumps(message)}\n\n"

    async def _process_stream(self, stream, section: str, inputs: Dict[str, Any]) -> AsyncGenerator[str, None]:
        async for chunk in stream.astream(inputs):
            if chunk:
                chunk_text = chunk if isinstance(chunk, str) else str(chunk)
                yield self._format_message(section, chunk_text)
        yield self._format_message(section, "DONE")

    async def _stream_and_accumulate(self, chain, section: str, inputs: Dict[str, Any], accumulator: List[str]) -> AsyncGenerator[str, None]:
        async for sse_message in self._process_stream(chain, section, inputs):
            try:
                message_json = sse_message[len("data:"):].strip()
                message = json.loads(message_json)
            except json.JSONDecodeError:
                continue
            if message.get("text") not in BAD_RESPONSES + ["DONE"]:
                accumulator.append(message.get("text", ""))
            yield sse_message

    async def _match_entities(self, entity_name: str, entity_type: str) -> List[dict]:
        attribute_mapping = {
            "Metabolite": ".name",
            "Pathway": ".pathway_name",
            "Disease": ".diseaseName",
            "Synonym": ".synonymText"
        }
        index_mapping = {
            "Metabolite": "metabolite_index",
            "Pathway": "pathway_index",
            "Disease": "disease_index",
            "Synonym": "synonym_index"
        }
    

        if entity_type not in index_mapping:
            raise ValueError(f"Unsupported entity type: {entity_type}")


        query = (
                f'CALL db.index.fulltext.queryNodes("{index_mapping[entity_type]}", "{entity_name}") '
                f'YIELD node, score '
                f'WITH node, score, apoc.text.levenshteinSimilarity("{entity_name}", node{attribute_mapping[entity_type]}) AS similarity '
                f'WHERE similarity > 0 '
                f'RETURN node, node{attribute_mapping[entity_type]}, score, similarity '
                f'ORDER BY similarity DESC, score DESC '
                f'LIMIT 1;'
        )
        result = self.neo4j_connection.run_query(query)

        string_result = str(result)

        return string_result

    async def run_pipeline(self, user_question: str) -> AsyncGenerator[str, None]:
        try:
            extraction_inputs = {"question": user_question, "schema": self.neo4j_schema_text}
            extraction_accumulator: List[str] = []
            async for sse_message in self._stream_and_accumulate(self.entity_chain, "Extracting entities", extraction_inputs, extraction_accumulator):
                yield sse_message
            full_extraction_response = "".join(extraction_accumulator)
            entities = self.entity_parser.parse(full_extraction_response)

            for entity in entities.entities:
                if entity.type == "Metabolite":
                    # entity = await self._match_entities(entity.name, entity.type)
                    # print(f"Matched metabolite: {entity}"
                    entity = entity
                elif entity.type == "Pathway":
                    # entity = await self._match_entities(entity.name, entity.type)
                    # print(f"Matched pathway: {entity}")
                    entity = entity
                elif entity.type == "Disease":
                    entity = await self._match_entities(entity.name, entity.type)
                    print(f"Matched disease: {entity}")


            planning_inputs = { "question": user_question, "entities": full_extraction_response, "schema": self.neo4j_schema_text}
            planning_accumulator: List[str] = []
            async for sse_message in self._stream_and_accumulate(self.query_plan_chain, "Query planning", planning_inputs, planning_accumulator):
                yield sse_message
            full_query_plan_response = "".join(planning_accumulator)
            query_plan = self.query_plan_parser.parse(full_query_plan_response)


            query_inputs = {"query_plan": query_plan, "schema": self.neo4j_schema_text}
            query_accumulator: List[str] = []
            if query_plan.should_query:
                async for sse_message in self._stream_and_accumulate(self.query_chain, "Query execution", query_inputs, query_accumulator):
                        yield sse_message
                full_query_response = "".join(query_accumulator)
                neo4j_results = self.neo4j_connection.run_query(full_query_response)
                # neo4j_results = neo4j_results + self.neo4j_connection.run_query(f"MATCH (m:Metabolite) WHERE m.name = '' RETURN m.description")

                yield self._format_message("Results", f"Query results: {neo4j_results}")

                summary_inputs = {"query_results": neo4j_results, "question": user_question}
                summary_accumulator: List[str] = []
                async for sse_message in self._stream_and_accumulate(self.summary_chain, "Summary", summary_inputs, summary_accumulator):
                    yield sse_message
                full_summary_response = "".join(summary_accumulator)
                print(full_summary_response)

            else:
                yield self._format_message("Response", f"No database query needed. {query_plan.reasoning}")

        except Exception as e:
            yield self._format_message("Error", f"Error in pipeline: {e}")
