import json
from typing import List, AsyncGenerator, Optional, Any, Dict

from langchain_core.runnables import RunnableSequence, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser, PydanticOutputParser
from langchain_ollama import ChatOllama
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler
from pydantic import BaseModel, Field

from pipeline.prompts import entity_prompt, query_plan_prompt, query_prompt, summary_prompt, other_prompt

BAD_RESPONSES = ["```", "json", "```json", "```cypher", "```cypher\n", "```", "cy", "pher", "``"]

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
    def __init__(self, neo4j_connection: Any, neo4j_schema_text: str, 
                 entity_model: str = "mistral-nemo:latest",
                 query_plan_model: str = "mistral-nemo:latest",
                 query_model: str = "tomasonjo/llama3-text2cypher-demo",
                 summary_model: str = "mistral-nemo:latest",
                 other_model: str = "mistral-nemo:latest"):
        self.neo4j_connection = neo4j_connection
        self.neo4j_schema_text = neo4j_schema_text

        self.entity_model = entity_model
        self.query_plan_model = query_plan_model
        self.query_model = query_model
        self.summary_model = summary_model
        self.other_model = other_model
        self.neo4j_connection = neo4j_connection
        self.neo4j_schema_text = neo4j_schema_text


        self.entity_parser = PydanticOutputParser(pydantic_object=EntityList)
        self.query_plan_parser = PydanticOutputParser(pydantic_object=QueryPlan)

        self.entity_chain = self._create_chain({"question": lambda inp: inp["question"], "schema": lambda _: self.neo4j_schema_text},
                                                entity_prompt, streaming=True, parser=None, streaming_model=False, format="json", model_name=self.entity_model)
        self.query_plan_chain = self._create_chain({"question": lambda inp: inp["question"], "entities": lambda inp: inp["entities"], "schema": lambda _: self.neo4j_schema_text}, 
                                                   query_plan_prompt, streaming=True, parser=None, streaming_model=False, format="json", model_name=self.query_plan_model)
        self.query_chain = self._create_chain({"query_plan": lambda inp: inp["query_plan"], "schema": lambda _: self.neo4j_schema_text}, 
                                              query_prompt, streaming=True, parser=None, streaming_model=False, format="", model_name=self.query_model)
        self.summary_chain = self._create_chain({"query_results": lambda inp: inp["query_results"], "question": lambda inp: inp["question"]}, 
                                                summary_prompt, streaming=True, parser=None, streaming_model=True, format="", model_name=self.summary_model)
        self.other_chain = self._create_chain({"question": lambda inp: inp["question"]}, 
                                              other_prompt, streaming=True, parser=None, streaming_model=True, format="", model_name=self.other_model)

    def _get_llm(self, model_name: str, streaming: bool = False, format: str = "json"):

        return ChatOllama(
            base_url="https://2vlm5q6h-11434.usw2.devtunnels.ms/",
            model=model_name,
            temperature=0.2,
            num_ctx=4096,
            callbacks=[StreamingStdOutCallbackHandler()] if streaming else None,
            format=format
        )

    def _create_chain(self, assignment_funcs: Dict[str, Any], chain_prompt: Any, streaming: bool, parser: Optional[PydanticOutputParser] = None, streaming_model: bool = False, format: str = "json", model_name: str = "mistral-nemo:latest") -> RunnableSequence:
        llm = self._get_llm(streaming=streaming_model, format=format, model_name=model_name)
        chain = RunnablePassthrough.assign(**assignment_funcs) | chain_prompt | llm
        chain |= parser if parser else StrOutputParser()
        return chain

    def _format_message(self, section: str, text: str) -> str:
        message = {"section": section, "text": text}
        return f"data:{json.dumps(message)}\n\n"

    def match_metabolite(self, metabolite: str):
        metabolite_query = f"""
            CALL db.index.fulltext.queryNodes("metabolite_names", "{metabolite}~0.5") YIELD node, score
            RETURN node.name AS name, score
            LIMIT 3
        """
        metabolite_results = self.neo4j_connection.run_query(metabolite_query)
        
        for result in metabolite_results:
            if result['score'] > 0.5:
                return result['name']
        
        synonyms_query_fuzzy = f"""
            CALL db.index.fulltext.queryNodes("synonymsFullText", "{metabolite}~0.3") YIELD node, score
            RETURN node.synonymText AS synonymText, score
            LIMIT 3
        """
        synonyms_results = self.neo4j_connection.run_query(synonyms_query_fuzzy)
        
        if not synonyms_results or len(synonyms_results) == 0:
            synonyms_query_wildcard = f"""
                CALL db.index.fulltext.queryNodes("synonymsFullText", "{metabolite}*") YIELD node, score
                RETURN node.synonymText AS synonymText, score
                LIMIT 3
            """
            synonyms_results = self.neo4j_connection.run_query(synonyms_query_wildcard)
        
        for synonym_result in synonyms_results:
            if synonym_result['score'] > 2:
                associated_metabolite_query = f"""
                    MATCH (m:Metabolite)-[:HAS_SYNONYM]->(s:Synonym)
                    WHERE toLower(s.synonymText) = toLower("{synonym_result['synonymText']}")
                    RETURN m.name AS name
                    LIMIT 1
                """
                metabolite_match = self.neo4j_connection.run_query(associated_metabolite_query)
                if metabolite_match and len(metabolite_match) > 0:
                    return metabolite_match[0]['name']
        
        return None

    async def _process_stream(self, chain: Any, section: str, inputs: Dict[str, Any], accumulator: List[str]) -> AsyncGenerator[str, None]:
        buffer = ""
        async for chunk in chain.astream(inputs):
            if not chunk:
                continue
            chunk_text = str(chunk)
            buffer += chunk_text
            while "<think>" in buffer and "</think>" in buffer:
                pre, _, remainder = buffer.partition("<think>")
                thinking, _, post = remainder.partition("</think>")
                if thinking.strip():
                    yield self._format_message("Thinking", thinking.strip())
                buffer = pre + post
            if buffer and "<think>" not in buffer:
                yield self._format_message(section, buffer)
                if section != "Thinking" and buffer not in BAD_RESPONSES + ["DONE"]:
                    accumulator.append(buffer)
                buffer = ""
        if buffer:
            if "<think>" in buffer and "</think>" in buffer:
                thinking = buffer.split("<think>")[1].split("</think>")[0].strip()
                if thinking:
                    yield self._format_message("Thinking", thinking)
            else:
                yield self._format_message(section, buffer)
                if section != "Thinking" and buffer not in BAD_RESPONSES + ["DONE"]:
                    accumulator.append(buffer)
        yield self._format_message(section, "DONE")
    
    async def run_pipeline(self, user_question: str) -> AsyncGenerator[str, None]:
        try:

            extraction_inputs = {"question": user_question, "schema": self.neo4j_schema_text}
            extraction_accumulator: List[str] = []
            async for message in self._process_stream(self.entity_chain, "Extracting entities", extraction_inputs, extraction_accumulator):
                yield message
            extraction_response = "".join(extraction_accumulator)

            try:
                entities = self.entity_parser.parse(extraction_response)
            except Exception as e:
                entities = EntityList(entities=[])

            for entity in entities.entities:
                if entity.type == "Metabolite":
                    entity.name = self.match_metabolite(entity.name)
            
            planning_inputs = {"question": user_question, "entities": extraction_response, "schema": self.neo4j_schema_text}
            planning_accumulator: List[str] = []
            async for message in self._process_stream(self.query_plan_chain, "Query planning", planning_inputs, planning_accumulator):
                yield message
            query_plan = self.query_plan_parser.parse("".join(planning_accumulator))

            if query_plan.should_query:
                query_inputs = {"query_plan": query_plan, "schema": self.neo4j_schema_text}
                query_accumulator: List[str] = []
                async for message in self._process_stream(self.query_chain, "Query execution", query_inputs, query_accumulator):
                    yield message
                query_response = "".join(query_accumulator)
                neo4j_results = self.neo4j_connection.run_query(query_response)

                metabolites = [entity.name for entity in entities.entities if entity.type == "Metabolite"]
                for metabolite in metabolites:
                    neo4j_results += self.neo4j_connection.run_query(f"""
                        MATCH (m:Metabolite)
                        WHERE toLower(m.name) = toLower('{metabolite}')
                        OR EXISTS {{
                            MATCH (m)-[:HAS_SYNONYM]->(s:Synonym)
                            WHERE toLower(s.synonymText) = toLower('{metabolite}')
                        }}
                        RETURN m.description
                    """)
                yield self._format_message("Results", f"Query results: {neo4j_results}")

                summary_inputs = {"query_results": neo4j_results, "question": user_question}
                summary_accumulator: List[str] = []
                async for message in self._process_stream(self.summary_chain, "Summary", summary_inputs, summary_accumulator):
                    yield message
            else:
                async for message in self._process_stream(self.other_chain, "Summary", {"question": user_question}, []):
                    yield message
        except Exception as error:
            yield self._format_message("Error", f"Error in pipeline: {error}")
