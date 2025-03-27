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
    def __init__(self, llm_service: MultiLLMService, neo4j_connection, neo4j_schema_text: str, hmdb_client=None):
        self.llm_service = llm_service
        self.neo4j_connection = neo4j_connection
        self.neo4j_schema_text = neo4j_schema_text
        self.hmdb_client = hmdb_client

        self.entity_parser = PydanticOutputParser(pydantic_object=EntityList)
        self.query_plan_parser = PydanticOutputParser(pydantic_object=QueryPlan)

        self.entity_chain = self._create_chain(
            {"question": lambda x: x["question"], "schema": lambda _: self.neo4j_schema_text}, 
            entity_prompt, 
            streaming=True, 
            parser=None,
            model_type="query"
        )
        self.query_plan_chain = self._create_chain(
            {"question": lambda x: x["question"], "entities": lambda x: x["entities"], "schema": lambda _: self.neo4j_schema_text}, 
            query_plan_prompt, 
            streaming=True, 
            parser=None,
            model_type="query"
        )
        self.query_chain = self._create_chain(
            {"query_plan": lambda x: x["query_plan"], "schema": lambda _: self.neo4j_schema_text}, 
            query_prompt, 
            streaming=True, 
            parser=None,
            model_type="query"
        )
        self.summary_chain = self._create_chain(
            {"query_results": lambda x: x["query_results"], "question": lambda x: x["question"]}, 
            summary_prompt, 
            streaming=True, 
            parser=None,
            model_type="summary"
        )

    def _create_chain(self, assignment_funcs: dict, chain_prompt, streaming: bool, parser: Optional[PydanticOutputParser] = None, model_type: str = "query") -> RunnableSequence:
        if self.llm_service.provider == "ollama":
            streaming = False
            
        if model_type == "summary":
            self.llm_service.default_query_model = self.llm_service.default_summary_model
            
        chain = RunnablePassthrough.assign(**assignment_funcs) | chain_prompt | self.llm_service.get_langchain_llm(streaming=streaming)
        
        if model_type == "summary":
            self.llm_service.default_query_model = self.llm_service.default_summary_model
            
        if parser:
            chain = chain | parser
        else:
            chain = chain | StrOutputParser()
            
        return chain

    def _format_message(self, section: str, text: str) -> str:
        message = {"section": section, "text": text}
        return f"data:{json.dumps(message)}\n\n"

    def _process_text_with_thinking(self, text: str) -> tuple[str, str]:
        """
        Process text to handle thinking tags and return both the thinking and non-thinking parts.
        Returns a tuple of (thinking_text, clean_text)
        """
        thinking_text = ""
        clean_text = text
        
        # Extract thinking content
        if "<think>" in text and "</think>" in text:
            start_idx = text.find("<think>") + len("<think>")
            end_idx = text.find("</think>")
            thinking_text = text[start_idx:end_idx].strip()
            
            # Remove thinking tags and content from clean text
            clean_text = text[:text.find("<think>")] + text[text.find("</think>") + len("</think>"):]
            clean_text = clean_text.strip()
            
        return thinking_text, clean_text

    async def _process_stream(self, stream, section: str, inputs: Dict[str, Any]) -> AsyncGenerator[str, None]:
        async for chunk in stream.astream(inputs):
            if chunk:
                chunk_text = chunk if isinstance(chunk, str) else str(chunk)
                thinking_text, clean_text = self._process_text_with_thinking(chunk_text)
                
                # Yield thinking text if it exists
                if thinking_text:
                    yield self._format_message("Thinking", thinking_text)
                
                # Yield clean text if it exists
                if clean_text:
                    yield self._format_message(section, clean_text)
                    
        yield self._format_message(section, "DONE")

    async def _stream_and_accumulate(self, chain, section: str, inputs: Dict[str, Any], accumulator: List[str]) -> AsyncGenerator[str, None]:
        async for sse_message in self._process_stream(chain, section, inputs):
            try:
                message_json = sse_message[len("data:"):].strip()
                message = json.loads(message_json)
            except json.JSONDecodeError:
                continue
                
            # Only accumulate non-thinking messages that aren't in BAD_RESPONSES
            if message.get("section") != "Thinking" and message.get("text") not in BAD_RESPONSES + ["DONE"]:
                accumulator.append(message.get("text", ""))
            yield sse_message

    async def _match_entities(self, entity_name: str, entity_type: str) -> List[dict]:
        pass

    async def run_pipeline(self, user_question: str) -> AsyncGenerator[str, None]:
        try:
            # 1) Entity Extraction
            extraction_inputs = {"question": user_question, "schema": self.neo4j_schema_text}
            extraction_accumulator: List[str] = []
            async for sse_message in self._stream_and_accumulate(self.entity_chain, "Extracting entities", extraction_inputs, extraction_accumulator):
                yield sse_message

            full_extraction_response = "".join(extraction_accumulator)
            print(f"\n[DEBUG] Entity Extraction Response: {full_extraction_response}")
            
            entities = self.entity_parser.parse(full_extraction_response)
            metabolites = [ent.name for ent in entities.entities if ent.type == "Metabolite"]

            # 2) Query Planning
            planning_inputs = {"question": user_question, "entities": full_extraction_response, "schema": self.neo4j_schema_text}
            planning_accumulator: List[str] = []
            async for sse_message in self._stream_and_accumulate(self.query_plan_chain, "Query planning", planning_inputs, planning_accumulator):
                yield sse_message

            full_query_plan_response = "".join(planning_accumulator)
            print(f"\n[DEBUG] Query Planning Response: {full_query_plan_response}")

            query_plan = self.query_plan_parser.parse(full_query_plan_response)

            # 3) If query_plan says we should query:
            if query_plan.should_query:
                query_inputs = {"query_plan": query_plan, "schema": self.neo4j_schema_text}
                query_accumulator: List[str] = []
                async for sse_message in self._stream_and_accumulate(self.query_chain, "Query execution", query_inputs, query_accumulator):
                    yield sse_message
                full_query_response = "".join(query_accumulator)
                
                print(f"\n[DEBUG] Generated Cypher Query: {full_query_response}")
                # neo4j_results = self.neo4j_connection.run_query(full_query_response)
                # print(f"\n[DEBUG] Neo4j query results => {neo4j_results}")
                try:
                    neo4j_results = self.neo4j_connection.run_query(full_query_response)
                    print(f"\n[DEBUG] Neo4j raw query results: {neo4j_results}")
                    
                    # Check if the results are valid
                    if not neo4j_results or neo4j_results == [None] or neo4j_results == [""]:
                        print("[DEBUG] No valid results from Neo4j, initiating HMDB fallback...")
                        neo4j_results = []
                except Exception as e:
                    print(f"\n[ERROR] Neo4j query execution failed: {e}")
                    neo4j_results = []


                # Check for synonyms as well
                for metabolite in metabolites:
                    more_results = self.neo4j_connection.run_query(f"""
                        MATCH (m:Metabolite)
                        WHERE toLower(m.name) = toLower('{metabolite}')
                        OR EXISTS {{
                            MATCH (m)-[:HAS_SYNONYM]->(s:Synonym)
                            WHERE toLower(s.synonymText) = toLower('{metabolite}')
                        }}
                        RETURN m.description
                    """)
                    if more_results:
                        neo4j_results += more_results

                # Validate neo4j_results properly
                if not neo4j_results or neo4j_results == [None] or neo4j_results == [""]:
                    print("[DEBUG] No valid results from Neo4j, initiating HMDB fallback...")
                    if len(neo4j_results) == 0 and len(metabolites) > 0 and self.hmdb_client:
                        first_metabolite = metabolites[0]
                        fallback_data = None

                        # Check if it's an HMDB ID or not
                        # Check if it's an HMDB ID or not
                        if first_metabolite.startswith("HMDB"):
                            print(f"\n[DEBUG] Making API call for ID '{first_metabolite}'")
                            
                            # Making the API call directly without caching
                            payload = {"hmdb_id": [first_metabolite]}
                            fallback_data = self.hmdb_client.post("metabolites", payload)
                            
                            if fallback_data and "found" in fallback_data:
                                print(f"\n[DEBUG] Fallback data from API: {fallback_data}")
                            else:
                                print(f"\n[DEBUG] No data found for ID '{first_metabolite}' from API")

                        else:  # If it's not an HMDB ID, try searching by name
                            print(f"\n[DEBUG] Making API search call for name '{first_metabolite}'")
                            
                            # Making the API search call directly without caching
                            payload = {"name": first_metabolite}
                            fallback_data = self.hmdb_client.post("metabolites/search", payload)
                            
                            if fallback_data and "found" in fallback_data:
                                print(f"\n[DEBUG] Fallback data from API (search): {fallback_data}")
                            else:
                                print(f"\n[DEBUG] No data found for name '{first_metabolite}' from API")


                        # Process the fallback data if it was found
                        if fallback_data:
                            print(f"\n[DEBUG] Fallback data returned: {fallback_data}")

                            summary_inputs = {
                                "query_results": fallback_data,
                                "question": user_question
                            }
                            summary_accumulator: List[str] = []
                            async for sse_message in self._stream_and_accumulate(
                                self.summary_chain,
                                "Summary (HMDB Fallback)",
                                summary_inputs,
                                summary_accumulator
                            ):
                                yield sse_message
                        else:
                            yield self._format_message("Results", "No data found in DB or from HMDB API fallback.")

                    # #working code .
                    # if len(neo4j_results) == 0 and len(metabolites) > 0 and self.hmdb_client:
                    #     first_metabolite = metabolites[0]
                    #     fallback_data = None

                    #     if first_metabolite.startswith("HMDB"):
                    #         endpoint = f"metabolites/{first_metabolite}"
                    #         print(f"\n[DEBUG] Making API call for ID '{first_metabolite}'")
                    #         fallback_data = self.hmdb_client.get(endpoint)
                    #         print(f"\n[DEBUG] API call completed with data: {fallback_data}")
                            
                    #     else:
                    #         search_payload = {"name": first_metabolite}
                    #         print(f"\n[DEBUG] Making API search call for name '{first_metabolite}'")
                    #         search_results = self.hmdb_client.post("metabolites/search", search_payload)
                    #         print(f"\n[DEBUG] API search results for name '{first_metabolite}': {search_results}")

                    #         if search_results and "found" in search_results:
                    #             hmdb_id = search_results["found"][0].get("hmdb_id")
                    #             if hmdb_id:
                    #                 endpoint = f"metabolites/{hmdb_id}"
                    #                 fallback_data = self.hmdb_client.get(endpoint)
                    #                 print(f"\n[DEBUG] Fetched data using name '{first_metabolite}': {fallback_data}")

                    #     if fallback_data:
                    #         yield self._format_message("Results", f"HMDB fallback results: {fallback_data}")
#ahmed code
                    # if len(metabolites) > 0 and self.hmdb_client:
                    #     first_metabolite = metabolites[0]
                    #     fallback_data = None

                    #     if first_metabolite.startswith("HMDB"):
                    #         endpoint = f"metabolites/{first_metabolite}"
                    #         fallback_data = self.hmdb_client.get(endpoint)
                    #         print(f"\n[DEBUG] Fetched data for ID {first_metabolite}: {fallback_data}")
                    #     else:
                    #         search_payload = {"name": first_metabolite}
                    #         search_results = self.hmdb_client.post("metabolites/search", search_payload)
                    #         print(f"\n[DEBUG] Search Results for '{first_metabolite}': {search_results}")

                    #         if search_results and "found" in search_results:
                    #             hmdb_id = search_results["found"][0].get("hmdb_id")
                    #             if hmdb_id:
                    #                 endpoint = f"metabolites/{hmdb_id}"
                    #                 fallback_data = self.hmdb_client.get(endpoint)
                    #                 print(f"\n[DEBUG] Fetched data for name '{first_metabolite}': {fallback_data}")

                    #     if fallback_data:
                    #         yield self._format_message("Results", f"HMDB fallback results: {fallback_data}")

                            summary_inputs = {"query_results": fallback_data, "question": user_question}
                            summary_accumulator: List[str] = []
                            async for sse_message in self._stream_and_accumulate(
                                self.summary_chain,
                                "Summary (HMDB Fallback)",
                                summary_inputs,
                                summary_accumulator
                            ):
                                yield sse_message
                            else:
                                yield self._format_message("Results", "No data found in DB or from HMDB fallback.")

                else:
                    yield self._format_message("Results", f"Query results: {neo4j_results}")
                    
                    summary_inputs = {"query_results": neo4j_results, "question": user_question}
                    summary_accumulator: List[str] = []
                    async for sse_message in self._stream_and_accumulate(self.summary_chain, "Summary", summary_inputs, summary_accumulator):
                        yield sse_message
            else:
                yield self._format_message("Response", f"No database query needed. {query_plan.reasoning}")

        except Exception as e:
            yield self._format_message("Error", f"Error in pipeline: {e}")

    
    # async def run_pipeline(self, user_question: str) -> AsyncGenerator[str, None]:
    #     try:
    #         # 1) Entity Extraction
    #         extraction_inputs = {"question": user_question, "schema": self.neo4j_schema_text}
    #         extraction_accumulator: List[str] = []

    #         async for sse_message in self._stream_and_accumulate(
    #             self.entity_chain,
    #             "Extracting entities",
    #             extraction_inputs,
    #             extraction_accumulator
    #         ):
    #             yield sse_message

    #         full_extraction_response = "".join(extraction_accumulator)
    #         print(full_extraction_response)

    #         entities = self.entity_parser.parse(full_extraction_response)

    #         # Gather any extracted metabolite names
    #         metabolites = []
    #         for entity in entities.entities:
    #             if entity.type == "Metabolite":
    #                 metabolites.append(entity.name)

    #         # 2) Query Planning
    #         planning_inputs = {
    #             "question": user_question,
    #             "entities": full_extraction_response,
    #             "schema": self.neo4j_schema_text
    #         }
    #         planning_accumulator: List[str] = []

    #         async for sse_message in self._stream_and_accumulate(
    #             self.query_plan_chain,
    #             "Query planning",
    #             planning_inputs,
    #             planning_accumulator
    #         ):
    #             yield sse_message

    #         full_query_plan_response = "".join(planning_accumulator)
    #         query_plan = self.query_plan_parser.parse(full_query_plan_response)

    #         # 3) If plan indicates we should query DB, do so
    #         query_inputs = {"query_plan": query_plan, "schema": self.neo4j_schema_text}
    #         query_accumulator: List[str] = []

    #         if query_plan.should_query:
    #             # 3A) Execute the Cypher chain
    #             async for sse_message in self._stream_and_accumulate(
    #                 self.query_chain,
    #                 "Query execution",
    #                 query_inputs,
    #                 query_accumulator
    #             ):
    #                 yield sse_message

    #             full_query_response = "".join(query_accumulator)

    #             # 3B) Actually run it in Neo4j
    #             neo4j_results = self.neo4j_connection.run_query(full_query_response)

    #             # 3C) Also do the local synonyms check for each metabolite
    #             for metabolite in metabolites:
    #                 synonym_results = self.neo4j_connection.run_query(f'''
    #                     MATCH (m:Metabolite)
    #                     WHERE toLower(m.name) = toLower('{metabolite}')
    #                     OR EXISTS {{
    #                         MATCH (m)-[:HAS_SYNONYM]->(s:Synonym)
    #                         WHERE toLower(s.synonymText) = toLower('{metabolite}')
    #                     }}
    #                     RETURN m.description
    #                 ''')
    #                 neo4j_results += synonym_results

    #             # 3D) If DB has some results, summarize them
    #             if len(neo4j_results) > 0:
    #                 yield self._format_message("Results", f"Query results: {neo4j_results}")

    #                 summary_inputs = {"query_results": neo4j_results, "question": user_question}
    #                 summary_accumulator: List[str] = []

    #                 async for sse_message in self._stream_and_accumulate(
    #                     self.summary_chain,
    #                     "Summary",
    #                     summary_inputs,
    #                     summary_accumulator
    #                 ):
    #                     yield sse_message

    #                 full_summary_response = "".join(summary_accumulator)
    #                 print(full_summary_response)

    #             else:
    #                 # <-- HMDB fallback changes START
    #                 # If the DB returned no results, we attempt the HMDB fallback
    #                 if len(metabolites) > 0 and self.hmdb_client is not None:
    #                     # We'll just pick the first metabolite (you could handle multiple if desired)
    #                     hmdb_id = metabolites[0]

    #                     # If the user provided "HMDB0001234" as an entity, hmdb_id is that. 
    #                     # If the user only gave a name (e.g. "lactic acid"), you might need a name-based search. 
    #                     # For now, assume they gave an actual HMDB ID:
    #                     endpoint = f"metabolites/{hmdb_id}"
    #                     fallback_data = self.hmdb_client.get(endpoint)

    #                     if fallback_data:
    #                         yield self._format_message("Results", f"HMDB fallback results: {fallback_data}")

    #                         # Summarize the fallback data
    #                         summary_inputs = {
    #                             "query_results": fallback_data,
    #                             "question": user_question
    #                         }
    #                         summary_accumulator: List[str] = []

    #                         async for sse_message in self._stream_and_accumulate(
    #                             self.summary_chain,
    #                             "Summary (HMDB Fallback)",
    #                             summary_inputs,
    #                             summary_accumulator
    #                         ):
    #                             yield sse_message
    #                     else:
    #                         yield self._format_message(
    #                             "Results",
    #                             "No data found in DB, and HMDB fallback also returned nothing."
    #                         )
    #                 else:
    #                     yield self._format_message(
    #                         "Results",
    #                         "No data found in DB, and no metabolite ID for HMDB fallback."
    #                     )
    #                 # <-- HMDB fallback changes END

    #         else:
    #             # If query_plan.should_query = false, just respond
    #             yield self._format_message("Response", f"No database query needed. {query_plan.reasoning}")

    #     except Exception as e:
    #         yield self._format_message("Error", f"Error in pipeline: {e}")


    # async def run_pipeline(self, user_question: str) -> AsyncGenerator[str, None]:
    #     try:
    #         extraction_inputs = {"question": user_question, "schema": self.neo4j_schema_text}
    #         extraction_accumulator: List[str] = []
    #         async for sse_message in self._stream_and_accumulate(self.entity_chain, "Extracting entities", extraction_inputs, extraction_accumulator):
    #             yield sse_message
    #         full_extraction_response = "".join(extraction_accumulator)
    #         print(full_extraction_response)
    #         entities = self.entity_parser.parse(full_extraction_response)

    #         metabolites = []
    #         for entity in entities.entities:
    #             if entity.type == "Metabolite":
    #                 metabolites.append(entity.name)



    #         planning_inputs = { "question": user_question, "entities": full_extraction_response, "schema": self.neo4j_schema_text}
    #         planning_accumulator: List[str] = []
    #         async for sse_message in self._stream_and_accumulate(self.query_plan_chain, "Query planning", planning_inputs, planning_accumulator):
    #             yield sse_message
    #         full_query_plan_response = "".join(planning_accumulator)
    #         query_plan = self.query_plan_parser.parse(full_query_plan_response)


    #         query_inputs = {"query_plan": query_plan, "schema": self.neo4j_schema_text}
    #         query_accumulator: List[str] = []
    #         if query_plan.should_query:
    #             async for sse_message in self._stream_and_accumulate(self.query_chain, "Query execution", query_inputs, query_accumulator):
    #                     yield sse_message
    #             full_query_response = "".join(query_accumulator)
    #             neo4j_results = self.neo4j_connection.run_query(full_query_response)


    #             for metabolite in metabolites:
    #                 neo4j_results += self.neo4j_connection.run_query(f'''
    #                     MATCH (m:Metabolite)
    #                     WHERE toLower(m.name) = toLower('{metabolite}')
    #                     OR EXISTS {{
    #                         MATCH (m)-[:HAS_SYNONYM]->(s:Synonym)
    #                         WHERE toLower(s.synonymText) = toLower('{metabolite}')
    #                     }}
    #                     RETURN m.description
    #                 ''')

    #             yield self._format_message("Results", f"Query results: {neo4j_results}")

    #             summary_inputs = {"query_results": neo4j_results, "question": user_question}
    #             summary_accumulator: List[str] = []
    #             async for sse_message in self._stream_and_accumulate(self.summary_chain, "Summary", summary_inputs, summary_accumulator):
    #                 yield sse_message
    #             full_summary_response = "".join(summary_accumulator)
    #             print(full_summary_response)

    #         else:
    #             yield self._format_message("Response", f"No database query needed. {query_plan.reasoning}")

    #     except Exception as e:
    #         yield self._format_message("Error", f"Error in pipeline: {e}")
