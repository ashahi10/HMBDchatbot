import json
from typing import List, AsyncGenerator, Optional, Any, Dict


from langchain_core.runnables import RunnableSequence, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser, PydanticOutputParser
from pydantic import BaseModel, Field

from services.llm_service import MultiLLMService
from pipeline.prompts import entity_prompt, query_plan_prompt, query_prompt, summary_prompt, api_reasoning_prompt

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
        self.api_reasoning_chain = self._create_chain(
            {"api_data": lambda x: x["api_data"], "question": lambda x: x["question"]},
            api_reasoning_prompt,
            streaming=True,
            parser=None,
            model_type="summary"
        )

    def _create_chain(self, assignment_funcs: dict, chain_prompt, streaming: bool, parser: Optional[PydanticOutputParser] = None, model_type: str = "query") -> RunnableSequence:
        if self.llm_service.provider == "ollama":
            streaming = False
            
        # if model_type == "summary":
        #     self.llm_service.default_query_model = self.llm_service.default_summary_model
            
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
        
        if "<think>" in text and "</think>" in text:
            start_idx = text.find("<think>") + len("<think>")
            end_idx = text.find("</think>")
            thinking_text = text[start_idx:end_idx].strip()
            clean_text = text[:text.find("<think>")] + text[text.find("</think>") + len("</think>"):]
            clean_text = clean_text.strip()
            
        return thinking_text, clean_text
    



        

    async def _process_stream(self, stream, section: str, inputs: Dict[str, Any]) -> AsyncGenerator[str, None]:
        async for chunk in stream.astream(inputs):
            if chunk:
                chunk_text = chunk if isinstance(chunk, str) else str(chunk)
                thinking_text, clean_text = self._process_text_with_thinking(chunk_text)
                
                if thinking_text:
                    yield self._format_message("Thinking", thinking_text)
                
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
                
            if message.get("section") != "Thinking" and message.get("text") not in BAD_RESPONSES + ["DONE"]:
                accumulator.append(message.get("text", ""))
            yield sse_message

    async def _match_entities(self, entity_name: str, entity_type: str) -> List[dict]:
        pass

    def _merge_summaries(self, neo4j_summary: str, api_summary: str) -> str:
        """
        Merges Neo4j and API summaries into a unified response.
        """
        if not neo4j_summary.strip():
            return api_summary
        if not api_summary.strip():
            return neo4j_summary

        return (
            f"{neo4j_summary.strip()}\n\n"
            f"---\n\n"
            f"**🔍 Additional Insights from HMDB API:**\n\n"
            f"{api_summary.strip()}"
        )

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
            first_metabolite = metabolites[0] if metabolites else None
            if first_metabolite:
                if first_metabolite.startswith("HMDB"):
                    payload = {"hmdb_id": [first_metabolite]}
                else:
                    payload = {"name": first_metabolite}
            else:
                payload = {}
            # 2) Query Planning
            planning_inputs = {"question": user_question, "entities": full_extraction_response, "schema": self.neo4j_schema_text}
            planning_accumulator: List[str] = []
            async for sse_message in self._stream_and_accumulate(self.query_plan_chain, "Query planning", planning_inputs, planning_accumulator):
                yield sse_message
            full_query_plan_response = "".join(planning_accumulator)
            print(f"\n[DEBUG] Query Planning Response: {full_query_plan_response}")
            query_plan = self.query_plan_parser.parse(full_query_plan_response)

            if query_plan.should_query:
                query_inputs = {"query_plan": query_plan, "schema": self.neo4j_schema_text}
                query_accumulator: List[str] = []
                async for sse_message in self._stream_and_accumulate(self.query_chain, "Query execution", query_inputs, query_accumulator):
                    yield sse_message
                full_query_response = "".join(query_accumulator)
                print(f"\n[DEBUG] Generated Cypher Query: {full_query_response}")

                try:
                    neo4j_results = self.neo4j_connection.run_query(full_query_response)
                    print(f"\n[DEBUG] Neo4j raw query results: {neo4j_results}")

                    # Check if we need fallback
                    should_fallback = analyze_missing_fields(user_question, neo4j_results)
                    if should_fallback:
                        print("[DEBUG] Neo4j results are insufficient, initiating HMDB fallback...")
                    elif not neo4j_results or neo4j_results == [None] or neo4j_results == [""]:
                        print("[DEBUG] No valid results from Neo4j, initiating HMDB fallback...")
                        should_fallback = True

                    # Get additional description results
                    for metabolite in metabolites:
                        more_results = self.neo4j_connection.run_query(f"""
                            MATCH (m:Metabolite)
                            WHERE toLower(m.name) = toLower('{metabolite}')
                            OR EXISTS {{ MATCH (m)-[:HAS_SYNONYM]->(s:Synonym) 
                                        WHERE toLower(s.synonymText) = toLower('{metabolite}') }}
                            RETURN m.description
                        """)
                        if more_results:
                            neo4j_results += more_results

                    # Handle fallback if needed
                    if should_fallback and len(metabolites) > 0 and self.hmdb_client:
                        first_metabolite = metabolites[0]
                        fallback_data = None

                        if first_metabolite.startswith("HMDB"):
                            print(f"\n[DEBUG] Making API call for ID '{first_metabolite}'")
                            payload = {"hmdb_id": [first_metabolite]}
                            fallback_data = self.hmdb_client.post("metabolites", payload)
                        else:
                            print(f"\n[DEBUG] Making API search call for name '{first_metabolite}'")
                            payload = {"name": first_metabolite}
                            fallback_data = self.hmdb_client.post("metabolites/search", payload)

                        if fallback_data and "found" in fallback_data:
                            filtered_fallback_data = self._filter_hmdb_response(fallback_data)
                            print(f"\n[DEBUG] Filtered HMDB fallback data: {filtered_fallback_data}")

                            # Run API reasoning chain
                            api_reasoning_inputs = {
                                "api_data": filtered_fallback_data,
                                "question": user_question
                            }
                            api_reasoning_accumulator: List[str] = []
                            async for sse_message in self._stream_and_accumulate(
                                self.api_reasoning_chain,
                                "API Summary",
                                api_reasoning_inputs,
                                api_reasoning_accumulator
                            ):
                                yield sse_message

                            # If we have Neo4j results, run DB summary chain too
                            # if neo4j_results and neo4j_results != [None] and neo4j_results != [""]:
                            if neo4j_results and not should_fallback:
                                summary_inputs = {
                                    "query_results": neo4j_results,
                                    "question": user_question
                                }
                                summary_accumulator: List[str] = []
                                async for sse_message in self._stream_and_accumulate(
                                    self.summary_chain,
                                    "DB Summary",
                                    summary_inputs,
                                    summary_accumulator
                                ):
                                    yield sse_message

                                # Merge both summaries
                                final_summary = self._merge_summaries(
                                    "".join(summary_accumulator),
                                    "".join(api_reasoning_accumulator)
                                )
                                yield self._format_message("Answer", final_summary)
                            else:
                                # If no Neo4j results, just use API summary
                                yield self._format_message("Answer", "".join(api_reasoning_accumulator))
                            return

                    # If no fallback needed or fallback failed, use Neo4j results
                    if neo4j_results and neo4j_results != [None] and neo4j_results != [""]:
                        summary_inputs = {
                            "query_results": neo4j_results,
                            "question": user_question
                        }
                        summary_accumulator: List[str] = []
                        async for sse_message in self._stream_and_accumulate(
                            self.summary_chain,
                            "Answer",
                            summary_inputs,
                            summary_accumulator
                        ):
                            yield sse_message
                    else:
                        yield self._format_message("Answer", "I apologize, but I couldn't find the information you're looking for in our database.")

                except Exception as e:
                    print(f"\n[ERROR] Neo4j query execution failed: {e}")
                    yield self._format_message("Answer", "I apologize, but I encountered an error while querying the database.")
            else:
                yield self._format_message("Response", f"No database query needed. {query_plan.reasoning}")

        except Exception as e:
            yield self._format_message("Error", f"Error in pipeline: {e}")

    def _filter_hmdb_response(self, hmdb_data: dict) -> dict:
        """Preserves full HMDB API response and chunks structured data for LLM use."""
        if not isinstance(hmdb_data, dict) or "found" not in hmdb_data or not hmdb_data["found"]:
            return {"error": "No valid data found in HMDB response"}

        # ✅ Store full API response internally
        self.last_hmdb_api_result = hmdb_data  # For internal LLM access or chaining
        with open("latest_hmdb_api_full.json", "w") as f:
            json.dump(hmdb_data, f, indent=2)

        # ✅ Instead of truncation, chunk by tag/key for LLM compatibility
        MAX_METABOLITES = 10
        metabolites = hmdb_data["found"][:MAX_METABOLITES]
        chunked_metabolites = []

        for metabolite in metabolites:
            chunked = {}
            for key, value in metabolite.items():
                if isinstance(value, dict):
                    chunked[key] = self._chunk_nested_dict(value)
                elif isinstance(value, list):
                    chunked[key] = self._chunk_list(value)
                elif isinstance(value, str):
                    chunked[key] = self._chunk_text(value)
                else:
                    chunked[key] = value
            chunked_metabolites.append(chunked)

        return {"metabolites": chunked_metabolites}


    # def _truncate_text(self, text: str, max_sentences: int = 3) -> str:
    #     """Truncates text to the first max_sentences sentences."""
    #     if not text:
    #         return ""
    #     sentences = text.split(". ")
    #     truncated = ". ".join(sentences[:max_sentences])
    #     return truncated + ("." if len(sentences) > max_sentences and truncated else "")

    # def _limit_list(self, lst: list, limit: int = 3) -> list:
    #     """Limits a list to 'limit' items, handling nested structures."""
    #     if not lst:
    #         return []
    #     limited = lst[:limit]
    #     processed = []
        
    #     for item in limited:
    #         if isinstance(item, dict):
    #             processed.append(self._filter_nested_dict(item, limit=3))
    #         elif isinstance(item, str):
    #             processed.append(item)
    #         else:
    #             processed.append(item)
        
    #     if len(lst) > limit:
    #         processed.append(f"...and {len(lst) - limit} more")
    #     return processed

    # def _filter_nested_dict(self, d: dict, limit: int = 3) -> dict:
    #     """Recursively filters a nested dictionary, limiting lists within it."""
    #     filtered = {}
    #     for key, value in d.items():
    #         if value is None:
    #             filtered[key] = ""
    #         elif isinstance(value, str):
    #             filtered[key] = self._truncate_text(value, max_sentences=3)
    #         elif isinstance(value, list):
    #             filtered[key] = self._limit_list(value, limit=limit)
    #         elif isinstance(value, dict):
    #             filtered[key] = self._filter_nested_dict(value, limit=limit)
    #         else:
    #             filtered[key] = value
    #     return filtered


    def _chunk_text(self, text: str, max_tokens: int = 150) -> list:
        """Chunks long text into readable parts for LLM input."""
        sentences = text.split(". ")
        chunks = []
        current = []
        count = 0

        for s in sentences:
            current.append(s)
            count += len(s.split())
            if count >= max_tokens:
                chunks.append(". ".join(current))
                current = []
                count = 0

        if current:
            chunks.append(". ".join(current))

        return chunks

    def _chunk_list(self, lst: list, max_items_per_chunk: int = 5) -> list:
        """Breaks long lists into manageable LLM-readable chunks."""
        chunks = []
        for i in 0, len(lst), max_items_per_chunk:
            chunk = lst[i:i + max_items_per_chunk]
            chunks.append(chunk)
        return chunks

    def _chunk_nested_dict(self, d: dict) -> list:
        """Converts nested dict into a list of labeled key-value chunks."""
        return [f"{k}: {v}" for k, v in d.items()]


def analyze_missing_fields(question: str, results: List[Dict]) -> bool:
    """
    Analyzes whether the results from Neo4j are insufficient to answer the user's question.
    
    This function checks for:
    - Sparse results (too many fields missing, None, or empty)
    - Irrelevant outputs (e.g., metabolite names returned when question asks for something else)
    - Lack of key information matching the user query intent

    Returns:
        True if API fallback is needed.
        False if Neo4j results are likely sufficient.
    """
    if not results or not isinstance(results, list):
        return True  # Completely empty or malformed result

    if all(result is None or result == {} for result in results):
        return True  # All results are null/empty dicts

    # Normalize keys from all result entries
    all_keys = set()
    for row in results:
        if isinstance(row, dict):
            all_keys.update(row.keys())

    empty_field_threshold = 0.3  # >30% missing fields triggers fallback
    missing_counter = 0
    total_fields = 0

    for row in results:
        if not isinstance(row, dict):
            continue
        for key in all_keys:
            total_fields += 1
            value = row.get(key)
            if value in (None, "", [], {}, "N/A", "null"):
                missing_counter += 1

    if total_fields == 0:
        return True

    empty_ratio = missing_counter / total_fields

    # Heuristic: if user explicitly asked for something and result is sparse, fallback
    question_lower = question.lower()
    if ("what is" in question_lower or "give me" in question_lower or "details" in question_lower):
        if empty_ratio > empty_field_threshold:
            return True

    return empty_ratio > 0.8  # fallback if 80%+ is garbage regardless of question
