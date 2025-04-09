import json
from typing import List, AsyncGenerator, Optional, Any, Dict, Tuple, Union, Set
import re


from langchain_core.runnables import RunnableSequence, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser, PydanticOutputParser
from pydantic import BaseModel, Field

from backend.services.llm_service import MultiLLMService
from backend.pipeline.prompts import entity_prompt, query_plan_prompt, query_prompt, summary_prompt, api_reasoning_prompt, query_necessity_prompt, general_answer_prompt, intent_splitting_prompt

BAD_RESPONSES = ["```", "json", "```json", "```cypher", "```cypher\n", "```", "cy", "pher"]

class Entity(BaseModel):
    name: str = Field(..., description="The name of the entity")
    type: str = Field(..., description="The entity category")
    confidence: float = Field(..., description="Confidence score (0-1)")

class EntityList(BaseModel):
    entities: List[Entity] = Field(..., description="List of extracted entities")

class Intent(BaseModel):
    intent_text: str = Field(..., description="The specific text portion of the original question for this intent")
    intent_type: str = Field(..., description="The intent category")
    confidence: float = Field(..., description="Confidence score (0-1)")

class IntentList(BaseModel):
    intents: List[Intent] = Field(..., description="List of extracted intents")

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
        self.intent_parser = PydanticOutputParser(pydantic_object=IntentList)
        
        # Add intent splitting chain for multi-intent detection
        self.intent_splitting_chain = self._create_chain(
            {"question": lambda x: x["question"]},
            intent_splitting_prompt,
            streaming=False,
            parser=None,
            model_type="query"
        )
        
        # PHASE 4: Add chain for query necessity detection
        self.query_necessity_chain = self._create_chain(
            {"question": lambda x: x["question"]},
            query_necessity_prompt,
            streaming=False,
            parser=None,
            model_type="query"
        )
        
        # Chain for direct answers to general questions
        self.general_answer_chain = self._create_chain(
            {"question": lambda x: x["question"], "context": lambda x: x.get("context", "")},
            general_answer_prompt,
            streaming=True,
            parser=None,
            model_type="summary"
        )

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

    # PHASE 4: Add method to determine if a query requires database access
    async def _should_query_llm_decision(self, question: str) -> bool:
        """
        Use the LLM to determine if the question requires database query.
        
        Args:
            question: The user's question
            
        Returns:
            bool: True if database query is needed, False otherwise
        """
        try:
            # Get decision from LLM
            inputs = {"question": question}
            result = await self.query_necessity_chain.ainvoke(inputs)
            
            # Clean the result and extract the YES/NO decision
            result = result.strip().upper()
            
            return "YES" in result
        except Exception as e:
            print(f"Error in query necessity decision: {e}")
            # Default to True (safer to query than not)
            return True
            
    # PHASE 1: Add method to identify multiple intents in a question
    async def _split_intents(self, question: str) -> List[Intent]:
        """
        Use the LLM to identify multiple intents in a question.
        
        Args:
            question: The user's question
            
        Returns:
            List[Intent]: A list of Intent objects, each with intent_text, intent_type, and confidence
        """
        try:
            # Get intents from LLM
            inputs = {"question": question}
            result = await self.intent_splitting_chain.ainvoke(inputs)
            
            # Parse the result as JSON
            try:
                # Clean the result if it contains markdown code blocks
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
                    print(f"Invalid intent structure, falling back to single intent: {cleaned_result}")
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
            except (json.JSONDecodeError, ValueError) as e:
                print(f"Error parsing intent JSON: {e}")
                print(f"Raw result: {result}")
                # Fall back to treating as a single intent
                return [Intent(intent_text=question, intent_type="GetBasicInfo", confidence=1.0)]
        except Exception as e:
            print(f"Error in intent splitting: {e}")
            # Default to treating as a single intent
            return [Intent(intent_text=question, intent_type="GetBasicInfo", confidence=1.0)]

    # PHASE 3: Add method to process raw data from memory for reuse
    def _process_memory_raw_data(self, memory_entry: Dict, query_intent: str) -> Dict:
        """
        Process and filter raw_data from memory for reuse.
        
        Args:
            memory_entry: Memory entry with raw_data
            query_intent: Current query intent to filter relevant data
            
        Returns:
            Processed raw_data in a format suitable for the summarizer
        """
        if not memory_entry or "raw_data" not in memory_entry:
            return None
            
        raw_data = memory_entry.get("raw_data", {})
        
        # Preprocess and filter based on query intent
        # This helps prevent context overflow by removing irrelevant data
        if "entity_extraction" in raw_data:
            # Keep entity extraction data as it's useful for context
            pass
            
        # Check if there are specific result types to extract based on query intent
        relevant_data = {}
        
        # Return processed data
        return raw_data
    
    # PHASE 3: Check if memory contains relevant raw data for reuse
    def _find_reusable_memory_data(self, memory_results: List[Dict], query_intent: str, 
                                  confidence_threshold: float = 0.8) -> Tuple[bool, Optional[Dict]]:
        """
        Find memory entries with reusable raw_data.
        
        Args:
            memory_results: List of memory entries
            query_intent: Current query intent
            confidence_threshold: Minimum confidence score to reuse memory
            
        Returns:
            Tuple of (found_reusable, processed_data)
        """
        if not memory_results:
            return False, None
            
        # Look for high-confidence matches with raw_data
        for entry in memory_results:
            if (entry.get("relevance_score", 0) >= confidence_threshold and 
                "raw_data" in entry and entry["raw_data"]):
                
                # Process the raw_data for reuse
                processed_data = self._process_memory_raw_data(entry, query_intent)
                if processed_data:
                    return True, processed_data
                    
        return False, None

    async def run_pipeline(self, user_question: str, conversation_history: List = None, relevant_history: List = None) -> AsyncGenerator[str, None]:
        try:
            # Enhanced context generation to properly handle entity continuity
            context_info = ""
            entity_context = {}
            current_entities = set()
            
            # Extract potential entities from current question for matching
            # Simple regex-based extraction of potential entity references
            entity_patterns = [
                (r'\b[A-Z][a-z]+(?:\s+[a-z]+)*\b', 'Named Entity'),  # Capitalized names like "Citric Acid"
                (r'\b[A-Z][a-z]?[0-9]*(?:[A-Z][a-z]?[0-9]*)*\b', 'Chemical Formula'),  # Chemical formulas like C6H12O6
                (r'\bHMDB\d+\b', 'HMDB ID'),  # HMDB IDs
                (r'\b[A-Z]{14}-[A-Z]{10}-[A-Z]\b', 'InChIKey')  # InChIKeys
            ]
            
            for pattern, entity_type in entity_patterns:
                matches = re.findall(pattern, user_question)
                for match in matches:
                    if len(match) > 2:  # Avoid single letters
                        current_entities.add(match.lower())
            
            # PHASE 1: Add intent splitting early in the process
            # Analyze the question to identify if it contains multiple intents
            intents = await self._split_intents(user_question)
            has_multiple_intents = len(intents) > 1
            
            # Store the detected intents as an attribute for future phases
            self.current_intents = intents
            
            # Log the detected intents
            if has_multiple_intents:
                intent_info = ", ".join([f"'{intent.intent_text}' ({intent.intent_type})" for intent in intents])
                yield self._format_message("Thinking", f"Your question contains multiple parts: {intent_info}")
                print(f"\n[DEBUG] Multiple intents detected: {intent_info}")
            else:
                print(f"\n[DEBUG] Single intent detected: {intents[0].intent_type}")
            
            # PHASE 4: Add early exit for questions not requiring database lookup
            should_query = await self._should_query_llm_decision(user_question)
            if not should_query:
                # If query is not necessary, use general answer chain
                yield self._format_message("Thinking", "This appears to be a general question that doesn't require database lookup.")
                
                # Extract context from relevant history
                memory_context = ""
                if relevant_history:
                    context_parts = []
                    for idx, turn in enumerate(relevant_history[:3]):  # Use up to 3 most relevant turns
                        if turn.get("user_query") and turn.get("answer"):
                            context_parts.append(f"Previous Q: {turn['user_query']}")
                            context_parts.append(f"Previous A: {turn['answer']}")
                    if context_parts:
                        memory_context = "\n".join(context_parts)
                
                # Generate direct answer
                general_inputs = {
                    "question": user_question,
                    "context": memory_context
                }
                
                # Stream the response
                answer_accumulator = []
                async for chunk in self.general_answer_chain.astream(general_inputs):
                    if isinstance(chunk, str):
                        answer_accumulator.append(chunk)
                        yield self._format_message("Answer", chunk)
                        
                # Signal completion        
                yield self._format_message("DONE", "")
                return
            
            # Check for relevant context from conversation history
            if relevant_history and len(relevant_history) > 0:
                # First, identify the most relevant turns that match the current entities
                matching_entity_turns = []
                non_matching_turns = []
                
                for turn in relevant_history:
                    # Extract entities from the memory turn
                    turn_entities = set()
                    if turn.get("entity"):
                        turn_entities.add(turn.get("entity").lower())
                    
                    # Check for entity overlap
                    if current_entities and turn_entities:
                        # Check if any current entity matches any turn entity
                        if any(current_entity in turn_entity or turn_entity in current_entity 
                               for current_entity in current_entities for turn_entity in turn_entities):
                            matching_entity_turns.append(turn)
                        else:
                            # Store for potential use if no matching turns are found
                            non_matching_turns.append(turn)
                    else:
                        non_matching_turns.append(turn)
                
                # Prioritize turns with matching entities
                prioritized_turns = matching_entity_turns + non_matching_turns
                
                # Format context information from up to 3 most relevant turns
                context_parts = []
                used_turns = 0
                
                for turn in prioritized_turns:
                    # Always include matching entity turns
                    if turn in matching_entity_turns or used_turns < 2:
                        if turn.get("user_query") and turn.get("answer"):
                            # Check for entity overlap to mark especially relevant information
                            is_entity_match = turn in matching_entity_turns
                            
                            # Format with entity relevance marker if applicable
                            prefix = "Previous (Entity Match): " if is_entity_match else "Previous: "
                            
                            context_parts.append(
                                f"{prefix}Q: {turn['user_query']}\n"
                                f"{prefix}A: {turn['answer']}"
                            )
                            
                            used_turns += 1
                            
                            # Stop after 2 turns or 3 matching entity turns
                            if (is_entity_match and used_turns >= 3) or used_turns >= 2:
                                break
                
                if context_parts:
                    context_info = "Related information from previous conversation:\n" + "\n\n".join(context_parts)
                    
                    # Also extract entity context for clarification (e.g., to resolve ambiguity)
                    for turn in prioritized_turns[:2]:  # Use only top 2 turns for entity context
                        if turn.get("entity") and turn.get("answer"):
                            entity = turn.get("entity")
                            if entity not in entity_context:
                                # Extract a short description from the answer
                                answer = turn.get("answer")
                                description = answer[:200] + "..." if len(answer) > 200 else answer
                                entity_context[entity] = description
            
            # Add recent turns for conversational continuity context
            recent_context = ""
            if conversation_history and len(conversation_history) > 0:
                # Extract most recent turns for continuity
                recent_parts = []
                for idx, turn in enumerate(conversation_history[-3:]):  # Use up to 3 most recent turns
                    if turn.get("user_query") and turn.get("answer"):
                        recent_parts.append(
                            f"User: {turn['user_query']}\n"
                            f"Assistant: {turn['answer']}"
                        )
                
                if recent_parts:
                    recent_context = "Recent conversation:\n" + "\n\n".join(recent_parts)
            
            # Add entity clarification context if available and current question seems ambiguous
            entity_clarification = ""
            ambiguity_terms = ["it", "this", "that", "the", "compound", "molecule", "substance", "above"]
            has_ambiguity = any(term in user_question.lower().split() for term in ambiguity_terms)
            
            if entity_context and has_ambiguity:
                clarification_parts = []
                for entity, description in entity_context.items():
                    # Extract a shorter description suitable for clarification
                    first_sentence = description.split(".")[0] + "." if "." in description else description
                    clarification_parts.append(f"Entity '{entity}': {first_sentence}")
                
                if clarification_parts:
                    entity_clarification = "Note - Previous entities mentioned:\n" + "\n".join(clarification_parts)
            
            # Combine question with context
            augmented_question = user_question
            extra_contexts = []
            
            if recent_context:
                extra_contexts.append(recent_context)
            
            if context_info:
                extra_contexts.append(context_info)
                
            if entity_clarification:
                extra_contexts.append(entity_clarification)
                
            if extra_contexts:
                augmented_question = f"{user_question}\n\n" + "\n\n".join(extra_contexts)
            
            # 1) Entity Extraction
            extraction_inputs = {"question": augmented_question, "schema": self.neo4j_schema_text}
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
            planning_inputs = {"question": augmented_question, "entities": full_extraction_response, "schema": self.neo4j_schema_text}
            planning_accumulator: List[str] = []
            async for sse_message in self._stream_and_accumulate(self.query_plan_chain, "Query planning", planning_inputs, planning_accumulator):
                yield sse_message
            full_query_plan_response = "".join(planning_accumulator)
            print(f"\n[DEBUG] Query Planning Response: {full_query_plan_response}")
            query_plan = self.query_plan_parser.parse(full_query_plan_response)

            # PHASE 3: Check if we have reusable raw data from memory
            memory_raw_data = None
            used_memory_data = False
            
            if relevant_history and query_plan.should_query:
                # Check if any relevant memory entry has reusable raw_data
                has_reusable_data, memory_raw_data = self._find_reusable_memory_data(
                    relevant_history, 
                    query_plan.query_intent
                )
                
                if has_reusable_data and memory_raw_data:
                    used_memory_data = True
                    yield self._format_message("Thinking", "Found relevant data from previous queries that can be reused.")

            if query_plan.should_query:
                # Skip query execution if we have memory data
                neo4j_results = None
                
                if not used_memory_data:
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

                                # PHASE 3: Store API results in memory_raw_data if not already present
                                if used_memory_data and "api_data" not in memory_raw_data:
                                    memory_raw_data["api_data"] = filtered_fallback_data
                                elif not used_memory_data:
                                    memory_raw_data = {"api_data": filtered_fallback_data}
                                    
                                # Run API reasoning chain
                                api_reasoning_inputs = {
                                    "api_data": memory_raw_data.get("api_data", filtered_fallback_data),
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
                                    # PHASE 3: Mark if using memory data
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
                            # PHASE 3: Store Neo4j results in memory_raw_data
                            if used_memory_data and "neo4j_results" not in memory_raw_data:
                                memory_raw_data["neo4j_results"] = neo4j_results
                            elif not used_memory_data:
                                memory_raw_data = {"neo4j_results": neo4j_results}
                                
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
                
                # PHASE 3: If we have memory_raw_data, use it directly
                elif used_memory_data:
                    # Process based on what raw data we have
                    if "neo4j_results" in memory_raw_data and memory_raw_data["neo4j_results"]:
                        # Use memory's Neo4j results
                        summary_inputs = {
                            "query_results": memory_raw_data["neo4j_results"],
                            "question": user_question
                        }
                        summary_accumulator: List[str] = []
                        
                        # Inform the user we're using memory data
                        yield self._format_message("Thinking", "Using previously retrieved database information to answer your question.")
                        
                        # Stream the summary
                        async for sse_message in self._stream_and_accumulate(
                            self.summary_chain,
                            "DB Summary", 
                            summary_inputs,
                            summary_accumulator
                        ):
                            yield sse_message
                            
                        # If we also have API data, use it too
                        api_summary = ""
                        if "api_data" in memory_raw_data and memory_raw_data["api_data"]:
                            api_reasoning_inputs = {
                                "api_data": memory_raw_data["api_data"],
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
                            api_summary = "".join(api_reasoning_accumulator)
                            
                        # Merge both summaries if we have API data
                        if api_summary:
                            final_summary = self._merge_summaries(
                                "".join(summary_accumulator),
                                api_summary
                            )
                            yield self._format_message("Answer", final_summary)
                        else:
                            yield self._format_message("Answer", "".join(summary_accumulator))
                            
                    elif "api_data" in memory_raw_data and memory_raw_data["api_data"]:
                        # Only have API data from memory
                        yield self._format_message("Thinking", "Using previously retrieved API information to answer your question.")
                        
                        api_reasoning_inputs = {
                            "api_data": memory_raw_data["api_data"],
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
                            
                        yield self._format_message("Answer", "".join(api_reasoning_accumulator))
                    else:
                        # No usable data in memory after all
                        yield self._format_message("Thinking", "Memory data could not be used. Proceeding with regular query.")
                        
                        # Fallback to normal query
                        query_inputs = {"query_plan": query_plan, "schema": self.neo4j_schema_text}
                        query_accumulator: List[str] = []
                        async for sse_message in self._stream_and_accumulate(self.query_chain, "Query execution", query_inputs, query_accumulator):
                            yield sse_message
                        # ... (continue with regular query flow)
            else:
                yield self._format_message("Response", f"No database query needed. {query_plan.reasoning}")

        except Exception as e:
            yield self._format_message("Error", f"Error in pipeline: {e}")

    def _filter_hmdb_response(self, hmdb_data: dict) -> dict:
        """Preserves full HMDB API response and chunks structured data for LLM use."""
        if not isinstance(hmdb_data, dict) or "found" not in hmdb_data or not hmdb_data["found"]:
            return {"error": "No valid data found in HMDB response"}

        # ✅ Store full API response in cache folder instead of current directory
        self.last_hmdb_api_result = hmdb_data  # For internal LLM access or chaining
        
        # Save to cache folder (if it exists) or default to current directory
        try:
            from pathlib import Path
            cache_dir = Path("cache/api_responses")
            if not cache_dir.exists():
                cache_dir.mkdir(parents=True, exist_ok=True)
            
            cache_file = cache_dir / "latest_hmdb_api_full.json"
            
            with open(cache_file, "w") as f:
                import json
                json.dump(hmdb_data, f, indent=2)
        except Exception as e:
            print(f"Failed to save HMDB response to cache: {e}")
            # Fallback to the original location if cache fails
            try:
                with open("latest_hmdb_api_full.json", "w") as f:
                    import json
                    json.dump(hmdb_data, f, indent=2)
            except Exception as e:
                print(f"Failed to save HMDB response to fallback location: {e}")

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
