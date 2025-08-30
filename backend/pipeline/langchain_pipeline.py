import json
from typing import List, AsyncGenerator, Optional, Any, Dict, Tuple, Union, Set
import re
import os
from dotenv import load_dotenv
import asyncio

from langchain_core.runnables import RunnableSequence, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser, PydanticOutputParser
from pydantic import BaseModel, Field

from backend.services.llm_service import MultiLLMService
from backend.pipeline.prompts import entity_prompt, query_plan_prompt, query_prompt, summary_prompt, query_necessity_prompt, general_answer_prompt, intent_splitting_prompt, aggregator_prompt
from backend.utils.enrich_links import inject_hyperlinks
from backend.pipeline.optimized_entity_matcher import OptimizedEntityMatcher
# Phase 4: Import spectra services
from backend.services.spectra_trigger_service import SpectraPipelineIntegrator
from backend.services.spectra_service import SpectraProcessor
from backend.services.spectra_graph_service import SpectraVisualizationPipeline, PlotFormat, PlotStyle

load_dotenv()

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
        # HMDB client now activated for spectra endpoint usage
        self.hmdb_client = hmdb_client
        self.env_groq_api_key = os.getenv("GROQ_API_KEY")
        self.env_groq_api_key_generation = os.getenv("GROQ_API_KEY_GENERATION")
        # Add Qwen API keys
        self.env_qwen_api_key = os.getenv("QWEN_API")
        self.env_qwen_api_key_generation = os.getenv("QWEN")

        # Initialize optimized entity matcher for Phase 3 integration
        self.entity_matcher = OptimizedEntityMatcher(neo4j_connection)
        
        # Phase 4: Initialize spectra services
        self.spectra_integrator = SpectraPipelineIntegrator(neo4j_connection, hmdb_client)
        self.spectra_visualization = SpectraVisualizationPipeline()

        self.entity_parser = PydanticOutputParser(pydantic_object=EntityList)
        self.query_plan_parser = PydanticOutputParser(pydantic_object=QueryPlan)
        self.intent_parser = PydanticOutputParser(pydantic_object=IntentList)
        
        # Add intent splitting chain for multi-intent detection
        self.intent_splitting_chain = self._create_chain(
            {"question": lambda x: x["question"]},
            intent_splitting_prompt,
            streaming=False,
            parser=None,
            model_type="query",
            use_env_key=True,
            custom_model="llama-3.1-8b-instant",
            custom_temperature=0.1,
            custom_max_tokens=1024
        )
        
        # PHASE 4: Add chain for query necessity detection
        self.query_necessity_chain = self._create_chain(
            {"question": lambda x: x["question"]},
            query_necessity_prompt,
            streaming=False,
            parser=None,
            model_type="query",
            use_env_key=True,
            custom_model="llama-3.1-8b-instant",
            custom_temperature=0.1,
            custom_max_tokens=1024
        )
        
        # PHASE 4: Add aggregator chain for combining multiple sub-intent results
        self.aggregator_chain = self._create_chain(
            {"question": lambda x: x["question"], "sub_intent_results": lambda x: x["sub_intent_results"]},
            aggregator_prompt,
            streaming=True,
            parser=None,
            model_type="summary",
            use_env_key=True,
            custom_model="meta-llama/llama-4-scout-17b-16e-instruct",
            custom_temperature=0.7,
            custom_max_tokens=8192
        )
        
        # Chain for direct answers to general questions
        self.general_answer_chain = self._create_chain(
            {"question": lambda x: x["question"], "context": lambda x: x.get("context", "")},
            general_answer_prompt,
            streaming=True,
            parser=None,
            model_type="summary",
            use_env_key=True,
            custom_model="llama-3.3-70b-versatile",
            custom_temperature=0.7,
            custom_max_tokens=4096
        )

        self.entity_chain = self._create_chain(
            {"question": lambda x: x["question"], "schema": lambda _: self.neo4j_schema_text}, 
            entity_prompt, 
            streaming=True, 
            parser=None,
            model_type="entity",
            use_env_key=True,
            custom_model="qwen2.5-32b-instruct",
            custom_temperature=0.1,
            custom_max_tokens=6000
        )
        self.query_plan_chain = self._create_chain(
            {"question": lambda x: x["question"], "entities": lambda x: x["entities"], "schema": lambda _: self.neo4j_schema_text}, 
            query_plan_prompt, 
            streaming=True, 
            parser=None,
            model_type="entity",
            use_env_key=True,
            custom_model="qwen2.5-32b-instruct",
            custom_temperature=0.1,
            custom_max_tokens=8192
        )
        self.query_chain = self._create_chain(
            {"query_plan": lambda x: x["query_plan"], "schema": lambda _: self.neo4j_schema_text}, 
            query_prompt, 
            streaming=True, 
            parser=None,
            model_type="query_generation",
            use_env_key=True,
            custom_model="qwen2.5-72b-instruct",
            custom_temperature=0.1,
            custom_max_tokens=4096
        )
        self.summary_chain = self._create_chain(
            {"query_results": lambda x: x["query_results"], "question": lambda x: x["question"]}, 
            summary_prompt, 
            streaming=True, 
            parser=None,
            model_type="summary",
            use_env_key=True,
            custom_model="meta-llama/llama-4-scout-17b-16e-instruct",
            custom_temperature=0.7,
            custom_max_tokens=4096
        )
        # Commented out api_reasoning_chain as the prompt is not available
        # self.api_reasoning_chain = self._create_chain(
        #     {"api_data": lambda x: x["api_data"], "question": lambda x: x["question"]},
        #     api_reasoning_prompt,
        #     streaming=True,
        #     parser=None,
        #     model_type="summary",
        #     use_env_key=True,
        #     custom_model="meta-llama/llama-4-scout-17b-16e-instruct",
        #     custom_temperature=0.7,
        #     custom_max_tokens=4096
        # )

    def _create_chain(self, assignment_funcs: dict, chain_prompt, streaming: bool, parser: Optional[PydanticOutputParser] = None, 
                     model_type: str = "query", use_env_key: bool = False,
                     custom_model: str = None, custom_temperature: float = None, custom_max_tokens: int = None) -> RunnableSequence:
        if self.llm_service.provider == "ollama":
            streaming = False
            
        # Create a temporary LLM service with environment API key if needed
        if use_env_key:
            # Select the appropriate API key based on model_type
            provider = "groq"  # Default provider
            api_key = None
            
            if model_type == "entity":
                # Use Qwen API for entity extraction and query planning
                api_key = self.env_qwen_api_key
                provider = "qwen" if api_key else "groq"
            elif model_type == "query_generation":
                # Use Qwen API for query generation
                api_key = self.env_qwen_api_key_generation
                provider = "qwen" if api_key else "groq"
            elif model_type == "query":
                # Use GROQ for other query operations
                api_key = self.env_groq_api_key_generation
            else:  # summary and other types
                api_key = self.env_groq_api_key
                
            if api_key:
                temp_llm_service = MultiLLMService(
                    provider=provider,
                    api_key=api_key,
                    query_generator_model_name=custom_model or self.llm_service.default_query_model,
                    query_summarizer_model=custom_model or self.llm_service.default_summary_model
                )
                llm = temp_llm_service.get_langchain_llm(
                    streaming=streaming,
                    temperature=custom_temperature if custom_temperature is not None else 0.2,
                    max_tokens=custom_max_tokens if custom_max_tokens is not None else 2048
                )
            else:
                llm = self.llm_service.get_langchain_llm(streaming=streaming)
        else:
            llm = self.llm_service.get_langchain_llm(streaming=streaming)
            
        chain = RunnablePassthrough.assign(**assignment_funcs) | chain_prompt | llm
        
        if model_type == "summary":
            self.llm_service.default_query_model = self.llm_service.default_summary_model
            
        if parser:
            chain = chain | parser
        else:
            chain = chain | StrOutputParser()
            
        return chain
    
    async def _handle_spectrum_comparison(self, spectra_result: Dict[str, Any]) -> AsyncGenerator[str, None]:
        """
        Handle spectrum comparison queries with multiple metabolites
        
        Args:
            spectra_result: Comparison result with multiple spectra data
            
        Yields:
            Formatted SSE messages for comparison analysis
        """
        from backend.services.spectra_service import SpectraProcessor
        
        yield self._format_message("Thinking", "Processing spectrum comparison data...")
        
        comparison_type = spectra_result.get("comparison_type", "compare")
        hmdb_ids = spectra_result.get("hmdb_ids", [])
        spectra_data_list = spectra_result.get("spectra_data", [])
        resolution_info = spectra_result.get("resolution_info", [])
        
        if not spectra_data_list or len(spectra_data_list) < 2:
            yield self._format_message("Answer", "❌ Insufficient spectrum data for comparison. At least 2 metabolites are required.")
            yield self._format_message("DONE", "")
            return
        
        # Process each metabolite's spectrum data
        all_processed_spectra = []
        metabolite_summaries = {}
        
        for metabolite_data in spectra_data_list:
            hmdb_id = metabolite_data.get("hmdb_id")
            raw_spectra_data = metabolite_data.get("spectra_data", {})
            
            if not hmdb_id or not raw_spectra_data:
                continue
            
            metabolite_summaries[hmdb_id] = {
                "hmdb_id": hmdb_id,
                "spectrum_types": [],
                "total_spectra": 0,
                "processed_spectra": []
            }
            
            # Process each spectrum type for this metabolite
            for spectrum_type in ['c_ms', 'ms_ms', 'nmr', 'ms_ir']:
                if spectrum_type in raw_spectra_data and raw_spectra_data[spectrum_type]:
                    for spectrum_raw in raw_spectra_data[spectrum_type]:
                        try:
                            processed = SpectraProcessor.process_spectrum_data(
                                hmdb_id=hmdb_id,
                                raw_data=spectrum_raw,
                                spectrum_type_key=spectrum_type
                            )
                            if processed:
                                all_processed_spectra.append(processed)
                                metabolite_summaries[hmdb_id]["processed_spectra"].append(processed)
                                if spectrum_type not in metabolite_summaries[hmdb_id]["spectrum_types"]:
                                    metabolite_summaries[hmdb_id]["spectrum_types"].append(spectrum_type)
                                metabolite_summaries[hmdb_id]["total_spectra"] += 1
                        except Exception as e:
                            print(f"[ERROR] Failed to process {spectrum_type} spectrum for {hmdb_id}: {str(e)}")
        
        if not all_processed_spectra:
            yield self._format_message("Answer", "❌ No valid spectrum data could be processed for comparison.")
            yield self._format_message("DONE", "")
            return
        
        # Format for LLM comparison analysis
        try:
            comparison_data = SpectraProcessor.format_multiple_spectra_for_llm_comparison(
                all_processed_spectra, comparison_type
            )
        except Exception as e:
            print(f"[ERROR] Failed to format comparison data: {str(e)}")
            yield self._format_message("Answer", f"❌ Error formatting comparison data: {str(e)}")
            yield self._format_message("DONE", "")
            return
        
        # Build comparison response
        response = f"# 🔬 Spectrum Comparison Analysis\n\n"
        response += f"**Comparison Type:** {comparison_type.title()}\n"
        response += f"**Metabolites:** {len(metabolite_summaries)}\n"
        response += f"**Total Spectra:** {len(all_processed_spectra)}\n\n"
        
        # Add resolution information if available
        if resolution_info:
            response += "## 🎯 Metabolite Resolution\n\n"
            for info in resolution_info:
                response += f"- **{info['query_name']}** → **{info['resolved_name']}** ({info['hmdb_id']}) "
                response += f"*[Confidence: {info['confidence']:.2f}]*\n"
            response += "\n"
        
        # Add individual metabolite summaries
        response += "## 📊 Individual Metabolite Spectra\n\n"
        for hmdb_id, summary in metabolite_summaries.items():
            response += f"### {hmdb_id}\n"
            response += f"- **Spectrum Types Available:** {', '.join(summary['spectrum_types'])}\n"
            response += f"- **Total Spectra:** {summary['total_spectra']}\n"
            
            # Add spectrum URLs if available
            urls_found = []
            for processed_spectrum in summary["processed_spectra"]:
                if processed_spectrum.metadata.spectrum_url:
                    spectrum_type = processed_spectrum.metadata.spectrum_type.value
                    urls_found.append(f"[{spectrum_type}]({processed_spectrum.metadata.spectrum_url})")
            
            if urls_found:
                response += f"- **Interactive Spectra:** {', '.join(urls_found)}\n"
            
            response += "\n"
        
        # Add comparison insights
        insights = comparison_data.get("comparison_insights", [])
        if insights:
            response += "## 🔍 Comparison Insights\n\n"
            for insight in insights:
                response += f"- {insight}\n"
            response += "\n"
        
        # Add analysis suggestions
        suggestions = comparison_data.get("analysis_suggestions", [])
        if suggestions:
            response += "## 💡 Analysis Suggestions\n\n"
            for suggestion in suggestions:
                response += f"- {suggestion}\n"
            response += "\n"
        
        # Generate LLM analysis using the comparison data
        yield self._format_message("Thinking", "Generating detailed comparison analysis...")
        
        try:
            comparison_prompt = f"""You are analyzing spectrum comparison data for multiple metabolites.

Comparison Type: {comparison_type}
Number of Metabolites: {len(metabolite_summaries)}

Detailed Comparison Data:
{json.dumps(comparison_data, indent=2, default=str)}

Please provide:
1. **Key Differences**: Highlight the most important spectral differences between the metabolites
2. **Chemical Insights**: What do these differences tell us about the molecular structures?
3. **Peak Analysis**: Compare significant peaks and their intensities
4. **Structural Implications**: How do the spectral patterns relate to molecular structure differences?

Focus on practical insights that would be valuable for metabolomics research."""
            
            from langchain_core.prompts import PromptTemplate
            
            llm_chain = self._create_chain(
                {"comparison_data": lambda x: x["comparison_data"]},
                PromptTemplate.from_template(comparison_prompt),
                model_type="query"
            )
            
            llm_analysis = ""
            async for chunk in llm_chain.astream({
                "comparison_data": json.dumps(comparison_data, indent=2, default=str)
            }):
                llm_analysis += chunk
            
            if llm_analysis.strip():
                response += "## 🧠 AI Analysis\n\n"
                response += llm_analysis
                response += "\n"
        
        except Exception as e:
            print(f"[ERROR] LLM analysis failed: {str(e)}")
            response += "## ⚠️ Analysis Note\n\n"
            response += "Detailed AI analysis could not be generated, but the comparison data above provides comprehensive insights.\n\n"
        
        # Add final summary
        response += "---\n\n"
        response += f"**Summary:** Successfully compared {len(metabolite_summaries)} metabolites across {len(all_processed_spectra)} spectra. "
        response += f"Use the interactive spectrum links above for detailed visualization.\n"
        
        yield self._format_message("Answer", response)
        yield self._format_message("DONE", "")

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

    async def _stream_and_accumulate(self, chain, section: str, inputs: Dict[str, Any], accumulator: List[str], neo4j_results: List[Dict] = None) -> AsyncGenerator[str, None]:
        """
        Stream a chain's output while accumulating the text chunks and optionally applying link processing.
        
        This method handles two purposes:
        1. Accumulates text chunks in the provided accumulator list
        2. Optionally processes "Answer" section text with hyperlinks when neo4j_results are provided
        
        The neo4j_results parameter should be passed whenever streaming content that will be shown
        directly to users and contains database identifiers that should be converted to hyperlinks.
        
        Args:
            chain: The LangChain chain to stream from
            section: The message section name
            inputs: The inputs to pass to the chain
            accumulator: A list to accumulate text chunks in
            neo4j_results: Optional Neo4j results to use for hyperlink injection
            
        Yields:
            Formatted SSE messages, with hyperlinks applied when appropriate
        """
        async for sse_message in self._process_stream(chain, section, inputs):
            try:
                message_json = sse_message[len("data:"):].strip()
                message = json.loads(message_json)
            except json.JSONDecodeError:
                continue
            
            text = message.get("text", "")
            
            # Only process valid content
            if message.get("section") != "Thinking" and text not in BAD_RESPONSES + ["DONE"]:
                # Accumulate the original text in the accumulator for downstream use
                accumulator.append(text)
                
                # If this is the Answer section and we have Neo4j results, process the text
                # to add hyperlinks before streaming to the user
                if message.get("section") == "Answer" and neo4j_results:
                    # Apply hyperlink processing to the text
                    processed_text = self._postprocess_text(text, neo4j_results)
                    
                    # Replace the text in the message with the processed version
                    processed_message = {
                        "section": message.get("section"),
                        "text": processed_text
                    }
                    
                    # Yield the processed message instead of the original
                    yield f"data:{json.dumps(processed_message)}\n\n"
                else:
                    # For non-Answer sections or when no Neo4j results, yield as-is
                    yield sse_message
            else:
                # Always yield control messages (DONE, Thinking)
                yield sse_message

    async def _match_entities(self, entity_name: str, entity_type: str) -> List[dict]:
        """
        Match entities using the optimized entity matcher with SynonymIndex support.
        
        Args:
            entity_name: Name of the entity to match
            entity_type: Type of entity (e.g., "Metabolite")
            
        Returns:
            List of matched entities with confidence scores
        """
        try:
            if entity_type.lower() == "metabolite":
                # Use the optimized entity matcher for metabolites
                results = await asyncio.to_thread(
                    self.entity_matcher.find_metabolite, 
                    entity_name
                )
                return results
            else:
                # For non-metabolite entities, fall back to basic Neo4j query
                query = f"""
                MATCH (n:{entity_type})
                WHERE toLower(n.name) = toLower($name)
                RETURN n.name as name, n.accession as accession
                LIMIT 5
                """
                results = self.neo4j_connection.run_query(query, parameters={"name": entity_name})
                return results
        except Exception as e:
            print(f"Error in entity matching for {entity_name}: {e}")
            return []

    def _merge_summaries(self, neo4j_summary: str, api_summary: str) -> str:
        # HMDB API integration temporarily disabled
        # """Merges Neo4j and API summaries into a unified response."""
        # if not neo4j_summary.strip():
        #     return api_summary
        # if not api_summary.strip():
        #     return neo4j_summary
        # 
        # return (
        #     f"{neo4j_summary.strip()}\n\n"
        #     f"---\n\n"
        #     f"**🔍 Additional Insights from HMDB API:**\n\n"
        #     f"{api_summary.strip()}"
        # )
        return neo4j_summary  # Return only Neo4j summary while HMDB API is disabled

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
                
                # Handle markdown code blocks
                if "```json" in cleaned_result or "```" in cleaned_result:
                    # Extract content between code blocks if present
                    import re
                    json_match = re.search(r'```(?:json)?\s*([\s\S]*?)```', cleaned_result)
                    if json_match:
                        cleaned_result = json_match.group(1).strip()
                    else:
                        # Remove prefix markdown indicators
                        for bad_prefix in BAD_RESPONSES:
                            if cleaned_result.startswith(bad_prefix):
                                cleaned_result = cleaned_result[len(bad_prefix):].strip()
                        # Remove suffix markdown indicators
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
                                  confidence_threshold: float = 0.6) -> Tuple[bool, Optional[Dict]]:
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

    def _clean_cypher_query(self, query_text: str) -> str:
        """
        Strip Markdown code block delimiters and other formatting from Cypher queries.
        
        Args:
            query_text: The raw query text that might contain Markdown formatting
            
        Returns:
            Clean Cypher query ready for execution
        """
        # Remove markdown code block start/end
        if "```" in query_text:
            # Extract content between code blocks if present
            import re
            cypher_match = re.search(r'```(?:cypher)?\s*([\s\S]*?)```', query_text)
            if cypher_match:
                query_text = cypher_match.group(1).strip()
        
        # Remove any remaining backticks
        query_text = query_text.replace('`', '')
        
        # Remove any leading/trailing whitespace and newlines
        query_text = query_text.strip()
        
        # Ensure query ends with semicolon
        if not query_text.endswith(';'):
            query_text += ';'
        
        return query_text

    def _postprocess_text(self, text: str, neo4j_results: List[Dict]) -> str:
        """
        Centralized text post-processing to apply consistent formatting and enrichment.
        
        Args:
            text: The raw text to process
            neo4j_results: The Neo4j results to use for link enrichment
            
        Returns:
            Processed text with links and other enhancements
        """
        if not text or not neo4j_results:
            return text
            
        # Apply hyperlink injection
        processed_text = inject_hyperlinks(text, neo4j_results)
        
        # Any additional text processing can be added here
        # For example:
        # - Formatting corrections
        # - Reference normalization
        # - Adding footers or disclaimers
        
        return processed_text

    async def _process_sub_intent(self, intent: Intent, user_question: str, 
                               conversation_history: List = None, 
                               relevant_history: List = None,
                               entity_extraction_results: Dict = None) -> Dict[str, Any]:
        """
        Process a single sub-intent and return its results.
        
        Args:
            intent: The Intent object to process
            user_question: The original user question
            conversation_history: List of recent conversation turns
            relevant_history: List of relevant memory entries
            entity_extraction_results: Optional pre-extracted entities to reuse
            
        Returns:
            Dict containing the processing results for this sub-intent
        """
        try:
            print(f"\n[DEBUG] Processing sub-intent: {intent.intent_type} - '{intent.intent_text}'")
            
            # Initialize result container
            intent_results = {
                "intent": intent,
                "text_accumulator": [],  # IMPORTANT: Raw text only, no hyperlinks yet
                "section": "Answer",
                "entities": [],
                "neo4j_results": None,  # Store results for later hyperlink injection
                "api_data": None,
                "query_plan": None,
                "error": None,
                "original_question": user_question  # Add original question for the aggregator
            }
            
            # IMPORTANT NOTE: This method should only accumulate raw text without any post-processing.
            # Hyperlink injection should ONLY happen after all text is aggregated in _combine_sub_intent_results
            # to prevent the LLM from potentially mangling markdown links during aggregation.
            
            # Use the sub-intent text as the specific question for this branch
            sub_question = intent.intent_text
            
            # 1) Entity Extraction (reuse if already done or do it for this sub-intent)
            full_extraction_response = None
            entities = None
            
            if entity_extraction_results:
                # Reuse entities from main question if they're already extracted
                full_extraction_response = entity_extraction_results.get("full_extraction_response")
                entities = entity_extraction_results.get("entities")
                print(f"\n[DEBUG] Reusing entity extraction results: {entities}")
            else:
                # Perform entity extraction for this sub-intent
                extraction_inputs = {"question": sub_question, "schema": self.neo4j_schema_text}
                extraction_accumulator: List[str] = []
                
                print(f"\n[DEBUG] Performing entity extraction for: {sub_question}")
                # Stream entity extraction (only for internal processing)
                try:
                    async for _ in self._stream_and_accumulate(
                        self.entity_chain, 
                        "Extracting entities", 
                        extraction_inputs, 
                        extraction_accumulator
                    ):
                        pass  # We don't yield these messages, just accumulate
                    
                    full_extraction_response = "".join(extraction_accumulator)
                    print(f"\n[DEBUG] Entity extraction successful: {full_extraction_response}")
                    entities = self.entity_parser.parse(full_extraction_response)
                except Exception as extraction_error:
                    print(f"\n[ERROR] Entity extraction failed: {extraction_error}")
                    return {
                        "intent": intent,
                        "text_accumulator": [f"Error during entity extraction: {str(extraction_error)}"],
                        "section": "Error",
                        "error": str(extraction_error)
                    }
            
            # Store entity results
            intent_results["entities"] = entities
            intent_results["full_extraction_response"] = full_extraction_response
            
            # Extract metabolites for potential API lookup
            metabolites = [ent.name for ent in entities.entities if ent.type == "Metabolite"]
            first_metabolite = metabolites[0] if metabolites else None
            print(f"\n[DEBUG] Extracted metabolites: {metabolites}")
            
            # NEW: Check if this is a pathway-related query
            is_pathway_query = False
            pathway_keywords = [
            "pathway", "pathways", "metabolic pathway", "biochemical pathway", "biosynthetic pathway",
            "catabolic pathway", "degradation pathway", "signal transduction pathway",
            "involved in", "participates in", "part of pathway", "metabolic process", 
            "biological process", "reaction network", "reaction map", "enzymatic pathway",
            "KEGG pathway", "SMPDB pathway", "WikiPathways", "pathway map", 
            "visualize pathway", "show pathway", "diagram of", "interaction pathway", 
            "regulatory pathway", "pathway diagram", "what pathway", "belongs to pathway",
            "linked to pathway", "pathway name", "HMDB pathway"
            ]

            # Check if query text contains pathway-related terms
            if any(keyword in sub_question.lower() for keyword in pathway_keywords):
                is_pathway_query = True
                print(f"\n[DEBUG] Detected pathway-related query: {sub_question}")
                
                # Try to retrieve pathway information if we have a metabolite
                if first_metabolite:
                    print(f"\n[DEBUG] Looking up pathways for metabolite: {first_metabolite}")
                    
                    # Get pathway data using our dedicated method
                    pathways = self._get_metabolite_pathways(first_metabolite)
                    
                    if pathways:
                        # Format pathway information for response
                        pathway_text = self._format_pathways_for_response(pathways)
                        print(f"\n[DEBUG] Found pathway information: {len(pathways)} pathways")
                        
                        # Add pathway data to result
                        intent_results["pathways"] = pathways
                        intent_results["text_accumulator"].append(pathway_text)
                        
                        # Skip database query since we've already found what we need
                        return intent_results
                    else:
                        print(f"\n[DEBUG] No pathways found for metabolite: {first_metabolite}")
                        # Continue with normal query flow if no pathways found
            
            # 2) Query Planning
            planning_inputs = {
                "question": sub_question, 
                "entities": full_extraction_response, 
                "schema": self.neo4j_schema_text
            }
            planning_accumulator: List[str] = []
            
            print(f"\n[DEBUG] Starting query planning for: {sub_question}")
            # Stream query planning (only for internal processing)
            try:
                async for _ in self._stream_and_accumulate(
                    self.query_plan_chain, 
                    "Query planning", 
                    planning_inputs, 
                    planning_accumulator
                ):
                    pass  # We don't yield these messages, just accumulate
                
                full_query_plan_response = "".join(planning_accumulator)
                print(f"\n[DEBUG] Query plan response: {full_query_plan_response}")
                query_plan = self.query_plan_parser.parse(full_query_plan_response)
                print(f"\n[DEBUG] Parsed query plan: should_query={query_plan.should_query}, intent={query_plan.query_intent}")
            except Exception as planning_error:
                print(f"\n[ERROR] Query planning failed: {planning_error}")
                return {
                    "intent": intent,
                    "text_accumulator": [f"Error during query planning: {str(planning_error)}"],
                    "section": "Error",
                    "error": str(planning_error)
                }
            
            # Store query plan
            intent_results["query_plan"] = query_plan
            
            # 3) Execute query if needed
            if query_plan.should_query:
                print(f"\n[DEBUG] Query execution required: {query_plan.reasoning}")
                # Check memory for reusable data first
                memory_raw_data = None
                used_memory_data = False
                
                if relevant_history:
                    # Check if any relevant memory entry has reusable raw_data
                    has_reusable_data, memory_raw_data = self._find_reusable_memory_data(
                        relevant_history, 
                        query_plan.query_intent
                    )
                    used_memory_data = has_reusable_data and memory_raw_data
                    
                    if used_memory_data:
                        print(f"\n[DEBUG] Using memory data for query: {memory_raw_data.keys() if memory_raw_data else None}")
                
                # Skip query execution if we have memory data
                neo4j_results = None
                
                if not used_memory_data:
                    # Generate and execute query
                    query_inputs = {"query_plan": query_plan, "schema": self.neo4j_schema_text}
                    query_accumulator: List[str] = []
                    
                    # Stream query generation (only for internal processing)
                    try:
                        print(f"\n[DEBUG] Generating Neo4j query for plan: {query_plan}")
                        async for _ in self._stream_and_accumulate(
                            self.query_chain, 
                            "Query execution", 
                            query_inputs, 
                            query_accumulator
                        ):
                            pass  # We don't yield these messages, just accumulate
                            
                        full_query_response = "".join(query_accumulator)
                        print(f"\n[DEBUG] Generated query: {full_query_response}")
                        
                        # Clean the query to remove markdown formatting
                        clean_query = self._clean_cypher_query(full_query_response)
                        print(f"\n[DEBUG] Cleaned query for execution: {clean_query}")
                    except Exception as query_gen_error:
                        print(f"\n[ERROR] Query generation failed: {query_gen_error}")
                        return {
                            "intent": intent,
                            "text_accumulator": [f"Error generating Neo4j query: {str(query_gen_error)}"],
                            "section": "Error",
                            "error": str(query_gen_error)
                        }
                    
                    try:
                        # Execute Neo4j query with cleaned query
                        print(f"\n[DEBUG] Executing Neo4j query...")
                        # Clean the query to remove markdown formatting before execution
                        clean_query = self._clean_cypher_query(full_query_response)
                        print(f"\n[DEBUG] Cleaned query for execution: {clean_query}")
                        neo4j_results = self.neo4j_connection.run_query(clean_query)
                        print(f"\n[DEBUG] Neo4j raw query results: {neo4j_results}")
                        
                        intent_results["neo4j_results"] = neo4j_results
                        
                        # Check if we need fallback to API
                        should_fallback = analyze_missing_fields(sub_question, neo4j_results)
                        if not neo4j_results or neo4j_results == [None] or neo4j_results == [""]:
                            should_fallback = True
                            
                        print(f"\n[DEBUG] Need API fallback? {should_fallback}")
                        
                        # Get additional description results if needed
                        for metabolite in metabolites:
                            more_results = self.neo4j_connection.run_query(f"""
                                MATCH (m:Metabolite)
                                WHERE toLower(m.name) = toLower('{metabolite}')
                                OR EXISTS {{ MATCH (m)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex) 
                                            WHERE any(syn IN si.synonyms WHERE toLower(syn) = toLower('{metabolite}')) }}
                                RETURN m.description
                            """)
                            if more_results:
                                print(f"\n[DEBUG] Additional description results: {more_results}")
                                neo4j_results += more_results
                                intent_results["neo4j_results"] = neo4j_results
                        
                        # Handle fallback to HMDB API if needed
                        if should_fallback and len(metabolites) > 0:
                            # HMDB API integration temporarily disabled
                            # if self.hmdb_client:
                            #     first_metabolite = metabolites[0]
                            #     fallback_data = None
                            #     
                            #     # Make API call based on ID or name
                            #     try:
                            #         if first_metabolite.startswith("HMDB"):
                            #             print(f"\n[DEBUG] Making HMDB API call for ID: {first_metabolite}")
                            #             payload = {"hmdb_id": [first_metabolite]}
                            #             fallback_data = self.hmdb_client.post("metabolites", payload)
                            #         else:
                            #             print(f"\n[DEBUG] Making HMDB API search for name: {first_metabolite}")
                            #             payload = {"name": first_metabolite}
                            #             fallback_data = self.hmdb_client.post("metabolites/search", payload)
                            #         
                            #         print(f"\n[DEBUG] HMDB API response: {fallback_data.keys() if fallback_data else None}")
                            #     except Exception as api_error:
                            #         print(f"\n[ERROR] HMDB API call failed: {api_error}")
                            #     
                            #     # Process API response
                            #     if fallback_data and "found" in fallback_data:
                            #         filtered_fallback_data = self._filter_hmdb_response(fallback_data)
                            #         intent_results["api_data"] = filtered_fallback_data
                            #         
                            #         # Run API reasoning chain
                            #         try:
                            #             print(f"\n[DEBUG] Running API reasoning chain...")
                            #             api_reasoning_inputs = {
                            #                 "api_data": filtered_fallback_data,
                            #                 "question": sub_question
                            #             }
                            #             api_reasoning_accumulator: List[str] = []
                            #             
                            #             # Stream API reasoning (only for internal processing)
                            #             async for _ in self._stream_and_accumulate(
                            #                 self.api_reasoning_chain,
                            #                 "API Summary",
                            #                 api_reasoning_inputs,
                            #                 api_reasoning_accumulator
                            #             ):
                            #                 pass  # We don't yield these messages, just accumulate
                            #             
                            #             # Store API reasoning results
                            #             api_summary = "".join(api_reasoning_accumulator)
                            #             intent_results["api_summary"] = api_summary
                            #             print(f"\n[DEBUG] API summary generated successfully")
                            pass  # Skip HMDB API integration for now
                        
                        # If no fallback needed or fallback failed, use Neo4j results
                        elif neo4j_results and neo4j_results != [None] and neo4j_results != [""]:
                            try:
                                print(f"\n[DEBUG] Generating summary from Neo4j results...")
                                summary_inputs = {
                                    "query_results": neo4j_results,
                                    "question": sub_question
                                }
                                summary_accumulator: List[str] = []
                                
                                # Stream DB summary (only for internal processing)
                                async for _ in self._stream_and_accumulate(
                                    self.summary_chain,
                                    "Answer",
                                    summary_inputs,
                                    summary_accumulator,
                                    neo4j_results=neo4j_results  # Pass the Neo4j results for hyperlink processing
                                ):
                                    pass  # We don't yield these messages, just accumulate
                                
                                # Store summary
                                summary = "".join(summary_accumulator)
                                intent_results["db_summary"] = summary
                                intent_results["text_accumulator"].append(summary)
                                print(f"\n[DEBUG] Neo4j summary generated successfully")
                            except Exception as summary_error:
                                print(f"\n[ERROR] Summary generation failed: {summary_error}")
                                intent_results["error"] = f"Summary generation failed: {summary_error}"
                                intent_results["text_accumulator"].append(f"Error generating summary: {summary_error}")
                        else:
                            intent_results["error"] = "No results found in database"
                            intent_results["text_accumulator"].append("I couldn't find the information you're looking for in our database.")
                            print(f"\n[DEBUG] No results found in database or API")
                    
                    except Exception as e:
                        print(f"\n[ERROR] Neo4j query execution failed: {e}")
                        intent_results["error"] = f"Neo4j query execution failed: {e}"
                        intent_results["text_accumulator"].append(f"I encountered an error while querying the database: {e}")
                
                # Use memory data if available
                elif used_memory_data:
                    # Process based on what raw data we have
                    if "neo4j_results" in memory_raw_data and memory_raw_data["neo4j_results"]:
                        # Use memory's Neo4j results
                        summary_inputs = {
                            "query_results": memory_raw_data["neo4j_results"],
                            "question": sub_question
                        }
                        summary_accumulator: List[str] = []
                        
                        # Stream DB summary (only for internal processing) 
                        async for _ in self._stream_and_accumulate(
                            self.summary_chain,
                            "DB Summary", 
                            summary_inputs,
                            summary_accumulator,
                            neo4j_results=memory_raw_data["neo4j_results"]  # Pass the Neo4j results for hyperlink processing
                        ):
                            pass  # We don't yield these messages, just accumulate
                        
                        # Store DB summary
                        db_summary = "".join(summary_accumulator)
                        intent_results["db_summary"] = db_summary
                        
                        # If we also have API data, use it too
                        api_summary = ""
                        if "api_data" in memory_raw_data and memory_raw_data["api_data"]:
                            api_reasoning_inputs = {
                                "api_data": memory_raw_data["api_data"],
                                "question": sub_question
                            }
                            api_reasoning_accumulator: List[str] = []
                            
                            # Stream API reasoning (only for internal processing)
                            async for _ in self._stream_and_accumulate(
                                self.api_reasoning_chain,
                                "API Summary",
                                api_reasoning_inputs,
                                api_reasoning_accumulator
                            ):
                                pass  # We don't yield these messages, just accumulate
                            
                            # Store API summary
                            api_summary = "".join(api_reasoning_accumulator)
                            intent_results["api_summary"] = api_summary
                        
                        # Merge both summaries if we have API data
                        if api_summary:
                            final_summary = self._merge_summaries(db_summary, api_summary)
                            
                            # Remove hyperlink injection at individual text piece level
                            # Let post-processing happen only after aggregation
                            intent_results["text_accumulator"].append(final_summary)
                        else:
                            # Remove hyperlink injection at individual text piece level
                            # Let post-processing happen only after aggregation
                            intent_results["text_accumulator"].append(db_summary)
                    elif "api_data" in memory_raw_data and memory_raw_data["api_data"]:
                        # Only have API data from memory
                        api_reasoning_inputs = {
                            "api_data": memory_raw_data["api_data"],
                            "question": sub_question
                        }
                        api_reasoning_accumulator: List[str] = []
                        
                        # Stream API reasoning (only for internal processing)
                        async for _ in self._stream_and_accumulate(
                            self.api_reasoning_chain,
                            "API Summary",
                            api_reasoning_inputs,
                            api_reasoning_accumulator
                        ):
                            pass  # We don't yield these messages, just accumulate
                        
                        # Store API summary
                        api_summary = "".join(api_reasoning_accumulator)
                        intent_results["api_summary"] = api_summary
                        intent_results["text_accumulator"].append(api_summary)
                    else:
                        # No usable data in memory after all
                        intent_results["error"] = "Memory data could not be used"
            else:
                print(f"\n[DEBUG] No database query needed: {query_plan.reasoning}")
                intent_results["text_accumulator"].append(f"No database query needed for this part. {query_plan.reasoning}")
            
            # Return the results for this sub-intent
            return intent_results
        
        except Exception as e:
            # Handle any errors in the sub-intent processing
            print(f"\n[ERROR] Error processing sub-intent '{intent.intent_type}': {str(e)}")
            import traceback
            traceback.print_exc()  # Print the full stack trace
            return {
                "intent": intent,
                "text_accumulator": [f"Error processing this part of your question: {str(e)}"],
                "section": "Error",
                "error": str(e)
            }

    async def _combine_sub_intent_results(self, results: List[Dict]) -> str:
        """
        Combine results from multiple sub-intents into a coherent response using the intelligent aggregator.
        
        Args:
            results: List of result dictionaries from processed sub-intents
            
        Returns:
            Combined text to present to the user
        """
        # IMPORTANT: This is where all text post-processing (like hyperlink injection) should happen.
        # We first let the LLM aggregate the raw text, then apply post-processing to the final result.
        # This ensures that markdown formatting doesn't get mangled during LLM processing.
        
        if not results:
            return "I couldn't process your question. Please try again."
        
        # If only one result, just return its text
        if len(results) == 1:
            combined_text = "".join(results[0].get("text_accumulator", []))
            
            # Apply post-processing if neo4j_results are available
            neo4j_results = results[0].get("neo4j_results")
            if neo4j_results:
                return self._postprocess_text(combined_text, neo4j_results)
            return combined_text
        
        # For multiple results, format them for the aggregator
        combined_parts = []
        formatted_results = []
        
        # Process each sub-intent result
        for idx, result in enumerate(results):
            intent = result.get("intent")
            text_accumulator = result.get("text_accumulator", [])
            error = result.get("error")
            
            # Get the text from the accumulator or an error message
            if text_accumulator and len(text_accumulator) > 0:
                text = "".join(text_accumulator)
            elif error:
                text = f"ERROR: {error}"
            else:
                text = "No information found for this part."
            
            # Create a formatted version with header for the aggregator input
            if intent:
                header = f"SUB-INTENT {idx+1}: {intent.intent_type} - {intent.intent_text}"
            else:
                header = f"SUB-INTENT {idx+1}"
            
            formatted_results.append(f"{header}\n{text}\n")
        
        # Combine all formatted results for the aggregator
        sub_intent_results_text = "\n".join(formatted_results)
        
        # Get the original question from the first result
        question = results[0].get("original_question", "")
        
        try:
            # Use the aggregator chain to create an intelligently combined response
            aggregator_inputs = {
                "question": question,
                "sub_intent_results": sub_intent_results_text
            }
            
            # Process the aggregation
            aggregated_response = ""
            async for chunk in self.aggregator_chain.astream(aggregator_inputs):
                if chunk:
                    aggregated_response += chunk if isinstance(chunk, str) else str(chunk)
            
            # Apply post-processing to the aggregated response
            # Collect all neo4j_results from all sub-intents
            all_neo4j_results = []
            for result in results:
                if result.get("neo4j_results"):
                    all_neo4j_results.extend(result.get("neo4j_results"))
                    
            # Apply post-processing if we have any neo4j_results
            if all_neo4j_results:
                return self._postprocess_text(aggregated_response, all_neo4j_results)
                
            # Return the aggregated response without post-processing if no neo4j_results
            return aggregated_response
            
        except Exception as e:
            print(f"[ERROR] Aggregator chain failed: {e}")
            
            # Fallback to the simpler combination method if aggregator fails
            for idx, result in enumerate(results):
                intent = result.get("intent")
                text_accumulator = result.get("text_accumulator", [])
                error = result.get("error")
                
                # Get the text from the accumulator or an error message
                if text_accumulator and len(text_accumulator) > 0:
                    text = "".join(text_accumulator)
                elif error:
                    text = f"I encountered an error while processing this part: {error}"
                else:
                    text = "No information found for this part."
                
                # Create a header for this sub-intent
                if intent:
                    header = f"\n\n## {intent.intent_type}: {intent.intent_text}"
                else:
                    header = f"\n\n## Part {idx + 1}"
                
                # Add this section
                combined_parts.append(f"{header}\n{text}")
            
            # Add a header for the combined response
            combined_parts.insert(0, "Here's the information you asked for:")
            
            # Combine all parts
            combined_text = "\n".join(combined_parts)
            
            # Apply post-processing to the fallback response
            # Collect all neo4j_results from all sub-intents
            all_neo4j_results = []
            for result in results:
                if result.get("neo4j_results"):
                    all_neo4j_results.extend(result.get("neo4j_results"))
                    
            # Apply post-processing if we have any neo4j_results
            if all_neo4j_results:
                return self._postprocess_text(combined_text, all_neo4j_results)
                
            # Return the combined text without post-processing if no neo4j_results
            return combined_text

    async def run_pipeline(self, user_question: str, conversation_history: List = None, relevant_history: List = None, spectrum_mode: bool = False) -> AsyncGenerator[str, None]:
        try:
            # IMPORTANT: Follow the pattern of separating text accumulation from post-processing.
            # 1. Accumulate raw text without hyperlink injection
            # 2. Perform any LLM aggregation on the raw text
            # 3. Only apply post-processing (like hyperlink injection) on the final text
            # This prevents markdown links from being mangled during LLM processing.
            
            # Initialize intent_results dictionary to avoid "name not defined" error
            intent_results = {
                "text_accumulator": [],
                "section": "Answer",
                "entities": [],
                "neo4j_results": None,
                "api_data": None,
                "query_plan": None,
                "error": None
            }
            
            # NEW: Check if this is a pathway-related query that can be handled directly
            pathway_keywords = [
                "pathway", "pathways", "metabolic pathway", "biochemical pathway", "biosynthetic pathway",
                "catabolic pathway", "degradation pathway", "signal transduction pathway",
                "involved in", "participates in", "part of pathway", "metabolic process", 
                "biological process", "reaction network", "reaction map", "enzymatic pathway",
                "KEGG pathway", "SMPDB pathway", "WikiPathways", "pathway map", 
                "visualize pathway", "show pathway", "diagram of", "interaction pathway", 
                "regulatory pathway", "pathway diagram", "what pathway", "belongs to pathway",
                "linked to pathway", "pathway name", "HMDB pathway"
            ]

            # Check if the query is pathway-related
            is_pathway_query = any(keyword in user_question.lower() for keyword in pathway_keywords)
            
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
            
            # PHASE 4: Check for spectra-related queries and handle them early
            # When spectrum_mode is True, force spectrum detection for any query
            if spectrum_mode:
                print(f"\n[DEBUG] Spectrum Mode enabled - forcing spectrum analysis")
                yield self._format_message("Thinking", "🔬 Spectrum Mode enabled - analyzing query for spectrum data...")
                spectra_result = self.spectra_integrator.process_spectra_query(f"spectrum of {user_question}")
            else:
                spectra_result = self.spectra_integrator.process_spectra_query(user_question)
            
            if spectra_result.get("is_spectra_query", False):
                print(f"\n[DEBUG] Spectra query detected!")
                print(f"[DEBUG] Spectra result structure: {json.dumps(spectra_result, indent=2, default=str)}")
                yield self._format_message("Thinking", "Detected a spectrum-related query. Processing spectral data...")
                
                # Process the spectra request - use spectra_result directly, not a nested "result"
                print(f"[DEBUG] Processing spectra_result directly")
                
                if spectra_result.get("success", False):
                    # Check if this is a comparison query
                    if spectra_result.get("is_comparison", False):
                        async for message in self._handle_spectrum_comparison(spectra_result):
                            yield message
                        return
                    
                    # Handle single spectrum query
                    # We have successful spectra data - extract from nested spectra_data structure
                    spectra_data_container = spectra_result.get("spectra_data", {})
                    all_spectra = spectra_data_container.get("data", [])
                    metadata = spectra_data_container.get("metadata", {})
                    
                    print(f"[DEBUG] Number of spectra available: {len(all_spectra)}")
                    print(f"[DEBUG] Spectrum types found: {metadata.get('spectrum_types', [])}")
                    
                    if all_spectra:
                        # Save ALL spectrum data to debug output folder
                        try:
                            import os
                            debug_dir = "debug_output"
                            os.makedirs(debug_dir, exist_ok=True)
                            
                            hmdb_id = metadata.get("hmdb_id", "unknown")
                            all_spectra_file = os.path.join(debug_dir, f"all_spectra_data_{hmdb_id}.json")
                            with open(all_spectra_file, 'w') as f:
                                json.dump({
                                    "hmdb_id": hmdb_id,
                                    "metadata": metadata,
                                    "all_spectra": all_spectra,
                                    "full_result": spectra_result
                                }, f, indent=2, default=str)
                            print(f"[DEBUG] Saved all spectra data to: {all_spectra_file}")
                        except Exception as save_error:
                            print(f"[DEBUG] Failed to save spectra data: {save_error}")
                        
                        # Start building the complete response
                        hmdb_id = metadata.get("hmdb_id", "Unknown")
                        total_spectra = len(all_spectra)
                        spectrum_types = metadata.get("spectrum_types", [])
                        
                        response = f"## Complete Spectrum Analysis for {hmdb_id}\n\n"
                        response += f"**Found {total_spectra} spectra** across {len(spectrum_types)} spectrum types:\n"
                        for spec_type in spectrum_types:
                            response += f"- {spec_type}\n"
                        response += "\n---\n\n"
                        
                        # Process each spectrum
                        processed_count = 0
                        visualization_files = []
                        
                        for spectrum_index, spectrum_result in enumerate(all_spectra):
                            if not spectrum_result.get("success", False):
                                continue
                                
                            try:
                                spectrum_data = spectrum_result.get("data", {})
                                spectrum_metadata = spectrum_result.get("metadata", {})
                                spectrum_category = spectrum_metadata.get("spectrum_category", "unknown")
                                spectrum_idx = spectrum_metadata.get("spectrum_index", spectrum_index + 1)
                                
                                print(f"[DEBUG] Processing spectrum {spectrum_idx} of type {spectrum_category}...")
                                
                                # Process the spectrum data
                                processed_spectrum = SpectraProcessor.process_raw_spectrum(hmdb_id, spectrum_data)
                                
                                if processed_spectrum:
                                    processed_count += 1
                                    
                                    # Add spectrum info to response
                                    response += f"### Spectrum {spectrum_idx} - {spectrum_category.upper()}\n"
                                    response += f"**Type:** {processed_spectrum.metadata.spectrum_type.value}\n"
                                    response += f"**Instrument:** {processed_spectrum.metadata.instrument_type.value}\n"
                                    response += f"**Quality Score:** {processed_spectrum.quality_score:.2f}\n"
                                    response += f"**Number of Peaks:** {len(processed_spectrum.peaks)}\n"
                                    
                                    # Check for spectrum URL first - prioritize direct links over visualization
                                    spectrum_url = processed_spectrum.metadata.spectrum_url
                                    if spectrum_url:
                                        response += f"🔗 **[View Interactive Spectrum on HMDB]({spectrum_url})**\n\n"
                                        response += "📊 *This links directly to HMDB's professional spectrum visualization*\n"
                                        print(f"[DEBUG] Found spectrum URL for {hmdb_id}: {spectrum_url}")
                                    else:
                                        response += "📊 *Spectrum URL not available - using processed data*\n"
                                    
                                    # Add experimental conditions if available
                                    if processed_spectrum.metadata.solvent:
                                        response += f"**Solvent:** {processed_spectrum.metadata.solvent}\n"
                                    if processed_spectrum.metadata.sample_concentration:
                                        response += f"**Concentration:** {processed_spectrum.metadata.sample_concentration}\n"
                                    
                                    # Add interpretation
                                    try:
                                        llm_data = SpectraProcessor.format_for_llm_reasoning(processed_spectrum)
                                        if llm_data.get("interpretation_hints"):
                                            response += "\n**Key Features:**\n"
                                            for hint in llm_data.get("interpretation_hints", []):
                                                response += f"- {hint}\n"
                                        
                                        if llm_data.get("key_peaks"):
                                            response += "\n**Top Peaks:**\n"
                                            for peak in llm_data.get("key_peaks", [])[:3]:  # Show top 3 peaks per spectrum
                                                response += f"- m/z {peak['mz']:.2f} (intensity: {peak['intensity']:.3f})\n"
                                    except Exception as llm_error:
                                        print(f"[ERROR] LLM formatting failed for spectrum {spectrum_idx}: {llm_error}")
                                    
                                    # Generate visualization only if spectrum URL is not available
                                    if not spectrum_url:
                                        try:
                                            visualization_result = self.spectra_visualization.create_spectrum_visualization(
                                                processed_spectrum,
                                                output_format=PlotFormat.INTERACTIVE_HTML,
                                                style=PlotStyle.SCIENTIFIC
                                            )
                                            
                                            if visualization_result.get("success", False):
                                                # Save visualization file
                                                viz_filename = f"spectrum_plot_{hmdb_id}_{spectrum_category}_{spectrum_idx}.html"
                                                viz_file = os.path.join(debug_dir, viz_filename)
                                                
                                                graph_data = visualization_result.get("graph_data", {})
                                                if graph_data.get("content"):
                                                    with open(viz_file, 'w') as f:
                                                        f.write(graph_data["content"])
                                                    visualization_files.append(viz_filename)
                                                    print(f"[DEBUG] Saved spectrum plot to: {viz_file}")
                                                    response += f"📊 **Local Visualization:** `{viz_filename}`\n"
                                            
                                        except Exception as viz_error:
                                            print(f"[ERROR] Visualization failed for spectrum {spectrum_idx}: {viz_error}")
                                            response += f"⚠️ **Visualization failed:** {viz_error}\n"
                                    else:
                                        print(f"[DEBUG] Skipping local visualization - using HMDB spectrum URL instead")
                                    
                                    response += "\n---\n\n"
                                
                                else:
                                    print(f"[ERROR] Failed to process spectrum {spectrum_idx} of type {spectrum_category}")
                                    
                            except Exception as process_error:
                                print(f"[ERROR] Error processing spectrum {spectrum_index + 1}: {process_error}")
                                continue
                        
                        # Add summary
                        response += f"## Summary\n"
                        response += f"- **Total spectra found:** {total_spectra}\n"
                        response += f"- **Successfully processed:** {processed_count}\n"
                        response += f"- **Visualization files created:** {len(visualization_files)}\n\n"
                        
                        if visualization_files:
                            response += "**Interactive plots saved to debug_output folder:**\n"
                            for viz_file in visualization_files:
                                response += f"- `{viz_file}`\n"
                        
                        # Send the complete response
                        yield self._format_message("Answer", response)
                        yield self._format_message("DONE", "")
                        return
                    
                    else:
                        print(f"[ERROR] No spectrum data available in result")
                        hmdb_id = metadata.get("hmdb_id", "Unknown")
                        yield self._format_message("Answer", f"No spectrum data available for {hmdb_id}. The compound may not have spectra in the HMDB database.")
                        yield self._format_message("DONE", "")
                        return
                
                elif spectra_result.get("disambiguation_required", False):
                    # Need user to disambiguate
                    prompt = spectra_result.get("disambiguation_prompt", "Please provide more specific information.")
                    yield self._format_message("Answer", prompt)
                    yield self._format_message("DONE", "")
                    return
                
                else:
                    # Error in spectra processing
                    error_msg = spectra_result.get("error", "Unknown error in spectrum processing")
                    error_type = spectra_result.get("error_type")
                    
                    print(f"[ERROR] Spectra processing error: {error_msg}")
                    print(f"[ERROR] Error type: {error_type}")
                    print(f"[ERROR] Full result structure: {json.dumps(spectra_result, indent=2, default=str)}")
                    
                    # For timeout/connection errors, show user-friendly message without "Sorry, I encountered an issue"
                    if error_type in ["timeout", "connection", "request"]:
                        yield self._format_message("Answer", error_msg)
                    else:
                        # For other errors, keep the apologetic tone
                        yield self._format_message("Answer", f"Sorry, I encountered an issue while processing your spectrum request: {error_msg}")
                    
                    yield self._format_message("DONE", "")
                    return
            
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
                
                # Check if context might contain database information that needs hyperlinks
                potential_neo4j_results = []
                if relevant_history:
                    for turn in relevant_history[:3]:  # Look at recent turns for DB results
                        if "raw_data" in turn and "neo4j_results" in turn["raw_data"]:
                            potential_neo4j_results.extend(turn["raw_data"]["neo4j_results"])
                
                async for chunk in self.general_answer_chain.astream(general_inputs):
                    if isinstance(chunk, str):
                        answer_accumulator.append(chunk)
                        # Apply hyperlink processing if we have potential data
                        if potential_neo4j_results:
                            processed_chunk = self._postprocess_text(chunk, potential_neo4j_results)
                            yield self._format_message("Answer", processed_chunk)
                        else:
                            yield self._format_message("Answer", chunk)
                        
                # Signal completion        
                yield self._format_message("DONE", "")
                return
            
            # NEW: Early handling of pathway-related queries if a clear metabolite is found
            # Only execute this path for single-intent queries to avoid complexity
            if is_pathway_query and not has_multiple_intents:
                # First extract entities to find metabolites
                extraction_inputs = {"question": user_question, "schema": self.neo4j_schema_text}
                extraction_accumulator: List[str] = []
                
                # Extract entities from question
                yield self._format_message("Thinking", "Looking for metabolites mentioned in your pathway question...")
                async for sse_message in self._stream_and_accumulate(
                    self.entity_chain, 
                    "Extracting entities", 
                    extraction_inputs, 
                    extraction_accumulator
                ):
                    yield sse_message
                
                # Process extraction results
                full_extraction_response = "".join(extraction_accumulator)
                entities = self.entity_parser.parse(full_extraction_response)
                metabolites = [ent.name for ent in entities.entities if ent.type == "Metabolite"]
                
                # If we found a metabolite, directly check for pathways
                if metabolites:
                    from backend.services.pathway_service import get_pathways_for_metabolite, get_pathways_by_metabolite_name
                    
                    # Use the first metabolite found
                    first_metabolite = metabolites[0]
                    yield self._format_message("Thinking", f"Looking up pathway information for {first_metabolite}...")
                    
                    # Get pathway data using the appropriate method
                    pathways = []
                    if first_metabolite.startswith("HMDB"):
                        pathways = get_pathways_for_metabolite(self.neo4j_connection, first_metabolite)
                    else:
                        pathways = get_pathways_by_metabolite_name(self.neo4j_connection, first_metabolite)
                    
                    # If pathways were found, format and return them
                    if pathways:
                        # Format the pathway information as a response
                        response = f"I found the following pathway information for {first_metabolite}:\n\n"
                        
                        for idx, pathway in enumerate(pathways, 1):
                            response += f"{idx}. {pathway['name']}\n"
                            
                            if "smpdb_url" in pathway:
                                response += f"   SMPDB: {pathway['smpdb_url']}\n"
                                
                            if "kegg_url" in pathway:
                                response += f"   KEGG: {pathway['kegg_url']}\n"
                                
                            response += "\n"
                        
                        # Send the response
                        yield self._format_message("Answer", response)
                        yield self._format_message("DONE", "")
                        return
                    
                    # If no pathways were found, inform the user
                    yield self._format_message("Thinking", f"No pathway information found for {first_metabolite}. Proceeding with standard query...")
            
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
            
            # PHASE 2: Handle multiple intents by processing them in parallel
            if has_multiple_intents:
                # First, perform entity extraction once for all intents (to avoid duplication)
                # This is a performance optimization as entity extraction is often similar
                # for related sub-intents
                yield self._format_message("Thinking", "Analyzing your multi-part question...")
                
                # 1) Entity Extraction (performed once for all intents)
                extraction_inputs = {"question": user_question, "schema": self.neo4j_schema_text}
                extraction_accumulator: List[str] = []
                async for sse_message in self._stream_and_accumulate(
                    self.entity_chain, 
                    "Extracting entities", 
                    extraction_inputs, 
                    extraction_accumulator
                ):
                    yield sse_message
                    
                full_extraction_response = "".join(extraction_accumulator)
                print(f"\n[DEBUG] Entity Extraction Response: {full_extraction_response}")
                entities = self.entity_parser.parse(full_extraction_response)
                
                # Store entity extraction results to reuse
                entity_extraction_results = {
                    "full_extraction_response": full_extraction_response,
                    "entities": entities
                }
                
                # Inform the user that we're processing multiple parts
                yield self._format_message(
                    "Thinking", 
                    f"Processing {len(intents)} parts of your question in parallel..."
                )
                
                # 2) Process each sub-intent in parallel
                try:
                    # Create tasks for parallel processing with error handling
                    tasks = [
                        self._process_sub_intent(
                            intent=intent,
                            user_question=user_question,
                            conversation_history=conversation_history,
                            relevant_history=relevant_history,
                            entity_extraction_results=entity_extraction_results
                        )
                        for intent in intents
                    ]
                    
                    # Execute all tasks in parallel with error handling
                    # Using gather with return_exceptions=True ensures that exceptions 
                    # won't break the whole gather operation
                    all_intent_results = await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Process results, handling any exceptions that were returned
                    processed_results = []
                    for i, result in enumerate(all_intent_results):
                        if isinstance(result, Exception):
                            # Handle exceptions returned by gather
                            error_msg = str(result)
                            print(f"\n[ERROR] Exception from sub-intent {i}: {error_msg}")
                            processed_results.append({
                                "intent": intents[i],
                                "text_accumulator": [f"Error processing this part: {error_msg}"],
                                "error": error_msg,
                                "section": "Error"
                            })
                        else:
                            # Add successful results
                            processed_results.append(result)
                    
                    # 3) Combine results from all sub-intents
                    combined_answer = await self._combine_sub_intent_results(processed_results)
                    
                    # 4) Return the combined answer
                    yield self._format_message("Answer", combined_answer)
                    
                    # Signal completion
                    yield self._format_message("DONE", "")
                    return
                    
                except Exception as e:
                    yield self._format_message(
                        "Error", 
                        f"Error processing multiple parts of your question: {e}"
                    )
                    # Fall back to the standard single-intent pipeline
                    yield self._format_message(
                        "Thinking", 
                        "Falling back to standard processing..."
                    )
                    # Continue with standard pipeline below, using the first intent
                    
            # Standard pipeline for single intent (or fallback if parallel processing failed)
            # 1) Entity Extraction (if not already done)
            extraction_inputs = {"question": user_question, "schema": self.neo4j_schema_text}
            extraction_accumulator: List[str] = []
            
            # Skip if already done in the multi-intent branch
            if not has_multiple_intents:
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
                        # Execute Neo4j query
                        print(f"\n[DEBUG] Executing Neo4j query...")
                        # Clean the query to remove markdown formatting before execution
                        clean_query = self._clean_cypher_query(full_query_response)
                        print(f"\n[DEBUG] Cleaned query for execution: {clean_query}")
                        neo4j_results = self.neo4j_connection.run_query(clean_query)
                        print(f"\n[DEBUG] Neo4j raw query results: {neo4j_results}")

                        # Check if we need fallback
                        should_fallback = analyze_missing_fields(user_question, neo4j_results)
                        if should_fallback:
                            print("[DEBUG] Neo4j results are insufficient")  # Removed HMDB fallback reference
                        elif not neo4j_results or neo4j_results == [None] or neo4j_results == [""]:
                            print("[DEBUG] No valid results from Neo4j")  # Removed HMDB fallback reference
                            should_fallback = True

                        # Get additional description results
                        for metabolite in metabolites:
                            more_results = self.neo4j_connection.run_query(f"""
                                MATCH (m:Metabolite)
                                WHERE toLower(m.name) = toLower('{metabolite}')
                                OR EXISTS {{ MATCH (m)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex) 
                                            WHERE any(syn IN si.synonyms WHERE toLower(syn) = toLower('{metabolite}')) }}
                                RETURN m.description
                            """)
                            if more_results:
                                print(f"\n[DEBUG] Additional description results: {more_results}")
                                neo4j_results += more_results

                        # Handle fallback if needed
                        if should_fallback and len(metabolites) > 0:
                            # HMDB API integration temporarily disabled
                            # if self.hmdb_client:
                            #     first_metabolite = metabolites[0]
                            #     fallback_data = None
                            #     
                            #     # Make API call based on ID or name
                            #     try:
                            #         if first_metabolite.startswith("HMDB"):
                            #             print(f"\n[DEBUG] Making HMDB API call for ID: {first_metabolite}")
                            #             payload = {"hmdb_id": [first_metabolite]}
                            #             fallback_data = self.hmdb_client.post("metabolites", payload)
                            #         else:
                            #             print(f"\n[DEBUG] Making HMDB API search for name: {first_metabolite}")
                            #             payload = {"name": first_metabolite}
                            #             fallback_data = self.hmdb_client.post("metabolites/search", payload)
                            #         
                            #         print(f"\n[DEBUG] HMDB API response: {fallback_data.keys() if fallback_data else None}")
                            #     except Exception as api_error:
                            #         print(f"\n[ERROR] HMDB API call failed: {api_error}")
                            #     
                            #     # Process API response
                            #     if fallback_data and "found" in fallback_data:
                            #         filtered_fallback_data = self._filter_hmdb_response(fallback_data)
                            #         intent_results["api_data"] = filtered_fallback_data
                            #         
                            #         # Run API reasoning chain
                            #         try:
                            #             print(f"\n[DEBUG] Running API reasoning chain...")
                            #             api_reasoning_inputs = {
                            #                 "api_data": filtered_fallback_data,
                            #                 "question": sub_question
                            #             }
                            #             api_reasoning_accumulator: List[str] = []
                            #             
                            #             # Stream API reasoning (only for internal processing)
                            #             async for _ in self._stream_and_accumulate(
                            #                 self.api_reasoning_chain,
                            #                 "API Summary",
                            #                 api_reasoning_inputs,
                            #                 api_reasoning_accumulator
                            #             ):
                            #                 pass  # We don't yield these messages, just accumulate
                            #             
                            #             # Store API reasoning results
                            #             api_summary = "".join(api_reasoning_accumulator)
                            #             intent_results["api_summary"] = api_summary
                            #             print(f"\n[DEBUG] API summary generated successfully")
                            pass  # Skip HMDB API integration for now
                        
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
                                summary_accumulator,
                                neo4j_results=neo4j_results  # Pass the Neo4j results for hyperlink processing
                            ):
                                yield sse_message
                        else:
                            intent_results["text_accumulator"].append("I apologize, but I couldn't find the information you're looking for in our database.")

                    except Exception as e:
                        print(f"\n[ERROR] Neo4j query execution failed: {e}")
                        intent_results["error"] = f"Neo4j query execution failed: {e}"
                        intent_results["text_accumulator"].append(f"I encountered an error while querying the database: {e}")
                
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
                        
                        # Stream DB summary (only for internal processing) 
                        async for _ in self._stream_and_accumulate(
                            self.summary_chain,
                            "DB Summary", 
                            summary_inputs,
                            summary_accumulator,
                            neo4j_results=memory_raw_data["neo4j_results"]  # Pass the Neo4j results for hyperlink processing
                        ):
                            pass  # We don't yield these messages, just accumulate
                        
                        # Store DB summary
                        db_summary = "".join(summary_accumulator)
                        intent_results["db_summary"] = db_summary
                        
                        # If we also have API data, use it too
                        api_summary = ""
                        if "api_data" in memory_raw_data and memory_raw_data["api_data"]:
                            api_reasoning_inputs = {
                                "api_data": memory_raw_data["api_data"],
                                "question": user_question
                            }
                            api_reasoning_accumulator: List[str] = []
                            
                            # Stream API reasoning (only for internal processing)
                            async for _ in self._stream_and_accumulate(
                                self.api_reasoning_chain,
                                "API Summary",
                                api_reasoning_inputs,
                                api_reasoning_accumulator
                            ):
                                pass  # We don't yield these messages, just accumulate
                            
                            # Store API summary
                            api_summary = "".join(api_reasoning_accumulator)
                            intent_results["api_summary"] = api_summary
                        
                        # Merge both summaries if we have API data
                        if api_summary:
                            final_summary = self._merge_summaries(db_summary, api_summary)
                            
                            # Remove hyperlink injection at individual text piece level
                            # Let post-processing happen only after aggregation
                            intent_results["text_accumulator"].append(final_summary)
                        else:
                            # Remove hyperlink injection at individual text piece level
                            # Let post-processing happen only after aggregation
                            intent_results["text_accumulator"].append(db_summary)
                    elif "api_data" in memory_raw_data and memory_raw_data["api_data"]:
                        # Only have API data from memory
                        api_reasoning_inputs = {
                            "api_data": memory_raw_data["api_data"],
                            "question": user_question
                        }
                        api_reasoning_accumulator: List[str] = []
                        
                        # Stream API reasoning (only for internal processing)
                        async for _ in self._stream_and_accumulate(
                            self.api_reasoning_chain,
                            "API Summary",
                            api_reasoning_inputs,
                            api_reasoning_accumulator
                        ):
                            pass  # We don't yield these messages, just accumulate
                        
                        # Store API summary
                        api_summary = "".join(api_reasoning_accumulator)
                        intent_results["api_summary"] = api_summary
                        intent_results["text_accumulator"].append(api_summary)
                    else:
                        # No usable data in memory after all
                        intent_results["error"] = "Memory data could not be used"
            else:
                yield self._format_message("Response", f"No database query needed. {query_plan.reasoning}")

        except Exception as e:
            yield self._format_message("Error", f"Error in pipeline: {e}")

    def _filter_hmdb_response(self, hmdb_data: dict) -> dict:
        # HMDB API integration temporarily disabled
        # """Preserves full HMDB API response and chunks structured data for LLM use."""
        # if not isinstance(hmdb_data, dict) or "found" not in hmdb_data or not hmdb_data["found"]:
        #     return {"error": "No valid data found in HMDB response"}
        # 
        # # Store full API response in cache folder instead of current directory
        # self.last_hmdb_api_result = hmdb_data  # For internal LLM access or chaining
        # 
        # # Save to cache folder (if it exists) or default to current directory
        # try:
        #     from pathlib import Path
        #     cache_dir = Path("cache/api_responses")
        #     if not cache_dir.exists():
        #         cache_dir.mkdir(parents=True, exist_ok=True)
        #     
        #     cache_file = cache_dir / "latest_hmdb_api_full.json"
        #     
        #     with open(cache_file, "w") as f:
        #         import json
        #         json.dump(hmdb_data, f, indent=2)
        # except Exception as e:
        #     print(f"Failed to save HMDB response to cache: {e}")
        #     try:
        #         with open("latest_hmdb_api_full.json", "w") as f:
        #             import json
        #             json.dump(hmdb_data, f, indent=2)
        #     except Exception as e:
        #         print(f"Failed to save HMDB response to fallback location: {e}")
        # 
        # # Instead of truncation, chunk by tag/key for LLM compatibility
        # MAX_METABOLITES = 10
        # metabolites = hmdb_data["found"][:MAX_METABOLITES]
        # chunked_metabolites = []
        # 
        # for metabolite in metabolites:
        #     chunked = {}
        #     for key, value in metabolite.items():
        #         if isinstance(value, dict):
        #             chunked[key] = self._chunk_nested_dict(value)
        #         elif isinstance(value, list):
        #             chunked[key] = self._chunk_list(value)
        #         elif isinstance(value, str):
        #             chunked[key] = self._chunk_text(value)
        #         else:
        #             chunked[key] = value
        #     chunked_metabolites.append(chunked)
        #     
        # return {"metabolites": chunked_metabolites}
        return {}  # Return empty dict while HMDB API is disabled

    def _get_metabolite_pathways(self, entity_name: str, entity_type: str = "Metabolite") -> List[Dict]:
        """
        Retrieve pathway information for a metabolite using the pathway service.
        This provides a direct way to get formatted pathway data without complex Cypher.
        
        Args:
            entity_name: The name or accession of the metabolite
            entity_type: The type of entity (should be "Metabolite")
            
        Returns:
            List of dictionaries containing pathway information with formatted URLs
        """
        from backend.services.pathway_service import get_pathways_for_metabolite, get_pathways_by_metabolite_name
        
        if not entity_name:
            return []
            
        # Handle different entity types
        if entity_type != "Metabolite":
            print(f"[WARNING] Pathway lookup only works for Metabolites, got {entity_type}")
            return []
            
        try:
            # Check if entity_name is an HMDB ID/accession
            if isinstance(entity_name, str) and entity_name.startswith("HMDB"):
                # Use accession-based lookup
                return get_pathways_for_metabolite(self.neo4j_connection, entity_name)
            else:
                # Use name-based lookup
                return get_pathways_by_metabolite_name(self.neo4j_connection, entity_name)
                
        except Exception as e:
            print(f"[ERROR] Error retrieving pathways for {entity_name}: {str(e)}")
            return []
            
    def _format_pathways_for_response(self, pathways: List[Dict]) -> str:
        """
        Format pathway information for inclusion in LLM responses.
        
        Args:
            pathways: List of pathway dictionaries from _get_metabolite_pathways
            
        Returns:
            Formatted string for inclusion in LLM prompts or responses
        """
        if not pathways:
            return "No pathway information found."
            
        result = "This metabolite is involved in the following pathways:\n\n"
        
        for idx, pathway in enumerate(pathways, 1):
            result += f"{idx}. {pathway['name']}\n"
            
            if "smpdb_url" in pathway:
                result += f"   SMPDB: {pathway['smpdb_url']}\n"
                
            if "kegg_url" in pathway:
                result += f"   KEGG: {pathway['kegg_url']}\n"
                
            result += "\n"
            
        return result

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
