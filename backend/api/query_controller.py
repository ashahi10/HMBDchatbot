from fastapi import APIRouter, Request, HTTPException, Depends, Cookie
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict, Any, Set
import json
import asyncio
import re
import os

router = APIRouter()

class QueryRequest(BaseModel):
    question: str
    session_id: Optional[str] = None

class CacheClearRequest(BaseModel):
    cache_type: Optional[str] = None

class MemoryClearRequest(BaseModel):
    session_id: str

class CreateSessionResponse(BaseModel):
    session_id: str

def extract_entities_from_response(message: Dict[str, Any]) -> Set[str]:
    """
    Extract entities from response messages for memory storage
    """
    entities = set()
    
    # Try to extract entities from different message sections
    if message.get("section") == "Extracting entities":
        try:
            raw_text = message.get("text", "")
            if raw_text and '"name":' in raw_text and '"type":' in raw_text:
                entities_dict = json.loads(raw_text)
                if "entities" in entities_dict and entities_dict["entities"]:
                    for entity in entities_dict["entities"]:
                        if "name" in entity and "type" in entity and "confidence" in entity:
                            # Prioritize entities with high confidence and metabolite types
                            if entity["confidence"] > 0.7 and entity["type"] in ["Metabolite", "CrossReference"]:
                                entities.add(entity["name"])
        except:
            pass
    
    # Look for HMDB IDs and chemical formulas in various sections
    if message.get("text"):
        text = message.get("text", "")
        
        # Look for HMDB IDs
        hmdb_pattern = r'\bHMDB\d+\b'
        hmdb_ids = re.findall(hmdb_pattern, text)
        entities.update(hmdb_ids)
        
        # Look for chemical names (like "Citric acid", "D-Psicose")
        if message.get("section") in ["Answer", "Response", "DB Summary", "API Summary"]:
            # First try to extract "X is a..." patterns often used in definitions
            definition_patterns = [
                r'([A-Z][a-z]+(?:\s+[a-z]+)*)\s+is\s+a',
                r'([A-Z][a-z]+(?:\s+[A-z][a-z]+)*)\s+\(',
                r'([A-Z][a-z]+(?:\s+[A-z][a-z]+)*)\s+has'
            ]
            
            for pattern in definition_patterns:
                matches = re.findall(pattern, text)
                entities.update(matches)
                
            # Special handling for D-X compounds
            d_compound_pattern = r'\b(D-[A-Z][a-z]+)\b'
            d_compounds = re.findall(d_compound_pattern, text)
            entities.update(d_compounds)
    
    return entities

@router.post("/session")
async def create_session(request: Request) -> CreateSessionResponse:
    """Create a new conversation session"""
    try:
        memory_service = request.app.state.memory_service
        session_id = memory_service.create_session()
        return CreateSessionResponse(session_id=session_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating session: {str(e)}")

@router.post("/query")
async def query_endpoint(query_request: QueryRequest, request: Request):
    pipeline = request.app.state.query_pipeline
    memory_service = request.app.state.memory_service
    llm_service = request.app.state.llm_service
    
    # Get or create session ID
    session_id = query_request.session_id
    if not session_id:
        # Create a new session if none provided
        session_id = memory_service.create_session()
    
    # Get recent conversation history
    recent_turns = memory_service.get_recent(session_id, limit=5)
    
    # Find relevant memory entries with improved relevance scoring
    relevant_turns = memory_service.find_relevant(session_id, query_request.question)
    
    # Debug output for relevance
    if os.getenv("DEBUG_MEMORY_RANKING", "").lower() == "true":
        print(f"\n[DEBUG] Found {len(relevant_turns)} relevant turns for: '{query_request.question}'")
    
    # Import and use the decision service
    from backend.services.decision_service import QueryDecisionService
    decision_service = QueryDecisionService()
    
    # Make intelligent decision about query path
    decision, memory_entry = decision_service.decide_query_path(
        query=query_request.question,
        memory_results=relevant_turns,
        conversation_history=recent_turns
    )
    
    # Debug log the decision
    print(f"[INFO] Decision for query '{query_request.question}': {decision}")
    
    # Accumulate response
    answer_accumulator = []
    source_accumulator = []
    entity_accumulator = set()
    tags_accumulator = set()
    
    # Enhanced raw data capture for Phase 3 memory reuse
    raw_data_accumulator = {
        "entity_extraction": None,
        "neo4j_results": None,
        "api_data": None,
        "query_plan": None,
        "query": None
    }
    
    # PHASE 2 ENHANCEMENT: Choose the best query handling path
    if decision == "memory":
        # Use memory directly for high-confidence matches
        async def memory_wrapper():
            # Extract the stored answer from memory
            stored_answer = memory_entry.get("answer", "")
            stored_entities = memory_entry.get("entities", [])
            
            # Check if we have raw_data for potential reuse
            if memory_entry.get("raw_data"):
                raw_data_accumulator.update(memory_entry.get("raw_data", {}))
            
            # Format as SSE message for consistency
            memory_section = {
                "section": "Answer",
                "text": f"Based on previous conversations: {stored_answer}",
                "source": "memory"
            }
            
            # Yield memory result
            yield f"data:{json.dumps(memory_section)}\n\n"
            
            # Add memory metadata to accumulators
            answer_accumulator.append(stored_answer)
            entity_accumulator.update(stored_entities)
            source_accumulator.append("memory")
            
            # Signal completion
            yield f"data:{json.dumps({'section': 'DONE'})}\n\n"
            
            # After completion, store that we used memory
            memory_service.store(
                session_id=session_id,
                user_query=query_request.question,
                answer=stored_answer,
                entity=list(entity_accumulator)[0] if entity_accumulator else None,
                tags=list(tags_accumulator) if tags_accumulator else None,
                source="memory_reuse",
                raw_data=raw_data_accumulator if any(raw_data_accumulator.values()) else None
            )
        
        return StreamingResponse(memory_wrapper(), media_type="text/event-stream")
    
    elif decision == "general":
        # Use the general question answering for non-DB queries
        context = decision_service.prepare_context_from_memory(relevant_turns)
        
        async def general_wrapper():
            # Inform the user we're answering a general question
            thinking_message = {
                "section": "Thinking",
                "text": "This appears to be a general question that doesn't require database lookup. Generating answer..."
            }
            yield f"data:{json.dumps(thinking_message)}\n\n"
            
            # Start the answer section
            start_answer = {
                "section": "Answer",
                "text": ""
            }
            yield f"data:{json.dumps(start_answer)}\n\n"
            
            # Use LLM service to answer general question with streaming
            answer_text = ""
            async for text_chunk in llm_service.answer_general_question(
                question=query_request.question, 
                context=context
            ):
                answer_text += text_chunk
                chunk_message = {
                    "section": "Answer",
                    "text": text_chunk
                }
                yield f"data:{json.dumps(chunk_message)}\n\n"
            
            # Signal completion
            yield f"data:{json.dumps({'section': 'DONE'})}\n\n"
            
            # Store in memory
            answer_accumulator.append(answer_text)
            source_accumulator.append("general")
            
            # Extract any obvious entities from the answer
            extracted_entities = extract_entities_from_response({
                "section": "Answer",
                "text": answer_text
            })
            entity_accumulator.update(extracted_entities)
            
            # Add general answer data to raw_data for potential future reuse
            raw_data_accumulator["general_answer"] = answer_text
            
            # Store the interaction in memory
            memory_service.store(
                session_id=session_id,
                user_query=query_request.question,
                answer=answer_text,
                entity=list(entity_accumulator)[0] if entity_accumulator else None,
                tags=list(tags_accumulator) if tags_accumulator else None,
                source="general",
                raw_data=raw_data_accumulator if any(raw_data_accumulator.values()) else None
            )
        
        return StreamingResponse(general_wrapper(), media_type="text/event-stream")
    
    else:  # decision == "pipeline"
        # Create a wrapper around the pipeline stream to capture the final response
        async def pipeline_wrapper():
            section = None
            async for sse_message in pipeline.run_pipeline(
                user_question=query_request.question,
                conversation_history=recent_turns,
                relevant_history=relevant_turns
            ):
                # Parse SSE message to extract content
                try:
                    message_json = sse_message[len("data:"):].strip()
                    message = json.loads(message_json)
                    
                    # Track response sections
                    msg_section = message.get("section")
                    if msg_section and msg_section not in ["Thinking", "DONE"]:
                        section = msg_section
                    
                    # Accumulate answer content
                    if msg_section == "Answer" or msg_section == "Response":
                        answer_accumulator.append(message.get("text", ""))
                    
                    # Accumulate entity info
                    extracted_entities = extract_entities_from_response(message)
                    entity_accumulator.update(extracted_entities)
                    
                    # Enhanced accumulation for Phase 3 memory reuse
                    # Capture entity extraction results
                    if msg_section == "Extracting entities" and message.get("text"):
                        try:
                            extracted_data = json.loads(message.get("text", "{}"))
                            raw_data_accumulator["entity_extraction"] = extracted_data
                        except:
                            pass
                    
                    # Capture query planning 
                    if msg_section == "Query planning" and message.get("text"):
                        try:
                            # Store the query plan for future reference
                            raw_data_accumulator["query_plan"] = message.get("text")
                        except:
                            pass
                            
                    # Capture query text
                    if msg_section == "Query execution" and message.get("text"):
                        # Store the actual query that was executed
                        raw_data_accumulator["query"] = message.get("text")
                    
                    # Track source
                    if msg_section == "DB Summary":
                        if "neo4j" not in source_accumulator:
                            source_accumulator.append("neo4j")
                    
                    if msg_section == "API Summary":
                        if "api" not in source_accumulator:
                            source_accumulator.append("api")
                    
                    # Add tags based on content patterns and query intent
                    if msg_section == "Answer" or msg_section == "Response" or msg_section == "DB Summary" or msg_section == "API Summary":
                        text = message.get("text", "").lower()
                        
                        # Common information patterns
                        if "chemical formula" in text or "formula:" in text:
                            tags_accumulator.add("chemical_formula")
                        
                        if "inchi" in text or "inchikey" in text:
                            tags_accumulator.add("inchikey")
                        
                        if "iupac" in text:
                            tags_accumulator.add("iupac_name")
                        
                        if "molecular weight" in text or "weight:" in text:
                            tags_accumulator.add("molecular_weight")
                        
                        if "smiles" in text:
                            tags_accumulator.add("smiles")
                        
                        if "pathway" in text:
                            tags_accumulator.add("pathway")
                        
                        if "summary" in text or "overview" in text:
                            tags_accumulator.add("summary")
                        
                        # Extract any formula patterns (like C6H12O6)
                        formula_pattern = r'\b[A-Z][a-z]?[0-9]*(?:[A-Z][a-z]?[0-9]*)*\b'
                        formulas = re.findall(formula_pattern, text)
                        for formula in formulas:
                            # Only consider typical chemical formulas
                            if re.match(r'^[A-Z][a-z]?[0-9]+', formula) and len(formula) > 3:
                                entity_accumulator.add(formula)
                                tags_accumulator.add("chemical_formula")
                        
                        # Extract HMDB IDs
                        hmdb_pattern = r'\bHMDB\d+\b'
                        hmdb_ids = re.findall(hmdb_pattern, text)
                        for hmdb_id in hmdb_ids:
                            entity_accumulator.add(hmdb_id)
                            
                        # Extract InChIKeys
                        inchikey_pattern = r'\b[A-Z]{14}-[A-Z]{10}-[A-Z]\b'
                        inchikeys = re.findall(inchikey_pattern, text)
                        for inchikey in inchikeys:
                            entity_accumulator.add(inchikey)
                            tags_accumulator.add("inchikey")
                        
                        # Explicitly look for D-Psicose type compounds (fix for test issue)
                        d_pattern = r'\b(D-[A-Z][a-z]+)\b'
                        d_compounds = re.findall(d_pattern, text)
                        entity_accumulator.update(d_compounds)
                except:
                    pass
                    
                # Forward the message to the client
                yield sse_message
                
            # After conversation is done, store the result in memory
            full_answer = "".join(answer_accumulator).strip()
            source = "llm"
            if source_accumulator:
                source = source_accumulator[0]  # Use the first source for classification
            
            entity = None
            if entity_accumulator:
                # Prioritize entities in this order: HMDB IDs > well-formed chemical names > other entities
                prioritized_entities = []
                
                # HMDB IDs first
                hmdb_entities = [e for e in entity_accumulator if e.startswith("HMDB")]
                if hmdb_entities:
                    prioritized_entities.extend(sorted(hmdb_entities))
                
                # Then D- prefixed compounds
                d_entities = [e for e in entity_accumulator if e.startswith("D-")]
                if d_entities:
                    prioritized_entities.extend(sorted(d_entities))
                
                # Then named chemical compounds
                chemical_entities = [e for e in entity_accumulator 
                                   if re.match(r'^[A-Z][a-z]+', e) 
                                   and not e.startswith("HMDB")
                                   and not e.startswith("D-")]
                if chemical_entities:
                    prioritized_entities.extend(sorted(chemical_entities))
                
                # Then any other entities
                other_entities = [e for e in entity_accumulator if e not in prioritized_entities]
                prioritized_entities.extend(other_entities)
                
                if prioritized_entities:
                    entity = prioritized_entities[0]
                    
                    # Debug output
                    if os.getenv("DEBUG_MEMORY_RANKING", "").lower() == "true":
                        print(f"\n[DEBUG] Selected entity: {entity}")
                        print(f"[DEBUG] All extracted entities: {entity_accumulator}")
            
            # Store in memory if we have a valid answer
            if full_answer:
                # Store with enhanced raw data for Phase 3 memory reuse
                success = memory_service.store(
                    session_id=session_id,
                    user_query=query_request.question,
                    answer=full_answer,
                    source=source,
                    entity=entity,
                    tags=list(tags_accumulator) if tags_accumulator else None,
                    raw_data=raw_data_accumulator if any(raw_data_accumulator.values()) else None
                )
                
                if os.getenv("DEBUG_MEMORY_RANKING", "").lower() == "true" and success:
                    print(f"[DEBUG] Stored memory with entity: {entity}")
                    if any(raw_data_accumulator.values()):
                        print(f"[DEBUG] Stored raw data types: {[k for k, v in raw_data_accumulator.items() if v]}")
    
    # Return streaming response with wrapped pipeline
    return StreamingResponse(pipeline_wrapper(), media_type="text/event-stream", headers={"X-Session-ID": session_id})

@router.get("/memory/{session_id}")
async def get_memory(session_id: str, request: Request):
    """Get conversation history for a session"""
    try:
        memory_service = request.app.state.memory_service
        session_data = memory_service.get_session(session_id)
        return JSONResponse(session_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving memory: {str(e)}")

@router.post("/memory/clear")
async def clear_memory(clear_request: MemoryClearRequest, request: Request):
    """Clear conversation history for a session"""
    try:
        memory_service = request.app.state.memory_service
        success = memory_service.clear(clear_request.session_id)
        
        if not success:
            raise HTTPException(status_code=404, detail="Session not found")
            
        return JSONResponse({"status": "success", "message": "Cleared session memory"})
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error clearing memory: {str(e)}")

@router.get("/cache/status")
async def cache_status(request: Request):
    """Get the status of all caches"""
    try:
        cache_manager = request.app.state.cache_manager
        
        # Count files in each cache directory
        schema_count = len(list(cache_manager.schema_cache_path.glob('*.json')))
        query_count = len(list(cache_manager.query_cache_path.glob('*.pickle')))
        api_count = len(list(cache_manager.api_cache_path.glob('*.json')))
        
        # Calculate approximate total size
        schema_size = sum(f.stat().st_size for f in cache_manager.schema_cache_path.glob('*.json'))
        query_size = sum(f.stat().st_size for f in cache_manager.query_cache_path.glob('*.pickle'))
        api_size = sum(f.stat().st_size for f in cache_manager.api_cache_path.glob('*.json'))
        
        return JSONResponse({
            "status": "active",
            "schema_cache": {
                "count": schema_count,
                "size_bytes": schema_size,
                "ttl_seconds": cache_manager.ttl['schema']
            },
            "query_cache": {
                "count": query_count,
                "size_bytes": query_size,
                "ttl_seconds": cache_manager.ttl['query']
            },
            "api_cache": {
                "count": api_count,
                "size_bytes": api_size,
                "ttl_seconds": cache_manager.ttl['api']
            },
            "total_size_bytes": schema_size + query_size + api_size
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting cache status: {str(e)}")

@router.post("/cache/clear")
async def clear_cache(request: Request, clear_request: CacheClearRequest = None):
    """Clear specific or all caches"""
    try:
        cache_manager = request.app.state.cache_manager
        cache_type = clear_request.cache_type if clear_request else None
        
        # Validate cache type
        if cache_type and cache_type not in ['schema', 'query', 'api']:
            raise HTTPException(
                status_code=400, 
                detail="Invalid cache type. Must be one of: 'schema', 'query', 'api', or null to clear all"
            )
        
        cache_manager.clear_cache(cache_type)
        
        message = f"Cleared {cache_type} cache" if cache_type else "Cleared all caches"
        return JSONResponse({"status": "success", "message": message})
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")
