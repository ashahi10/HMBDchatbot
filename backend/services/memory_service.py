import os
import json
import time
import uuid
import re
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Set, Tuple
from datetime import datetime

class MemoryService:
    """
    Service for managing conversation memory and history.
    Provides functions for storing, retrieving, and searching conversation history.
    
    The memory structure is:
    {
      "session_id": {
        "timestamp": 1234567890,
        "turns": [
          {
            "user_query": "...",
            "answer": "...",
            "source": "neo4j" | "api" | "llm",
            "entity": "dopamine",
            "tags": ["chemical_formula", "summary"],
            "raw_data": {...}
          },
          ...
        ]
      }
    }
    """
    
    def __init__(self, 
                 base_path: str = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "memory"),
                 memory_ttl: int = 2592000):  # 30 days in seconds
        
        self.base_path = Path(base_path)
        self.memory_ttl = memory_ttl
        
        # Create memory directory if it doesn't exist
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
    
    def _get_session_file_path(self, session_id: str) -> Path:
        """Get the file path for a given session ID"""
        return self.base_path / f"{session_id}.json"
    
    def _is_valid_memory(self, memory_data: Dict) -> bool:
        """
        Check if memory data is valid and useful
        
        Filters out:
        - Empty or None answers
        - Failed queries or irrelevant fallbacks
        """
        if not memory_data.get("answer"):
            return False
        
        if memory_data.get("tags") and "failed" in memory_data.get("tags"):
            return False
            
        return True
    
    def _extract_entities(self, text: str) -> Set[str]:
        """
        Extract potential entity names from text
        
        Uses heuristics to identify chemical names, IDs, and other entities.
        """
        # Extracted entities will be stored here
        entities = set()
        
        # Extract words that look like chemical names or identifiers
        if text:
            # Match potential chemical names (capitalized words, chemical formulas, HMDB IDs)
            # Chemical formulas like C6H12O6
            formula_pattern = r'\b[A-Z][a-z]?[0-9]*(?:[A-Z][a-z]?[0-9]*)*\b'
            chemical_formulas = re.findall(formula_pattern, text)
            entities.update(chemical_formulas)
            
            # HMDB IDs like HMDB0000001
            hmdb_pattern = r'\bHMDB\d+\b'
            hmdb_ids = re.findall(hmdb_pattern, text)
            entities.update(hmdb_ids)
            
            # Capitalized multi-word chemical names with potential hyphens (e.g., "Citric Acid", "D-Psicose")
            # Modified to better handle D-Psicose pattern
            chemical_name_pattern = r'\b(?:[A-Z][a-z]*-)?[A-Z][a-z]+(?:[ -][A-Z]?[a-z]+)*\b'
            chemical_names = re.findall(chemical_name_pattern, text)
            entities.update(chemical_names)
            
            # InChIKeys (fixed format)
            inchikey_pattern = r'\b[A-Z]{14}-[A-Z]{10}-[A-Z]\b'
            inchikeys = re.findall(inchikey_pattern, text)
            entities.update(inchikeys)
            
            # Common keywords that might indicate entity references
            for term in ["acid", "amine", "protein", "enzyme", "receptor", "pathway"]:
                if term in text.lower():
                    # Find phrases containing these terms
                    pattern = r'\b\w+\s+' + term + r'\b|\b\w+[-]?' + term + r'\b'
                    matches = re.findall(pattern, text.lower())
                    entities.update(m.strip() for m in matches)
        
        return entities
    
    def get_session(self, session_id: str) -> Dict:
        """Get all turns for a given session"""
        session_file = self._get_session_file_path(session_id)
        
        if not session_file.exists():
            # Return empty session structure
            return {
                "session_id": session_id,
                "timestamp": int(time.time()),
                "turns": []
            }
        
        try:
            with open(session_file, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError:
            # If file is corrupted, return empty session
            return {
                "session_id": session_id,
                "timestamp": int(time.time()),
                "turns": []
            }
    
    def store(self, session_id: str, user_query: str, answer: str, 
              source: str = "llm", entity: Optional[str] = None, 
              tags: Optional[List[str]] = None, raw_data: Optional[Dict] = None) -> bool:
        """
        Store a new memory entry in the session
        
        Args:
            session_id: Unique ID for the conversation session
            user_query: User's question
            answer: Answer provided to the user
            source: Source of the answer (neo4j, api, llm)
            entity: Main entity discussed (if any)
            tags: List of tags for categorizing the memory
            raw_data: Raw data that was used to generate the answer
            
        Returns:
            bool: True if memory was stored, False if filtered out
        """
        # Create memory entry
        memory_data = {
            "user_query": user_query,
            "answer": answer,
            "source": source,
            "timestamp": int(time.time())
        }
        
        # Add optional fields if provided
        if entity:
            memory_data["entity"] = entity
        else:
            # Try to extract entity from query if not provided
            extracted_entities = self._extract_entities(user_query)
            if extracted_entities:
                memory_data["entity"] = list(extracted_entities)[0]
                
                # Also add extracted entities as tags if not already present
                if not tags:
                    tags = []
                for ent in extracted_entities:
                    if ent not in tags:
                        tags.append(ent)
        
        if tags:
            memory_data["tags"] = tags
            
        if raw_data:
            memory_data["raw_data"] = raw_data
        
        # Check if memory should be stored (filter out low-quality entries)
        if not self._is_valid_memory(memory_data):
            return False
        
        # Get existing session data
        session_data = self.get_session(session_id)
        
        # Add new turn
        session_data["turns"].append(memory_data)
        
        # Update timestamp
        session_data["timestamp"] = int(time.time())
        
        # Write back to file
        session_file = self._get_session_file_path(session_id)
        with open(session_file, 'w') as f:
            json.dump(session_data, f, indent=2)
            
        return True
    
    def get_recent(self, session_id: str, limit: int = 5) -> List[Dict]:
        """
        Get the most recent memory entries for a session
        
        Args:
            session_id: Unique ID for the conversation session
            limit: Maximum number of recent turns to retrieve
            
        Returns:
            List of memory entries (most recent first)
        """
        session_data = self.get_session(session_id)
        turns = session_data.get("turns", [])
        
        # Return the most recent 'limit' turns
        return turns[-limit:] if turns else []
    
    def _calculate_relevance_score(self, 
                                   query: str, 
                                   memory: Dict, 
                                   recency_index: int = 0,
                                   total_memories: int = 1) -> Tuple[float, Dict[str, float]]:
        """
        Calculate a sophisticated relevance score between current query and a memory entry
        
        Args:
            query: Current user query
            memory: Memory entry to score
            recency_index: Index of memory (0 = most recent)
            total_memories: Total number of memories
            
        Returns:
            Tuple of (total_score, score_components) where total_score is normalized to [0,1]
        """
        score_components = {}
        
        # 1. Extract entities from current query
        query_entities = self._extract_entities(query.lower())
        
        # 2. Get entities from memory
        memory_entities = set()
        if memory.get("entity"):
            memory_entities.add(memory.get("entity").lower())
        
        # Add any entities from tags
        if memory.get("tags"):
            memory_entities.update(tag.lower() for tag in memory.get("tags", []) 
                                  if not tag.startswith("_") and not tag in ["failed", "complete"])
        
        # Also extract entities from memory's user query
        memory_entities.update(ent.lower() for ent in self._extract_entities(memory.get("user_query", "")))
        
        # 3. Calculate entity overlap score (highest weight)
        entity_overlap = query_entities.intersection(memory_entities)
        # Normalize entity score to [0,1] range - each entity match gets 0.4 points, max 0.4
        entity_score = min(len(entity_overlap) * 0.4, 0.4)
        score_components["entity_match"] = entity_score
        
        # 4. Calculate keyword similarity (medium weight)
        # Get words from queries
        query_words = set(query.lower().split())
        memory_words = set(memory.get("user_query", "").lower().split())
        
        common_words = query_words.intersection(memory_words)
        # Normalize keyword score to [0,1] range - max 0.3
        keyword_score = min(len(common_words) * 0.3 / max(len(query_words), len(memory_words)), 0.3)
        score_components["keyword_similarity"] = keyword_score
        
        # 5. Calculate tag similarity (lower weight)
        query_tags = self._extract_query_intent_tags(query)
        memory_tags = set(memory.get("tags", []))
        
        tag_overlap = query_tags.intersection(memory_tags)
        # Normalize tag score to [0,1] range - max 0.15
        tag_score = min(len(tag_overlap) * 0.15, 0.15)
        score_components["tag_match"] = tag_score
        
        # 6. Apply recency bias (newer memories get slight boost)
        # Normalize recency score to [0,1] range - max 0.1
        recency_score = 0.1 * (1 - (recency_index / max(1, total_memories)))
        score_components["recency"] = recency_score
        
        # 7. Special handling for ambiguous follow-up queries
        ambiguity_score = 0
        ambiguity_terms = ["it", "this", "that", "the", "these", "those", "its", "about", "for"]
        
        # Check if the query is likely a follow-up question (very short or has ambiguous terms)
        is_ambiguous = len(query.split()) <= 5 or any(term in query.lower().split() for term in ambiguity_terms)
        
        if is_ambiguous:
            # For ambiguous queries, strongly boost the recency factor
            # Normalize ambiguity score to [0,1] range - max 0.2
            ambiguity_score = min(0.2 * (1 - (recency_index / max(1, min(3, total_memories)))), 0.2)
            score_components["ambiguity_boost"] = ambiguity_score
        
        # 8. Penalize entity mismatch (if queries are about different entities)
        # Only apply when there's a clear entity mismatch and not an ambiguous follow-up
        entity_mismatch_penalty = 0
        
        if not is_ambiguous and query_entities and memory_entities and not entity_overlap:
            # Apply a normalized penalty that won't push score below 0
            entity_mismatch_penalty = -0.3
            score_components["entity_mismatch"] = entity_mismatch_penalty
        
        # Calculate total score - sum of all components
        total_score = entity_score + keyword_score + tag_score + recency_score + ambiguity_score + entity_mismatch_penalty
        
        # Ensure final score is in [0,1] range
        total_score = max(0.0, min(1.0, total_score))
        
        return (total_score, score_components)
    
    def _extract_query_intent_tags(self, query: str) -> Set[str]:
        """Extract tags that represent query intent like 'chemical_formula', 'inchikey', etc."""
        tags = set()
        query_lower = query.lower()
        
        # Check for common intents
        intent_patterns = {
            "chemical_formula": ["formula", "chemical formula"],
            "inchikey": ["inchikey", "inchi key"],
            "iupac_name": ["iupac", "iupac name"],
            "structure": ["structure", "molecular structure"],
            "property": ["property", "properties"],
            "pathway": ["pathway", "pathways", "metabolic pathway"],
            "concentration": ["concentration", "level", "amount"],
            "reference": ["reference", "citation", "paper", "study"],
            "disease": ["disease", "condition", "disorder"],
            "summary": ["summary", "overview", "information about"]
        }
        
        for tag, patterns in intent_patterns.items():
            if any(pattern in query_lower for pattern in patterns):
                tags.add(tag)
        
        return tags
    
    def find_relevant(self, session_id: str, user_query: str, threshold: float = 0.2) -> List[Dict]:
        """
        Find relevant memory entries based on the current query using a sophisticated scoring system
        
        Args:
            session_id: Unique ID for the conversation session  
            user_query: Current user query to match against
            threshold: Minimum similarity score (0-1) for relevance
            
        Returns:
            List of relevant memory entries (most relevant first)
        """
        session_data = self.get_session(session_id)
        turns = session_data.get("turns", [])
        
        if not turns:
            return []
        
        # Extract entities from current query
        query_entities = self._extract_entities(user_query.lower())
        
        # Process all memory turns with sophisticated scoring
        relevant_turns = []
        total_turns = len(turns)
        
        for idx, turn in enumerate(turns):
            recency_index = total_turns - idx - 1  # Newer turns have lower index
            
            # Calculate sophisticated relevance score
            relevance_score, score_components = self._calculate_relevance_score(
                user_query, 
                turn, 
                recency_index=recency_index,
                total_memories=total_turns
            )
            
            # Lower the threshold for ambiguous queries
            effective_threshold = threshold
            if "ambiguity_boost" in score_components:
                effective_threshold = threshold * 0.7  # 30% lower threshold for ambiguous queries
            
            # Only include memories that exceed the threshold and don't have severe penalties
            if relevance_score >= effective_threshold:
                # Add scores to turn for debugging/analysis
                turn_with_score = {
                    **turn, 
                    "relevance_score": relevance_score,
                    "score_components": score_components
                }
                relevant_turns.append(turn_with_score)
        
        # Sort by relevance score (most relevant first)
        sorted_turns = sorted(relevant_turns, key=lambda x: x.get("relevance_score", 0), reverse=True)
        
        # Print debug info for top results
        if sorted_turns and os.getenv("DEBUG_MEMORY_RANKING", "").lower() == "true":
            print("\n[DEBUG] Memory relevance ranking:")
            for i, turn in enumerate(sorted_turns[:3]):
                print(f"  {i+1}. Score: {turn['relevance_score']:.2f}")
                print(f"     Query: {turn['user_query']}")
                print(f"     Entity: {turn.get('entity', 'None')}")
                print(f"     Components: {turn['score_components']}")
        
        return sorted_turns
    
    def clear(self, session_id: str) -> bool:
        """
        Clear all memory entries for a session
        
        Args:
            session_id: Unique ID for the conversation session
            
        Returns:
            bool: True if session was cleared, False otherwise
        """
        session_file = self._get_session_file_path(session_id)
        
        if session_file.exists():
            try:
                # Remove the file
                session_file.unlink()
                return True
            except Exception:
                return False
        
        return False
    
    def create_session(self) -> str:
        """
        Create a new session and return its ID
        
        Returns:
            str: New session ID
        """
        session_id = str(uuid.uuid4())
        
        # Initialize empty session
        session_data = {
            "session_id": session_id,
            "timestamp": int(time.time()),
            "turns": []
        }
        
        # Write to file
        session_file = self._get_session_file_path(session_id)
        with open(session_file, 'w') as f:
            json.dump(session_data, f, indent=2)
            
        return session_id
    
    def clean_expired_sessions(self) -> int:
        """
        Remove expired session files
        
        Returns:
            int: Number of sessions removed
        """
        current_time = time.time()
        expired_count = 0
        
        for session_file in self.base_path.glob("*.json"):
            try:
                # Check file modification time
                file_time = session_file.stat().st_mtime
                
                # If file is older than TTL, remove it
                if (current_time - file_time) > self.memory_ttl:
                    session_file.unlink()
                    expired_count += 1
            except Exception:
                continue
                
        return expired_count 