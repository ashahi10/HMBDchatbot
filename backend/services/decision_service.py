import re
from typing import Dict, List, Tuple, Optional, Set, Any, Literal
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class QueryDecisionService:
    """
    Service for making intelligent decisions about query handling.
    Determines whether a query should:
    1. Directly use memory (high-confidence memory match)
    2. Use the full database query pipeline
    3. Be handled as a general question (without database query)
    """
    
    # Patterns for questions that likely don't require database queries
    GENERAL_QUESTION_PATTERNS = [
        # Definition patterns
        r"(?:what|define|explain|describe)\s+(?:is|are|does)\s+(?:a|an|the)?\s*",
        r"(?:what|define|explain|describe)\s+(?:do|does)\s+(?:we|you|I)\s+(?:mean|understand|know)\s+(?:by|about)\s+",
        r"(?:can|could)\s+you\s+(?:explain|describe|tell\s+me)\s+(?:about|what|how)\s+",

        # General knowledge patterns
        r"how\s+(?:do|does|can|should)\s+(?:I|one|we|you)\s+",
        r"why\s+(?:do|does|is|are|can|should)\s+",
        r"(?:what|which)\s+(?:are|is)\s+the\s+(?:difference|similarities|relationship)s?\s+between\s+",
        
        # Conceptual patterns
        r"(?:explain|tell\s+me\s+about|describe)\s+the\s+(?:concept|principle|theory|process|mechanism)\s+of\s+",
        r"(?:how|why)\s+(?:does|do|is|are)\s+(?:a|an|the)?\s*\w+\s+(?:related|connected|linked|associated)\s+to\s+",
        
        # Casual conversation
        r"(?:hi|hello|hey|greetings|howdy)",
        r"(?:how\s+are\s+you|what's\s+up|how's\s+it\s+going)",
        r"(?:thank|thanks)",
        r"(?:bye|goodbye|see\s+you)",
        
        # Additional explanation patterns
        r"can\s+you\s+explain\s+how\s+.+\s+works",
        r"(?:what|how)\s+(?:is|are|does)\s+(?:the\s+)?(?:process|mechanism|function|principle)\s+of\s+",
        r"(?:can|could)\s+you\s+(?:give|provide)\s+(?:me|us)\s+(?:a|an|some)\s+(?:explanation|overview|insight)\s+(?:about|on|into)\s+"
    ]
    
    # List of ambiguous entities or concepts that should always use the pipeline
    REQUIRE_QUERY_ENTITIES = {
        "metabolism", "citric acid", "hmdb", "inchi", "smiles", "kegg", "pubchem", 
        "pathway", "metabolite", "enzyme", "protein", "gene"
    }
    
    def __init__(self, memory_confidence_threshold: float = 0.65):
        """
        Initialize the decision service
        
        Args:
            memory_confidence_threshold: Minimum confidence score for directly using memory
        """
        self.memory_confidence_threshold = memory_confidence_threshold
    
    def is_general_question(self, query: str) -> bool:
        """
        Check if a query is likely a general question not requiring database access.
        
        Args:
            query: The user's question
            
        Returns:
            bool: True if it appears to be a general question
        """
        query = query.lower().strip()
        
        # Special handling for specific education/explanation patterns
        educational_patterns = {
            "how does metabolism work in humans": True,
            "what is metabolism in humans": True,
            "explain how metabolism works": True
        }
        
        # Check exact matches for known educational patterns
        for pattern, result in educational_patterns.items():
            if pattern in query:
                return result
        
        # Check if the query matches any of the general question patterns
        for pattern in self.GENERAL_QUESTION_PATTERNS:
            if re.match(pattern, query):
                
                # Even if it matches a pattern, we need more nuanced logic for entities
                # Special case: Explanatory questions about entities are still general
                explanatory_patterns = [
                    r"how\s+does\s+.+\s+work",
                    r"what\s+is\s+.+\s+in\s+general",
                    r"explain\s+the\s+concept\s+of\s+",
                    r"explain\s+what\s+.+\s+is",
                    r"how\s+does\s+.+\s+function",
                ]
                
                # If it's clearly an explanatory question, treat as general regardless of entities
                if any(re.search(exp_pattern, query) for exp_pattern in explanatory_patterns):
                    logger.debug(f"Query identified as explanatory general question: {query}")
                    return True
                
                # Check for specific database lookup indicators
                db_lookup_indicators = [
                    "show me", "look up", "find", "search for", "structure of", 
                    "formula for", "molecular weight of", "properties of", "id for"
                ]
                
                # If it contains specific lookup phrases, it's likely a DB question
                if any(indicator in query for indicator in db_lookup_indicators):
                    return False
                
                # Check for compound words like "metabolism in humans" which should be general
                compound_educational_phrases = [
                    "in humans", "in cells", "in the body", "process of", "concept of",
                    "general overview", "basics of", "introduction to"
                ]
                
                # Check if the query contains both an entity and an educational context
                for entity in self.REQUIRE_QUERY_ENTITIES:
                    if entity in query:
                        # Check if it's in an educational context
                        for phrase in compound_educational_phrases:
                            if phrase in query and entity in query:
                                return True
                                
                        # If it matches explanation patterns about the entity, still general
                        explanation_about_entity = [
                            rf"what\s+is\s+{re.escape(entity)}",
                            rf"explain\s+{re.escape(entity)}",
                            rf"how\s+does\s+{re.escape(entity)}\s+work",
                            rf"why\s+is\s+{re.escape(entity)}"
                        ]
                        
                        if any(re.search(pattern, query) for pattern in explanation_about_entity):
                            logger.debug(f"Query is explanatory about entity '{entity}': {query}")
                            return True
                            
                        logger.debug(f"Query matches general pattern but contains DB entity '{entity}': {query}")
                        return False
                
                logger.debug(f"Query identified as general question: {query}")
                return True
        
        # Check word count - very short or very long queries are often general
        word_count = len(query.split())
        if word_count <= 3 or word_count >= 25:
            # Short greetings or very lengthy explanatory questions tend to be general
            # But still check for specific entities with the same nuanced logic
            
            # Check for specific database lookup indicators
            db_lookup_indicators = [
                "show me", "look up", "find", "search for", "structure of", 
                "formula for", "molecular weight of", "properties of", "id for"
            ]
            
            if any(indicator in query for indicator in db_lookup_indicators):
                return False
                
            for entity in self.REQUIRE_QUERY_ENTITIES:
                if entity in query:
                    # If it's a clearly explanatory question about the entity, still general
                    if re.search(rf"how\s+does\s+{re.escape(entity)}\s+work", query):
                        return True
                        
                    return False
            
            logger.debug(f"Query identified as general based on length ({word_count} words): {query}")
            return True
            
        return False
    
    def should_use_memory(self, query: str, memory_results: List[Dict], 
                         conversation_history: List[Dict] = None) -> Tuple[bool, Optional[Dict]]:
        """
        Determine if the memory contains a high-confidence match for this query.
        
        Args:
            query: The user's question
            memory_results: List of relevant memory entries sorted by relevance
            conversation_history: Optional list of recent conversation history
            
        Returns:
            Tuple of (should_use_memory, memory_entry)
        """
        if not memory_results:
            return False, None
        
        # Get the most relevant memory result
        top_memory = memory_results[0]
        relevance_score = top_memory.get("relevance_score", 0)
        
        # Check if we have relevance score components for more detailed analysis
        score_components = top_memory.get("score_components", {})
        
        # Enhance the decision with contextual information
        is_followup = self._is_likely_followup(query, conversation_history)
        has_entity_match = score_components.get("entity_match", 0) > 0
        exact_entity_match = score_components.get("entity_match", 0) > 0.35
        high_keyword_similarity = score_components.get("keyword_similarity", 0) > 0.2
        
        # Determine if this is a high-confidence match
        if relevance_score >= self.memory_confidence_threshold:
            logger.info(f"Found high-confidence memory match ({relevance_score:.2f}): {top_memory.get('user_query')}")
            return True, top_memory
        
        # For follow-up questions, we can be more lenient with the threshold
        if is_followup and has_entity_match and relevance_score >= (self.memory_confidence_threshold * 0.9):
            logger.info(f"Found good memory match for follow-up question ({relevance_score:.2f}): {top_memory.get('user_query')}")
            return True, top_memory
        
        # For repeated questions with the same entity
        if exact_entity_match and high_keyword_similarity and relevance_score >= (self.memory_confidence_threshold * 0.8):
            logger.info(f"Found match for repeated question with same entity ({relevance_score:.2f}): {top_memory.get('user_query')}")
            return True, top_memory
            
        # Log the decision for debugging
        if relevance_score >= 0.5:
            logger.debug(f"Memory match below threshold ({relevance_score:.2f} < {self.memory_confidence_threshold}): {top_memory.get('user_query')}")
            
        return False, None
    
    def decide_query_path(self, query: str, memory_results: List[Dict], 
                         conversation_history: List[Dict] = None) -> Tuple[Literal["memory", "pipeline", "general"], Optional[Dict]]:
        """
        Decide the appropriate query handling path.
        
        Args:
            query: The user's question
            memory_results: List of relevant memory entries sorted by relevance
            conversation_history: Optional list of recent conversation history
            
        Returns:
            Tuple of (decision, memory_entry)
        """
        # Check if we should use memory directly
        use_memory, memory_entry = self.should_use_memory(query, memory_results, conversation_history)
        if use_memory:
            return "memory", memory_entry
        
        # Check if this is a general question not requiring database query
        if self.is_general_question(query):
            return "general", None
        
        # Default to using the full pipeline
        return "pipeline", None

    def prepare_context_from_memory(self, memory_results: List[Dict], limit: int = 3) -> str:
        """
        Prepare context from memory results to enhance other paths.
        
        Args:
            memory_results: List of relevant memory entries
            limit: Maximum number of memory entries to include
            
        Returns:
            str: Formatted context from memory
        """
        if not memory_results:
            return ""
        
        context_parts = []
        for i, memory in enumerate(memory_results[:limit]):
            if i >= limit:
                break
                
            user_query = memory.get("user_query", "")
            answer = memory.get("answer", "")
            
            if user_query and answer:
                context_parts.append(f"Previous Q: {user_query}")
                context_parts.append(f"Previous A: {answer}")
                context_parts.append("")  # Empty line for spacing
        
        return "\n".join(context_parts)
    
    def _is_likely_followup(self, query: str, conversation_history: List[Dict] = None) -> bool:
        """
        Determine if a query is likely a follow-up question.
        
        Args:
            query: The user's question
            conversation_history: Recent conversation history
            
        Returns:
            bool: True if likely a follow-up question
        """
        query = query.lower()
        
        # Check for direct indicators of follow-up questions
        followup_indicators = ["it", "this", "that", "these", "those", "the compound", 
                              "its", "about it", "for it", "the same", "as well", "too"]
        
        # Check if query starts with certain patterns
        followup_starters = ["what about", "how about", "what is its", "what's its", 
                           "and what", "and how", "can you also", "also"]
        
        # Check for direct indicators
        if any(indicator in query for indicator in followup_indicators):
            return True
            
        # Check for starter patterns
        if any(query.startswith(starter) for starter in followup_starters):
            return True
            
        # Check if query is very short (likely a follow-up)
        if len(query.split()) <= 4:
            return True
            
        return False 