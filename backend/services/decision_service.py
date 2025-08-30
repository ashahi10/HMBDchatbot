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
    
    # List of entities or concepts that should always trigger the database pipeline
    REQUIRE_QUERY_ENTITIES = {
        # Basic metabolomics terms
        "metabolite", "metabolites", "compound", "compounds", "molecule", "molecules",
        # Common metabolites
        "glucose", "fructose", "citric acid", "pyruvate", "lactate", "alanine", "glycine",
        "dopamine", "serotonin", "acetylcholine", "creatine", "urea", "cholesterol",
        # Chemical identifiers and databases
        "hmdb", "inchi", "inchikey", "smiles", "kegg", "pubchem", "chebi", "drugbank",
        # Pathway terms
        "pathway", "pathways", "metabolic pathway", "biochemical pathway", 
        # Molecular properties
        "molecular weight", "chemical formula", "molecular formula", "structure",
        # Biospecimen terms
        "concentration", "serum", "plasma", "urine", "csf", "saliva", "tissue",
        # Biological entities
        "enzyme", "protein", "gene", "receptor", "transporter",
        # Analytical terms
        "spectrum", "spectra", "mass spec", "nmr", "ms/ms", "chromatography"
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
        
        PRIORITY: Database pipeline should be triggered for ANY question that:
        1. Contains specific metabolites, compounds, or HMDB IDs
        2. Asks for specific molecular properties, structures, or data
        3. Contains database-relevant entities or identifiers
        
        Only truly general conceptual questions should be handled by general pipeline.
        
        Args:
            query: The user's question
            
        Returns:
            bool: True if it appears to be a general question
        """
        query_original = query
        query = query.lower().strip()
        
        # STEP 1: Immediate database triggers - if any of these are present, use database pipeline
        immediate_db_triggers = [
            # Specific lookup phrases
            "show me", "look up", "find", "search for", "get", "retrieve",
            # Molecular properties
            "structure of", "formula for", "molecular weight of", "properties of", 
            "molecular formula", "chemical formula", "smiles", "inchi", "inchikey",
            # Database identifiers  
            "hmdb", "pubchem", "chebi", "kegg", "drugbank", "id for",
            # Concentration and biospecimen data
            "concentration", "levels in", "found in", "present in", "biospecimen",
            # Pathway information
            "pathway", "pathways", "involved in", "participates in", "metabolic pathway",
            # Comparison queries
            "compare", "difference between", "vs", "versus", "relationship between",
            # Spectrum-related
            "spectrum", "spectra", "ms", "nmr", "mass spec",
        ]
        
        # If query contains any immediate DB triggers, use database pipeline
        for trigger in immediate_db_triggers:
            if trigger in query:
                logger.debug(f"Query contains DB trigger '{trigger}': {query_original}")
                return False
        
        # STEP 2: Check for specific entities, compounds, or identifiers
        # Expanded entity patterns for better detection
        entity_patterns = [
            # HMDB IDs
            r'\bhmdb\d+\b',
            # Chemical formulas (like C6H12O6, CH3CH2OH)
            r'\b[A-Z][a-z]?[0-9]*(?:[A-Z][a-z]?[0-9]*)*\b',
            # InChIKeys
            r'\b[A-Z]{14}-[A-Z]{10}-[A-Z]\b',
            # Specific metabolite names (capitalized compounds)
            r'\b[A-Z][a-z]+(?:\s+[A-Z]?[a-z]+)*\s+(?:acid|amine|alcohol|sugar|glucose|fructose)\b',
            # D- prefixed compounds (like D-Glucose, D-Fructose)
            r'\b[DL]-[A-Z][a-z]+\b',
            # Numbers that could be concentrations or measurements
            r'\b\d+\.?\d*\s*(?:mg|μg|ng|ml|μl|mm|μm|nm)\b',
        ]
        
        # Check for entity patterns
        for pattern in entity_patterns:
            if re.search(pattern, query):
                logger.debug(f"Query contains entity pattern '{pattern}': {query_original}")
                return False
        
        # STEP 3: Check for entities from the require query list
        for entity in self.REQUIRE_QUERY_ENTITIES:
            if entity in query:
                # Only allow general if it's VERY clearly conceptual/educational
                very_general_contexts = [
                    f"what is {entity} in general",
                    f"explain the concept of {entity}",
                    f"what does {entity} mean",
                    f"define {entity}",
                ]
                
                # Check if it matches very general patterns
                is_very_general = any(context in query for context in very_general_contexts)
                
                if not is_very_general:
                    logger.debug(f"Query contains DB entity '{entity}' without general context: {query_original}")
                    return False
        
        # STEP 4: Only NOW check for general question patterns
        # But be much more restrictive
        
        # Truly casual conversation patterns (these should be general)
        casual_patterns = [
            r"^(?:hi|hello|hey|greetings|howdy)[\s!.?]*$",
            r"^(?:how\s+are\s+you|what's\s+up|how's\s+it\s+going)[\s!.?]*$",
            r"^(?:thank|thanks|thank\s+you)[\s!.?]*$",
            r"^(?:bye|goodbye|see\s+you)[\s!.?]*$",
        ]
        
        for pattern in casual_patterns:
            if re.match(pattern, query):
                logger.debug(f"Query is casual conversation: {query_original}")
                return True
        
        # Pure methodology or technology questions (without specific compounds)
        methodology_patterns = [
            r"^how\s+does\s+(?:mass\s+spectrometry|nmr|lcms|gcms|hplc)\s+work\s*\??\s*$",
            r"^what\s+is\s+(?:metabolomics|proteomics|genomics|bioinformatics)\s*\??\s*$",
            r"^explain\s+(?:how\s+)?(?:mass\s+spectrometry|nmr|chromatography)\s+works?\s*\??\s*$",
            r"^(?:what|how)\s+(?:is|are|does)\s+(?:the\s+)?(?:principle|process|method)\s+of\s+(?:mass\s+spec|nmr|hplc)\s*\??\s*$",
            r"^define\s+(?:metabolomics|proteomics|genomics|bioinformatics)\s*\??\s*$",
            r"^what\s+is\s+the\s+principle\s+of\s+chromatography\s*\??\s*$",
        ]
        
        for pattern in methodology_patterns:
            if re.match(pattern, query):
                logger.debug(f"Query is pure methodology question: {query_original}")
                return True
        
        # Very broad conceptual questions without specific entities
        broad_conceptual_patterns = [
            r"^how\s+does\s+metabolism\s+work\s+in\s+(?:general|humans|cells)$",
            r"^what\s+is\s+metabolism\s+in\s+(?:general|humans|biology)$",
            r"^explain\s+(?:the\s+concept\s+of\s+)?metabolism\s+in\s+general$",
        ]
        
        for pattern in broad_conceptual_patterns:
            if re.match(pattern, query):
                logger.debug(f"Query is broad conceptual question: {query_original}")
                return True
        
        # STEP 5: Default to database pipeline for safety
        # If we can't confidently classify it as general, use database pipeline
        logger.debug(f"Query defaulting to database pipeline for safety: {query_original}")
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
        
        # Extract the latest user query from the first memory result to analyze it
        latest_query = ""
        if memory_results and len(memory_results) > 0:
            latest_query = memory_results[0].get("user_query", "").lower()
        
        # Don't include memory context for casual conversations or simple greetings
        casual_patterns = [
            r"^(?:hi|hello|hey|greetings|howdy)[\s!.?]*$",
            r"^(?:how\s+are\s+you|what's\s+up|how's\s+it\s+going)[\s!.?]*$",
            r"^(?:thank|thanks)[\s!.?]*$",
            r"^(?:bye|goodbye|see\s+you)[\s!.?]*$"
        ]
        
        # Check if the query matches any casual pattern
        if latest_query and any(re.match(pattern, latest_query) for pattern in casual_patterns):
            return ""  # Return empty context for casual conversations
        
        # For very short queries (less than 5 words), don't include memory context
        if latest_query and len(latest_query.split()) < 5:
            # Check if it's a simple question not requiring context
            simple_patterns = [
                r"^what\s+is[\s\w]*\?*$",
                r"^who\s+are[\s\w]*\?*$",
                r"^where\s+is[\s\w]*\?*$",
                r"^how\s+do[\s\w]*\?*$"
            ]
            if any(re.match(pattern, latest_query) for pattern in simple_patterns):
                return ""  # Return empty context for simple questions
                
        # For other queries, prepare context as before
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