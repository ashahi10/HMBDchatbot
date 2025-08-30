"""
Spectra Trigger Detection Service

This module provides intelligent detection of spectra-related commands and handles
HMDB ID extraction with multiple fallback strategies and AI-powered entity recognition.

Author: Senior Engineering Implementation
Version: 2.0.0 - Enhanced with AI Entity Recognition
"""

import re
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import logging
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

class SpectraTriggerType(Enum):
    """Types of spectra triggers detected"""
    EXPLICIT_COMMAND = "explicit_command"  # "show spectra", "spectrum of", etc.
    HMDB_ID_WITH_SPECTRA = "hmdb_id_with_spectra"  # "HMDB0000001 spectra"
    METABOLITE_NAME_WITH_SPECTRA = "metabolite_name_with_spectra"  # "glucose spectra"
    BUTTON_TRIGGER = "button_trigger"  # UI button "generate spectra"
    IMPLICIT_SCIENTIFIC = "implicit_scientific"  # "mass spectrum analysis"
    INTELLIGENT_ENTITY = "intelligent_entity"  # AI-detected entity request
    SPECTRUM_COMPARISON = "spectrum_comparison"  # "compare spectra of glucose and fructose"

@dataclass
class SpectraIntent:
    """
    Data structure for detected spectra intent
    
    Attributes:
        trigger_type: Type of trigger detected
        confidence: Confidence score (0-1)
        hmdb_id: Extracted HMDB ID if found
        metabolite_name: Extracted metabolite name if found
        metabolite_names: List of metabolite names for comparison queries
        hmdb_ids: List of HMDB IDs for comparison queries
        comparison_type: Type of comparison ("compare", "difference", "shift", etc.)
        is_comparison: Whether this is a multi-metabolite comparison query
        original_query: Original user query
        trigger_phrase: The specific phrase that triggered detection
        requires_disambiguation: Whether user confirmation is needed
        entity_candidates: List of potential entity matches for disambiguation
    """
    trigger_type: SpectraTriggerType
    confidence: float
    hmdb_id: Optional[str] = None
    metabolite_name: Optional[str] = None
    metabolite_names: List[str] = None
    hmdb_ids: List[str] = None
    comparison_type: Optional[str] = None
    is_comparison: bool = False
    original_query: str = ""
    trigger_phrase: str = ""
    requires_disambiguation: bool = False
    entity_candidates: List[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Initialize list fields if None"""
        if self.metabolite_names is None:
            self.metabolite_names = []
        if self.hmdb_ids is None:
            self.hmdb_ids = []

class IntelligentSpectraTriggerDetector:
    """Enhanced detector with AI-powered entity recognition and comprehensive cleaning"""
    
    # MINIMUM CONFIDENCE THRESHOLD - patterns below this are ignored
    MIN_CONFIDENCE_THRESHOLD = 0.75
    
    # Comprehensive trigger patterns with confidence scores
    # Order matters! More specific patterns should come first
    TRIGGER_PATTERNS = [
        # COMPARISON PATTERNS (HIGHEST PRIORITY - most specific)
        # Compare spectrum of X and Y / X vs Y
        (r'\bcompare\s+(?:the\s+)?spectr(?:a|um)\s+(?:of|between)\s+(.+?)\s+(?:and|vs|versus|with)\s+(.+?)(?:\s|$)', 0.95, SpectraTriggerType.SPECTRUM_COMPARISON),
        (r'\bcompare\s+(?:the\s+)?(?:mass\s+)?spectr(?:a|um)\s+(?:data\s+)?(?:of|between)\s+(.+?)\s+(?:and|vs|versus|with)\s+(.+?)(?:\s|$)', 0.95, SpectraTriggerType.SPECTRUM_COMPARISON),
        
        # Difference/shift patterns
        (r'\b(?:difference|differences)\s+(?:in|between)\s+(?:the\s+)?(?:mass\s+)?spectr(?:a|um)\s+(?:of|between)\s+(.+?)\s+(?:and|vs|versus)\s+(.+?)(?:\s|$)', 0.90, SpectraTriggerType.SPECTRUM_COMPARISON),
        (r'\bspectr(?:a|um)\s+(?:difference|differences)\s+(?:in|between)\s+(.+?)\s+(?:and|vs|versus)\s+(.+?)(?:\s|$)', 0.90, SpectraTriggerType.SPECTRUM_COMPARISON),
        (r'\b(?:chemical\s+shift|shift)\s+(?:difference|differences)\s+(?:in|between)\s+(.+?)\s+(?:and|vs|versus)\s+(.+?)(?:\s|$)', 0.90, SpectraTriggerType.SPECTRUM_COMPARISON),
        (r'\b(?:NMR|MS|mass\s+spec)\s+(?:difference|differences)\s+(?:between|of)\s+(.+?)\s+(?:and|vs|versus)\s+(.+?)(?:\s|$)', 0.90, SpectraTriggerType.SPECTRUM_COMPARISON),
        
        # X vs Y spectrum patterns
        (r'\b(.+?)\s+(?:vs|versus)\s+(.+?)\s+(?:mass\s+)?spectr(?:a|um)(?:\s|$)', 0.85, SpectraTriggerType.SPECTRUM_COMPARISON),
        (r'\b(?:mass\s+)?spectr(?:a|um)\s+(?:of\s+)?(.+?)\s+(?:vs|versus)\s+(.+?)(?:\s|$)', 0.85, SpectraTriggerType.SPECTRUM_COMPARISON),
        
        # Multiple HMDB IDs
        (r'\bspectr(?:a|um)\s+(?:of|for)\s+(HMDB\d+)\s+(?:and|vs|versus|with)\s+(HMDB\d+)', 0.95, SpectraTriggerType.SPECTRUM_COMPARISON),
        (r'\bcompare\s+(HMDB\d+)\s+(?:and|vs|versus|with)\s+(HMDB\d+)', 0.95, SpectraTriggerType.SPECTRUM_COMPARISON),
        
        # Button/command style patterns (HIGH PRIORITY)
        (r'\bgenerate\s+spectr(?:a|um)\s*$', 0.95, SpectraTriggerType.BUTTON_TRIGGER),
        (r'\bgenerate\s+spectr(?:a|um)\s+(?:of|for)\s+(HMDB\d+)', 0.95, SpectraTriggerType.BUTTON_TRIGGER),
        (r'\bgenerate\s+spectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.90, SpectraTriggerType.BUTTON_TRIGGER),
        (r'\bspectr(?:a|um)\s+generation(?:\s+(.+))?', 0.85, SpectraTriggerType.BUTTON_TRIGGER),
        
        # HMDB ID patterns (high priority but after comparison and button commands)
        (r'\b(HMDB\d+)\s+spectr(?:a|um)', 0.95, SpectraTriggerType.HMDB_ID_WITH_SPECTRA),
        (r'\bspectr(?:a|um)\s+(?:of|for)\s+(HMDB\d+)', 0.90, SpectraTriggerType.HMDB_ID_WITH_SPECTRA),
        
        # Scientific analysis terms (specific instruments)
        (r'\b(?:mass\s+spectr(?:a|um)|MS|GC-MS|LC-MS)\s+(?:of|for|analysis)\s+(.+)', 0.90, SpectraTriggerType.IMPLICIT_SCIENTIFIC),
        (r'\b(?:analyze|analysis)\s+(?:the\s+)?(?:mass\s+)?spectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.85, SpectraTriggerType.IMPLICIT_SCIENTIFIC),
        (r'\b(?:show|give)\s+me\s+(?:the\s+)?(?:GC-MS|LC-MS|MS)\s+(?:of|for)\s+(.+)', 0.90, SpectraTriggerType.IMPLICIT_SCIENTIFIC),
        
        # Enhanced: Handle "need/want/require spectrum data for X" type queries
        (r'\b(?:need|want|require)\s+spectr(?:a|um)\s+(?:data\s+)?(?:of|for)\s+(.+)', 0.90, SpectraTriggerType.EXPLICIT_COMMAND),
        (r'\b(?:need|want|require)\s+(?:the\s+)?spectr(?:a|um)\s+(?:data\s+)?(?:of|for)\s+(.+)', 0.90, SpectraTriggerType.EXPLICIT_COMMAND),
        (r'\b(?:can\s+you\s+)?(?:get|find|search)\s+(?:the\s+)?spectr(?:a|um)\s+(?:data\s+)?(?:of|for)\s+(.+)', 0.85, SpectraTriggerType.EXPLICIT_COMMAND),
        
        # High confidence explicit commands
        (r'\b(?:show|display|get|fetch)\s+(?:the\s+)?spectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.95, SpectraTriggerType.EXPLICIT_COMMAND),
        (r'\bspectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.90, SpectraTriggerType.EXPLICIT_COMMAND),
        (r'\b(?:give|show)\s+me\s+(?:the\s+)?spectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.95, SpectraTriggerType.EXPLICIT_COMMAND),
        
        # Enhanced: Direct entity requests without explicit spectrum mentions
        (r'\b(?:what\s+(?:is|are)\s+the\s+)?(?:mass\s+spec|spectr(?:a|um))\s+(?:data\s+)?(?:of|for)\s+(.+)', 0.85, SpectraTriggerType.EXPLICIT_COMMAND),
        (r'\b(?:analyze|examine|study)\s+(.+)\s+(?:using\s+)?(?:mass\s+spec|spectr(?:a|um))', 0.80, SpectraTriggerType.IMPLICIT_SCIENTIFIC),
        
        # Direct spectrum requests (metabolite + spectrum) - ONLY for clear metabolite names
        (r'\b(?!(?:infrared|UV|visible|IR|NMR|mass|nuclear|magnetic))\b(\w+(?:\s+\w+)?)\s+spectr(?:a|um)$', 0.85, SpectraTriggerType.METABOLITE_NAME_WITH_SPECTRA),
        
        # Generic spectrum with something (HIGHER THRESHOLD - must be more specific)
        (r'\bspectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.85, SpectraTriggerType.METABOLITE_NAME_WITH_SPECTRA),
        
        # FIXED: Only trigger on spectrum-specific entity requests (much more restrictive)
        (r'\b(?:what\s+(?:is|are)\s+the\s+)?(?:mass\s+)?spectr(?:a|um)\s+(?:data\s+)?(?:of|for)\s+(.+)', 0.85, SpectraTriggerType.INTELLIGENT_ENTITY),
        (r'\b(?:find|get|show)\s+(?:the\s+)?spectr(?:a|um)\s+(?:data\s+)?(?:of|for)\s+(.+)', 0.80, SpectraTriggerType.INTELLIGENT_ENTITY),
        
        # Single word "spectrum" (MUCH HIGHER THRESHOLD - avoid false triggers)
        (r'^\s*spectr(?:a|um)\s*$', 0.90, SpectraTriggerType.METABOLITE_NAME_WITH_SPECTRA),
    ]
    
    # HMDB ID extraction patterns
    HMDB_PATTERNS = [
        r'\b(HMDB\d{7})\b',  # Standard format HMDB0000001
        r'\b(HMDB\d+)\b',    # Any HMDB with digits
    ]
    
    # Enhanced metabolite name patterns for better entity recognition
    METABOLITE_PATTERNS = [
        r'\b([A-Z][a-z]+(?:\s+[a-z]+)*)\b',  # Capitalized names like "Glucose"
        r'\b([a-z]+-[A-Z][a-z]+)\b',         # Names like "D-Glucose"
        r'\b([A-Z]+)\b',                     # Acronyms like "ATP"
        r'\b([A-Z][a-z]*[a-z]+)\b',         # Single capitalized words
        r'\b([a-z]+(?:ine|ose|ate|ide|ene|ole|one|iol|acid))\b',  # Common chemical suffixes
    ]
    
    def __init__(self, neo4j_connection=None):
        """
        Initialize the enhanced trigger detector
        
        Args:
            neo4j_connection: Optional Neo4j connection for metabolite name validation
        """
        self.neo4j_connection = neo4j_connection
        
    def detect_spectra_intent(self, query: str) -> Optional[SpectraIntent]:
        """
        Detect if a query contains spectra-related intent with enhanced AI recognition
        
        Args:
            query: User query string
            
        Returns:
            SpectraIntent object if spectra intent detected, None otherwise
        """
        if not query or not isinstance(query, str):
            return None
        
        query_lower = query.lower().strip()
        
        # First pass: Check explicit spectrum trigger patterns
        for pattern, confidence, trigger_type in self.TRIGGER_PATTERNS:
            match = re.search(pattern, query_lower, re.IGNORECASE)
            if match and confidence >= self.MIN_CONFIDENCE_THRESHOLD:
                logger.info(f"Spectra trigger detected: {trigger_type.value} with confidence {confidence}")
                
                # Handle comparison patterns (multiple captured groups)
                if trigger_type == SpectraTriggerType.SPECTRUM_COMPARISON and len(match.groups()) >= 2:
                    entity1 = match.group(1).strip()
                    entity2 = match.group(2).strip()
                    
                    # Determine comparison type from the pattern
                    if "difference" in query_lower or "shift" in query_lower:
                        comparison_type = "difference"
                    elif "vs" in query_lower or "versus" in query_lower:
                        comparison_type = "versus"
                    else:
                        comparison_type = "compare"
                    
                    # Check if entities are HMDB IDs or metabolite names
                    hmdb_pattern = r'HMDB\d+'
                    hmdb_ids = []
                    metabolite_names = []
                    
                    if re.match(hmdb_pattern, entity1, re.IGNORECASE) and re.match(hmdb_pattern, entity2, re.IGNORECASE):
                        hmdb_ids = [entity1.upper(), entity2.upper()]
                    else:
                        # Clean metabolite names
                        cleaned1 = self._intelligent_clean_metabolite_name(entity1)
                        cleaned2 = self._intelligent_clean_metabolite_name(entity2)
                        if cleaned1 and cleaned2:
                            metabolite_names = [cleaned1, cleaned2]
                        elif cleaned1 or cleaned2:
                            # At least one valid metabolite found
                            metabolite_names = [name for name in [cleaned1, cleaned2] if name]
                    
                    return SpectraIntent(
                        trigger_type=trigger_type,
                        confidence=confidence,
                        hmdb_ids=hmdb_ids,
                        metabolite_names=metabolite_names,
                        comparison_type=comparison_type,
                        is_comparison=True,
                        original_query=query,
                        trigger_phrase=match.group(0),
                        requires_disambiguation=len(metabolite_names) > 0,
                        entity_candidates=[]
                    )
                
                # Handle single entity patterns (existing logic)
                else:
                    # Extract captured group (metabolite name or ID)
                    captured_text = match.group(1) if match.groups() else ""
                    
                    # Try to extract HMDB ID
                    hmdb_id = self._extract_hmdb_id(query)
                    
                    # Extract metabolite name if no HMDB ID found
                    metabolite_name = None
                    if not hmdb_id and captured_text:
                        metabolite_name = self._intelligent_clean_metabolite_name(captured_text)
                    
                    # For intelligent entity detection, perform database lookup
                    entity_candidates = []
                    if trigger_type == SpectraTriggerType.INTELLIGENT_ENTITY and metabolite_name:
                        entity_candidates = self._find_entity_candidates(metabolite_name)
                        # If we found good candidates, upgrade to explicit command
                        if entity_candidates and len(entity_candidates) > 0:
                            trigger_type = SpectraTriggerType.EXPLICIT_COMMAND
                            confidence = min(confidence + 0.2, 0.95)
                    
                    return SpectraIntent(
                        trigger_type=trigger_type,
                        confidence=confidence,
                        hmdb_id=hmdb_id,
                        metabolite_name=metabolite_name,
                        original_query=query,
                        trigger_phrase=match.group(0),
                        requires_disambiguation=not hmdb_id and bool(metabolite_name),
                        entity_candidates=entity_candidates
                    )
        
        # REMOVED: Overly aggressive second pass that converted ANY entity mention to spectrum request
        # This was causing false positives where questions like "what is molecular weight of X" 
        # were being treated as spectrum requests just because X was a metabolite
        
        return None
    
    def _extract_hmdb_id(self, text: str) -> Optional[str]:
        """
        Extract HMDB ID from text using multiple patterns
        
        Args:
            text: Input text
            
        Returns:
            HMDB ID string or None if not found
        """
        for pattern in self.HMDB_PATTERNS:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                hmdb_id = match.group(1).upper()
                logger.info(f"Extracted HMDB ID: {hmdb_id}")
                return hmdb_id
        return None
    
    def _intelligent_clean_metabolite_name(self, raw_name: str) -> str:
        """
        COMPLETELY BULLETPROOF metabolite name cleaning with comprehensive coverage
        
        Args:
            raw_name: Raw extracted name
            
        Returns:
            Cleaned metabolite name
        """
        if not raw_name:
            return ""
        
        # COMPREHENSIVE stopwords covering all possible query variations
        comprehensive_stopwords = {
            # Spectrum-related
            'spectrum', 'spectra', 'spectral', 'spec', 'ms', 'nmr', 'ir', 'uv',
            'mass', 'nuclear', 'magnetic', 'resonance', 'infrared', 'ultraviolet',
            
            # # Data/info related
            # 'data', 'information', 'info', 'details', 'results', 'analysis', 'profile',
            # 'properties', 'characteristics', 'features', 'structure', 'composition',
            
            # # Action words
            # 'show', 'give', 'get', 'find', 'search', 'fetch', 'display', 'retrieve',
            # 'obtain', 'acquire', 'lookup', 'examine', 'analyze', 'study', 'investigate',
            # 'need', 'want', 'require', 'looking', 'seeking', 'requesting',
            
            # # Common connectors and particles
            # 'me', 'the', 'of', 'for', 'about', 'on', 'with', 'from', 'to', 'in', 'at',
            # 'a', 'an', 'and', 'or', 'but', 'by', 'is', 'are', 'was', 'were', 'be',
            # 'what', 'how', 'where', 'when', 'why', 'which', 'who', 'whose',
            
            # # Question words and phrases
            # 'can', 'could', 'would', 'should', 'may', 'might', 'will', 'shall',
            # 'you', 'please', 'help', 'tell', 'explain', 'describe',
            
            # # Generic descriptors
            # 'using', 'through', 'via', 'method', 'technique', 'approach', 'way'
        }
        
        # Step 1: Basic cleaning
        words = raw_name.strip().split()
        primary_cleaned = []
        
        for word in words:
            clean_word = word.lower().strip('.,!?;:"()[]{}')
            # Keep words that are substantial and not stopwords
            if (len(clean_word) > 1 and 
                clean_word not in comprehensive_stopwords and
                not clean_word.isdigit()):
                primary_cleaned.append(word)
        
        primary_result = ' '.join(primary_cleaned).strip()
        
        # Step 2: If primary cleaning worked, return it
        if primary_result and len(primary_result) >= 3:
            logger.info(f"Primary cleaning: '{raw_name}' -> '{primary_result}'")
            return primary_result
        
        # Step 3: Fallback - more conservative cleaning (only remove obvious spectrum words)
        minimal_stopwords = {
            'spectrum', 'spectra', 'data', 'information', 'analysis', 'show', 'me', 'the'
        }
        
        fallback_cleaned = []
        for word in words:
            clean_word = word.lower().strip('.,!?;:"()[]{}')
            if clean_word not in minimal_stopwords and len(word) > 1:
                fallback_cleaned.append(word)
        
        fallback_result = ' '.join(fallback_cleaned).strip()
        
        if fallback_result and len(fallback_result) >= 3:
            logger.info(f"Fallback cleaning: '{raw_name}' -> '{fallback_result}'")
            return fallback_result
        
        # Step 4: Pattern-based extraction for compound names
        potential_compounds = []
        for word in words:
            # Look for words that match chemical naming patterns
            if (len(word) > 3 and
                (re.match(r'^[A-Z][a-z]*$', word) or  # Capitalized
                 re.match(r'^[a-z]+(?:ine|ose|ate|ide|ene|ole|one|iol|acid)$', word) or  # Chemical suffixes
                 re.match(r'^[A-Z]+$', word))):  # Acronyms
                potential_compounds.append(word)
        
        if potential_compounds:
            compound_result = ' '.join(potential_compounds).strip()
            logger.info(f"Pattern-based extraction: '{raw_name}' -> '{compound_result}'")
            return compound_result
        
        # Step 5: Last resort - return the longest meaningful word
        longest_word = ""
        for word in words:
            clean_word = word.strip('.,!?;:"()[]{}')
            if len(clean_word) > len(longest_word) and len(clean_word) > 2:
                longest_word = clean_word
        
        if longest_word:
            logger.info(f"Longest word extraction: '{raw_name}' -> '{longest_word}'")
            return longest_word
        
        # Absolute final fallback
        logger.warning(f"All cleaning methods failed for: '{raw_name}', returning original")
        return raw_name.strip()
    
    def _extract_potential_entities(self, query: str) -> List[str]:
        """
        Extract potential metabolite entities from query using pattern matching
        
        Args:
            query: User query
            
        Returns:
            List of potential entity names
        """
        entities = []
        
        # Look for patterns that suggest metabolite names
        for pattern in self.METABOLITE_PATTERNS:
            matches = re.findall(pattern, query)
            for match in matches:
                cleaned = self._intelligent_clean_metabolite_name(match)
                if cleaned and len(cleaned) > 2:
                    entities.append(cleaned)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_entities = []
        for entity in entities:
            if entity.lower() not in seen:
                seen.add(entity.lower())
                unique_entities.append(entity)
        
        return unique_entities
    
    def _find_entity_candidates(self, entity_name: str) -> List[Dict[str, Any]]:
        """
        Find database candidates for an entity using fuzzy matching
        
        Args:
            entity_name: Entity name to search
            
        Returns:
            List of candidate matches
        """
        if not self.neo4j_connection or not entity_name:
            return []
        
        try:
            # Enhanced query with fuzzy matching
            query = """
            MATCH (m:Metabolite)
            WHERE toLower(m.name) CONTAINS toLower($name) 
               OR EXISTS { 
                   MATCH (m)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex) 
                   WHERE any(syn IN si.synonyms WHERE toLower(syn) CONTAINS toLower($name))
               }
            OPTIONAL MATCH (m)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex)
            RETURN m.accession as hmdb_id, m.name as name, si.synonyms as synonyms
            LIMIT 10
            """
            
            results = self.neo4j_connection.run_query(query, {"name": entity_name})
            
            candidates = []
            for record in results:
                similarity = self._calculate_fuzzy_similarity(entity_name, record["name"])
                candidates.append({
                    "hmdb_id": record["hmdb_id"],
                    "name": record["name"],
                    "synonyms": record["synonyms"] or [],
                    "confidence": similarity,
                    "match_type": "exact" if similarity > 0.9 else "fuzzy"
                })
            
            # Sort by confidence
            candidates.sort(key=lambda x: x["confidence"], reverse=True)
            
            logger.info(f"Found {len(candidates)} entity candidates for '{entity_name}'")
            return candidates
            
        except Exception as e:
            logger.error(f"Error finding entity candidates: {e}")
            return []
    
    def _calculate_fuzzy_similarity(self, query_name: str, db_name: str) -> float:
        """
        Calculate fuzzy similarity between query name and database name
        
        Args:
            query_name: User query name
            db_name: Database metabolite name
            
        Returns:
            Similarity score (0-1)
        """
        if not query_name or not db_name:
            return 0.0
        
        query_lower = query_name.lower()
        db_lower = db_name.lower()
        
        # Exact match
        if query_lower == db_lower:
            return 1.0
        
        # Contains match
        if query_lower in db_lower or db_lower in query_lower:
            return 0.9
        
        # Sequence matcher for fuzzy similarity
        similarity = SequenceMatcher(None, query_lower, db_lower).ratio()
        
        # Word overlap bonus
        query_words = set(query_lower.split())
        db_words = set(db_lower.split())
        
        if query_words and db_words:
            overlap = len(query_words.intersection(db_words))
            total = len(query_words.union(db_words))
            word_similarity = overlap / total if total > 0 else 0.0
            
            # Combine sequence and word similarities
            final_similarity = max(similarity, word_similarity)
        else:
            final_similarity = similarity
        
        return final_similarity

# Alias for backward compatibility
SpectraTriggerDetector = IntelligentSpectraTriggerDetector

class SpectraPipelineIntegrator:
    """Integration class for pipeline spectra functionality"""
    
    def __init__(self, neo4j_connection=None, hmdb_client=None):
        """
        Initialize the pipeline integrator
        
        Args:
            neo4j_connection: Neo4j connection for metabolite lookup
            hmdb_client: HMDB API client for spectra fetching
        """
        self.detector = SpectraTriggerDetector(neo4j_connection)
        self.hmdb_client = hmdb_client
        self.neo4j_connection = neo4j_connection
    
    def process_spectra_query(self, query: str) -> Dict[str, Any]:
        """
        Main entry point for processing spectra queries
        
        Args:
            query: User query
            
        Returns:
            Processing result with spectra data or disambiguation request
        """
        # Detect spectra intent
        intent = self.detector.detect_spectra_intent(query)
        
        if not intent:
            return {
                "is_spectra_query": False,
                "result": None
            }
        
        logger.info(f"Processing spectra query with intent: {intent.trigger_type.value}")
        
        # Handle comparison queries
        if intent.is_comparison:
            return self._process_comparison_query(intent)
        
        # Handle single metabolite queries (existing logic)
        # If we have an HMDB ID, fetch spectra directly
        if intent.hmdb_id:
            return self._fetch_spectra_by_hmdb_id(intent.hmdb_id, intent)
        
        # If we have a metabolite name, try to resolve it
        if intent.metabolite_name:
            matches = self.detector.find_metabolite_matches(intent.metabolite_name)
            
            if len(matches) == 1:
                # Single match found - proceed automatically regardless of confidence
                # (since there's only one option, it's likely correct)
                hmdb_id = matches[0]["hmdb_id"]
                logger.info(f"Auto-resolving to {hmdb_id} for '{intent.metabolite_name}' (single match)")
                return self._fetch_spectra_by_hmdb_id(hmdb_id, intent)
            elif len(matches) > 1 and matches[0]["confidence"] > 0.8:
                # Multiple matches but highest confidence is very high - auto-resolve
                hmdb_id = matches[0]["hmdb_id"]
                logger.info(f"Auto-resolving to {hmdb_id} for '{intent.metabolite_name}' (high confidence: {matches[0]['confidence']:.2f})")
                return self._fetch_spectra_by_hmdb_id(hmdb_id, intent)
            
            elif matches:
                # Multiple matches or low confidence - request disambiguation
                disambiguation_prompt = self.detector.create_disambiguation_prompt(intent, matches)
                return {
                    "is_spectra_query": True,
                    "requires_disambiguation": True,
                    "disambiguation_prompt": disambiguation_prompt,
                    "potential_matches": matches,
                    "original_intent": intent
                }
            
            else:
                # No matches found
                return {
                    "is_spectra_query": True,
                    "error": f"No metabolites found matching '{intent.metabolite_name}'. Please provide a specific HMDB ID or try a different name.",
                    "original_intent": intent
                }
        
        # No actionable information found
        return {
            "is_spectra_query": True,
            "error": "I detected a spectra request but couldn't identify the specific metabolite. Please provide an HMDB ID or metabolite name.",
            "original_intent": intent
        }
    
    def _fetch_spectra_by_hmdb_id(self, hmdb_id: str, intent: SpectraIntent) -> Dict[str, Any]:
        """
        Fetch spectra data for a specific HMDB ID
        
        Args:
            hmdb_id: HMDB identifier
            intent: Original spectra intent
            
        Returns:
            Spectra fetch result
        """
        if not self.hmdb_client:
            return {
                "is_spectra_query": True,
                "error": "HMDB API client not available",
                "original_intent": intent
            }
        
        try:
            # Use the new spectra method
            spectra_result = self.hmdb_client.get_all_spectra_for_hmdb_id(hmdb_id)
            
            # Check for timeout/connection errors and provide user-friendly messages
            if not spectra_result.get("success", False):
                error_type = spectra_result.get("error_type")
                errors = spectra_result.get("errors", [])
                
                if error_type == "timeout":
                    user_message = "🕐 HMDB server is busy. Please try again in some time."
                elif error_type == "connection":
                    user_message = "🔌 Unable to connect to HMDB server. Please try again later."
                elif error_type == "request":
                    user_message = "⚠️ HMDB server error. Please try again in some time."
                else:
                    # Generic error fallback
                    user_message = errors[0] if errors else "Unable to fetch spectrum data at this time."
                
                return {
                    "is_spectra_query": True,
                    "hmdb_id": hmdb_id,
                    "error": user_message,
                    "error_type": error_type,
                    "original_intent": intent,
                    "success": False
                }
            
            return {
                "is_spectra_query": True,
                "hmdb_id": hmdb_id,
                "spectra_data": spectra_result,
                "original_intent": intent,
                "success": spectra_result.get("success", False)
            }
            
        except Exception as e:
            logger.error(f"Error fetching spectra for {hmdb_id}: {e}")
            return {
                "is_spectra_query": True,
                "error": "🔧 Technical error occurred while fetching spectrum data. Please try again later.",
                "original_intent": intent,
                "success": False
            }
    
    def _process_comparison_query(self, intent: SpectraIntent) -> Dict[str, Any]:
        """
        Process comparison queries for multiple metabolites
        
        Args:
            intent: SpectraIntent with comparison data
            
        Returns:
            Comparison processing result
        """
        logger.info(f"Processing comparison query: {intent.comparison_type} for {len(intent.metabolite_names) + len(intent.hmdb_ids)} metabolites")
        
        # If we have HMDB IDs directly, fetch spectra
        if intent.hmdb_ids and len(intent.hmdb_ids) >= 2:
            return self._fetch_multiple_spectra_by_hmdb_ids(intent.hmdb_ids, intent)
        
        # If we have metabolite names, resolve them to HMDB IDs
        if intent.metabolite_names and len(intent.metabolite_names) >= 2:
            return self._resolve_and_fetch_multiple_spectra(intent.metabolite_names, intent)
        
        # Handle mixed or insufficient data
        if (len(intent.metabolite_names) + len(intent.hmdb_ids)) < 2:
            return {
                "is_spectra_query": True,
                "error": f"Comparison requires at least 2 metabolites. Found {len(intent.metabolite_names) + len(intent.hmdb_ids)} metabolites.",
                "original_intent": intent
            }
        
        # Mixed case: some HMDB IDs, some names
        resolved_hmdb_ids = list(intent.hmdb_ids) if intent.hmdb_ids else []
        
        for metabolite_name in intent.metabolite_names:
            matches = self.detector.find_metabolite_matches(metabolite_name)
            if len(matches) == 1:
                resolved_hmdb_ids.append(matches[0]["hmdb_id"])
            elif len(matches) > 1 and matches[0]["confidence"] > 0.8:
                resolved_hmdb_ids.append(matches[0]["hmdb_id"])
            else:
                # Need disambiguation for this metabolite
                return {
                    "is_spectra_query": True,
                    "requires_disambiguation": True,
                    "disambiguation_prompt": f"Multiple matches found for '{metabolite_name}' in your comparison. Please specify which metabolite you meant:",
                    "potential_matches": matches,
                    "original_intent": intent
                }
        
        if len(resolved_hmdb_ids) >= 2:
            return self._fetch_multiple_spectra_by_hmdb_ids(resolved_hmdb_ids, intent)
        else:
            return {
                "is_spectra_query": True,
                "error": "Could not resolve enough metabolites for comparison. Please provide more specific names or HMDB IDs.",
                "original_intent": intent
            }
    
    def _fetch_multiple_spectra_by_hmdb_ids(self, hmdb_ids: List[str], intent: SpectraIntent) -> Dict[str, Any]:
        """
        Fetch spectra for multiple HMDB IDs in parallel
        
        Args:
            hmdb_ids: List of HMDB identifiers
            intent: Original spectra intent
            
        Returns:
            Multiple spectra fetch result
        """
        if not self.hmdb_client:
            return {
                "is_spectra_query": True,
                "error": "HMDB API client not available",
                "original_intent": intent
            }
        
        import concurrent.futures
        
        # Fetch spectra for all metabolites
        results = []
        errors = []
        
        # Use ThreadPoolExecutor for parallel requests
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(hmdb_ids), 4)) as executor:
            # Submit all requests
            future_to_hmdb_id = {
                executor.submit(self.hmdb_client.get_all_spectra_for_hmdb_id, hmdb_id): hmdb_id 
                for hmdb_id in hmdb_ids
            }
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_hmdb_id):
                hmdb_id = future_to_hmdb_id[future]
                try:
                    spectra_result = future.result(timeout=30)  # 30 second timeout per request
                    
                    if spectra_result.get("success", False):
                        results.append({
                            "hmdb_id": hmdb_id,
                            "spectra_data": spectra_result.get("data", {}),
                            "success": True
                        })
                    else:
                        errors.append({
                            "hmdb_id": hmdb_id,
                            "error": spectra_result.get("error", "Unknown error"),
                            "error_type": spectra_result.get("error_type", "unknown")
                        })
                        
                except Exception as e:
                    logger.error(f"Error fetching spectra for {hmdb_id}: {str(e)}")
                    errors.append({
                        "hmdb_id": hmdb_id,
                        "error": f"Failed to fetch spectra: {str(e)}",
                        "error_type": "processing_error"
                    })
        
        # Check if we have enough successful results for comparison
        if len(results) < 2:
            if errors:
                error_messages = [f"{err['hmdb_id']}: {err['error']}" for err in errors]
                return {
                    "is_spectra_query": True,
                    "error": f"Could not fetch enough spectra for comparison. Errors: {'; '.join(error_messages)}",
                    "original_intent": intent,
                    "errors": errors
                }
            else:
                return {
                    "is_spectra_query": True,
                    "error": "Not enough spectra data available for comparison.",
                    "original_intent": intent
                }
        
        # Process the successful results using the same pattern as single spectra
        return {
            "is_spectra_query": True,
            "is_comparison": True,
            "comparison_type": intent.comparison_type,
            "hmdb_ids": hmdb_ids,
            "success": True,
            "spectra_data": results,
            "comparison_count": len(results),
            "errors": errors if errors else None,
            "original_intent": intent
        }
    
    def _resolve_and_fetch_multiple_spectra(self, metabolite_names: List[str], intent: SpectraIntent) -> Dict[str, Any]:
        """
        Resolve metabolite names to HMDB IDs and fetch their spectra
        
        Args:
            metabolite_names: List of metabolite names
            intent: Original spectra intent
            
        Returns:
            Resolution and fetch result
        """
        resolved_hmdb_ids = []
        resolution_info = []
        
        for metabolite_name in metabolite_names:
            matches = self.detector.find_metabolite_matches(metabolite_name)
            
            if len(matches) == 1:
                # Single match - auto-resolve
                hmdb_id = matches[0]["hmdb_id"]
                resolved_hmdb_ids.append(hmdb_id)
                resolution_info.append({
                    "query_name": metabolite_name,
                    "resolved_name": matches[0]["name"],
                    "hmdb_id": hmdb_id,
                    "confidence": matches[0]["confidence"],
                    "auto_resolved": True
                })
                logger.info(f"Auto-resolved '{metabolite_name}' to {hmdb_id} (single match)")
                
            elif len(matches) > 1 and matches[0]["confidence"] > 0.8:
                # High confidence match - auto-resolve
                hmdb_id = matches[0]["hmdb_id"]
                resolved_hmdb_ids.append(hmdb_id)
                resolution_info.append({
                    "query_name": metabolite_name,
                    "resolved_name": matches[0]["name"],
                    "hmdb_id": hmdb_id,
                    "confidence": matches[0]["confidence"],
                    "auto_resolved": True
                })
                logger.info(f"Auto-resolved '{metabolite_name}' to {hmdb_id} (high confidence: {matches[0]['confidence']:.2f})")
                
            else:
                # Need disambiguation
                return {
                    "is_spectra_query": True,
                    "requires_disambiguation": True,
                    "disambiguation_prompt": self.detector.create_disambiguation_prompt(intent, matches),
                    "potential_matches": matches,
                    "original_intent": intent,
                    "problematic_metabolite": metabolite_name
                }
        
        if len(resolved_hmdb_ids) >= 2:
            # Fetch spectra for resolved IDs
            result = self._fetch_multiple_spectra_by_hmdb_ids(resolved_hmdb_ids, intent)
            # Add resolution information
            if result.get("success"):
                result["resolution_info"] = resolution_info
            return result
        else:
            return {
                "is_spectra_query": True,
                "error": "Could not resolve enough metabolites for comparison.",
                "original_intent": intent,
                "resolution_info": resolution_info
            }

# Add backward compatibility methods
def find_metabolite_matches(self, metabolite_name: str) -> List[Dict[str, Any]]:
    """
    Backward compatibility method - delegates to _find_entity_candidates
    
    Args:
        metabolite_name: Metabolite name to search
        
    Returns:
        List of potential matches with HMDB IDs
    """
    return self._find_entity_candidates(metabolite_name)

def create_disambiguation_prompt(self, intent: SpectraIntent, matches: List[Dict[str, Any]]) -> str:
    """
    Create an intelligent disambiguation prompt with enhanced context
    
    Args:
        intent: The detected spectra intent
        matches: List of potential metabolite matches
        
    Returns:
        Formatted disambiguation prompt
    """
    if not matches:
        return f"I couldn't find any metabolites matching '{intent.metabolite_name}'. Could you please provide a specific HMDB ID or try a different metabolite name?"
    
    if len(matches) == 1:
        match = matches[0]
        confidence_text = f"(confidence: {match['confidence']:.2f})" if 'confidence' in match else ""
        return f"I found one match for '{intent.metabolite_name}': **{match['name']}** ({match['hmdb_id']}) {confidence_text}. Should I generate the spectrum for this metabolite?"
    
    # Multiple matches - show intelligent ranking
    prompt = f"I found **{len(matches)}** metabolites matching '{intent.metabolite_name}':\n\n"
    
    for i, match in enumerate(matches[:5], 1):  # Show top 5
        confidence = match.get('confidence', 0)
        match_type = match.get('match_type', 'unknown')
        confidence_indicator = "🎯" if confidence > 0.9 else "🔍" if confidence > 0.7 else "💭"
        
        prompt += f"{i}. {confidence_indicator} **{match['name']}** ({match['hmdb_id']}) - {match_type} match ({confidence:.2f})\n"
    
    if len(matches) > 5:
        prompt += f"\n... and {len(matches) - 5} more matches.\n"
    
    prompt += "\n💡 **Please specify which metabolite you want the spectrum for:**\n"
    prompt += "- Reply with the number (e.g., '1' or '2')\n"
    prompt += "- Reply with the HMDB ID (e.g., 'HMDB0006026')\n" 
    prompt += "- Or provide a more specific metabolite name\n"
    
    return prompt

# Add the backward compatibility methods to the class
IntelligentSpectraTriggerDetector.find_metabolite_matches = find_metabolite_matches
IntelligentSpectraTriggerDetector.create_disambiguation_prompt = create_disambiguation_prompt