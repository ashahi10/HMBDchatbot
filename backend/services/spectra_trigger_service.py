"""
Spectra Trigger Detection Service

This module provides intelligent detection of spectra-related commands and handles
HMDB ID extraction with multiple fallback strategies.

Author: Senior Engineering Implementation
Version: 1.0.0
"""

import re
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class SpectraTriggerType(Enum):
    """Types of spectra triggers detected"""
    EXPLICIT_COMMAND = "explicit_command"  # "show spectra", "spectrum of", etc.
    HMDB_ID_WITH_SPECTRA = "hmdb_id_with_spectra"  # "HMDB0000001 spectra"
    METABOLITE_NAME_WITH_SPECTRA = "metabolite_name_with_spectra"  # "glucose spectra"
    BUTTON_TRIGGER = "button_trigger"  # UI button "generate spectra"
    IMPLICIT_SCIENTIFIC = "implicit_scientific"  # "mass spectrum analysis"

@dataclass
class SpectraIntent:
    """
    Data structure for detected spectra intent
    
    Attributes:
        trigger_type: Type of trigger detected
        confidence: Confidence score (0-1)
        hmdb_id: Extracted HMDB ID if found
        metabolite_name: Extracted metabolite name if found
        original_query: Original user query
        trigger_phrase: The specific phrase that triggered detection
        requires_disambiguation: Whether user confirmation is needed
    """
    trigger_type: SpectraTriggerType
    confidence: float
    hmdb_id: Optional[str] = None
    metabolite_name: Optional[str] = None
    original_query: str = ""
    trigger_phrase: str = ""
    requires_disambiguation: bool = False

class SpectraTriggerDetector:
    """Main class for detecting spectra-related intents in user queries"""
    
    # Comprehensive trigger patterns with confidence scores
    # Order matters! More specific patterns should come first
    TRIGGER_PATTERNS = [
        # Button/command style patterns (HIGHEST PRIORITY)
        (r'\bgenerate\s+spectr(?:a|um)\s*$', 0.95, SpectraTriggerType.BUTTON_TRIGGER),  # "generate spectrum" alone
        (r'\bgenerate\s+spectr(?:a|um)\s+(?:of|for)\s+(HMDB\d+)', 0.95, SpectraTriggerType.BUTTON_TRIGGER),
        (r'\bgenerate\s+spectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.90, SpectraTriggerType.BUTTON_TRIGGER),
        (r'\bspectr(?:a|um)\s+generation(?:\s+(.+))?', 0.85, SpectraTriggerType.BUTTON_TRIGGER),
        
        # HMDB ID patterns (high priority but after button commands)
        (r'\b(HMDB\d+)\s+spectr(?:a|um)', 0.95, SpectraTriggerType.HMDB_ID_WITH_SPECTRA),
        (r'\bspectr(?:a|um)\s+(?:of|for)\s+(HMDB\d+)', 0.90, SpectraTriggerType.HMDB_ID_WITH_SPECTRA),
        
        # Scientific analysis terms (specific instruments)
        (r'\b(?:mass\s+spectr(?:a|um)|MS|GC-MS|LC-MS)\s+(?:of|for|analysis)\s+(.+)', 0.90, SpectraTriggerType.IMPLICIT_SCIENTIFIC),
        (r'\b(?:analyze|analysis)\s+(?:the\s+)?(?:mass\s+)?spectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.85, SpectraTriggerType.IMPLICIT_SCIENTIFIC),
        (r'\b(?:show|give)\s+me\s+(?:the\s+)?(?:GC-MS|LC-MS|MS)\s+(?:of|for)\s+(.+)', 0.90, SpectraTriggerType.IMPLICIT_SCIENTIFIC),
        
        # High confidence explicit commands
        (r'\b(?:show|display|get|fetch)\s+(?:the\s+)?spectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.95, SpectraTriggerType.EXPLICIT_COMMAND),
        (r'\bspectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.90, SpectraTriggerType.EXPLICIT_COMMAND),
        (r'\b(?:give|show)\s+me\s+(?:the\s+)?spectr(?:a|um)\s+(?:of|for)\s+(.+)', 0.95, SpectraTriggerType.EXPLICIT_COMMAND),
        
        # Direct spectrum requests (metabolite + spectrum)
        (r'\b(.+)\s+spectr(?:a|um)$', 0.85, SpectraTriggerType.METABOLITE_NAME_WITH_SPECTRA),
        
        # Generic spectrum with something (lower priority)
        (r'\bspectr(?:a|um)\s+(.+)', 0.75, SpectraTriggerType.METABOLITE_NAME_WITH_SPECTRA),
        
        # Single word "spectrum" (lowest priority)
        (r'^\s*spectr(?:a|um)\s*$', 0.70, SpectraTriggerType.METABOLITE_NAME_WITH_SPECTRA),
    ]
    
    # HMDB ID extraction patterns
    HMDB_PATTERNS = [
        r'\b(HMDB\d{7})\b',  # Standard format HMDB0000001
        r'\b(HMDB\d+)\b',    # Any HMDB with digits
    ]
    
    # Common metabolite name patterns
    METABOLITE_PATTERNS = [
        r'\b([A-Z][a-z]+(?:\s+[a-z]+)*)\b',  # Capitalized names like "Glucose"
        r'\b([a-z]+-[A-Z][a-z]+)\b',         # Names like "D-Glucose"
        r'\b([A-Z]+)\b',                     # Acronyms like "ATP"
    ]
    
    def __init__(self, neo4j_connection=None):
        """
        Initialize the trigger detector
        
        Args:
            neo4j_connection: Optional Neo4j connection for metabolite name validation
        """
        self.neo4j_connection = neo4j_connection
        
    def detect_spectra_intent(self, query: str) -> Optional[SpectraIntent]:
        """
        Detect if a query contains spectra-related intent
        
        Args:
            query: User query string
            
        Returns:
            SpectraIntent object if spectra intent detected, None otherwise
        """
        if not query or not isinstance(query, str):
            return None
        
        query_lower = query.lower().strip()
        
        # Check each trigger pattern (order matters!)
        for pattern, confidence, trigger_type in self.TRIGGER_PATTERNS:
            match = re.search(pattern, query_lower, re.IGNORECASE)
            if match:
                logger.info(f"Spectra trigger detected: {trigger_type.value} with confidence {confidence}")
                
                # Extract captured group (metabolite name or ID)
                captured_text = match.group(1) if match.groups() else ""
                
                # Try to extract HMDB ID
                hmdb_id = self._extract_hmdb_id(query)
                
                # Extract metabolite name if no HMDB ID found
                metabolite_name = None
                if not hmdb_id and captured_text:
                    metabolite_name = self._clean_metabolite_name(captured_text)
                
                return SpectraIntent(
                    trigger_type=trigger_type,
                    confidence=confidence,
                    hmdb_id=hmdb_id,
                    metabolite_name=metabolite_name,
                    original_query=query,
                    trigger_phrase=match.group(0),
                    requires_disambiguation=not hmdb_id and bool(metabolite_name)
                )
        
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
    
    def _clean_metabolite_name(self, raw_name: str) -> str:
        """
        Clean and normalize metabolite name
        
        Args:
            raw_name: Raw extracted name
            
        Returns:
            Cleaned metabolite name
        """
        if not raw_name:
            return ""
        
        # Remove common stop words and clean up
        stop_words = {'the', 'of', 'for', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'with'}
        
        # Split and filter words
        words = raw_name.strip().split()
        cleaned_words = [word for word in words if word.lower() not in stop_words]
        
        # Join and capitalize properly
        cleaned_name = ' '.join(cleaned_words)
        
        # Handle special cases
        if cleaned_name.lower() in ['glucose', 'fructose', 'lactate', 'pyruvate', 'citrate']:
            cleaned_name = cleaned_name.capitalize()
        
        logger.info(f"Cleaned metabolite name: '{raw_name}' -> '{cleaned_name}'")
        return cleaned_name
    
    def find_metabolite_matches(self, metabolite_name: str) -> List[Dict[str, Any]]:
        """
        Find potential HMDB matches for a metabolite name
        
        Args:
            metabolite_name: Metabolite name to search
            
        Returns:
            List of potential matches with HMDB IDs
        """
        if not self.neo4j_connection or not metabolite_name:
            return []
        
        try:
            # Query Neo4j for metabolite matches
            query = """
            MATCH (m:Metabolite)
            WHERE toLower(m.name) CONTAINS toLower($name) 
               OR ANY(syn IN m.synonyms WHERE toLower(syn) CONTAINS toLower($name))
            RETURN m.hmdb_id as hmdb_id, m.name as name, m.synonyms as synonyms
            LIMIT 10
            """
            
            results = self.neo4j_connection.run_query(query, {"name": metabolite_name})
            
            matches = []
            for record in results:
                matches.append({
                    "hmdb_id": record["hmdb_id"],
                    "name": record["name"],
                    "synonyms": record["synonyms"] or [],
                    "confidence": self._calculate_name_similarity(metabolite_name, record["name"])
                })
            
            # Sort by confidence
            matches.sort(key=lambda x: x["confidence"], reverse=True)
            
            logger.info(f"Found {len(matches)} potential matches for '{metabolite_name}'")
            return matches
            
        except Exception as e:
            logger.error(f"Error finding metabolite matches: {e}")
            return []
    
    def _calculate_name_similarity(self, query_name: str, db_name: str) -> float:
        """
        Calculate similarity between query name and database name
        
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
            return 0.8
        
        # Word overlap
        query_words = set(query_lower.split())
        db_words = set(db_lower.split())
        
        if query_words and db_words:
            overlap = len(query_words.intersection(db_words))
            total = len(query_words.union(db_words))
            return overlap / total if total > 0 else 0.0
        
        return 0.0
    
    def create_disambiguation_prompt(self, intent: SpectraIntent, matches: List[Dict[str, Any]]) -> str:
        """
        Create a user-friendly disambiguation prompt
        
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
            return f"I found one match for '{intent.metabolite_name}': {match['name']} ({match['hmdb_id']}). Should I generate the spectrum for this metabolite?"
        
        # Multiple matches
        prompt = f"I found multiple metabolites matching '{intent.metabolite_name}':\n\n"
        for i, match in enumerate(matches[:5], 1):  # Show top 5
            prompt += f"{i}. {match['name']} ({match['hmdb_id']}) - Confidence: {match['confidence']:.2f}\n"
        
        prompt += "\nPlease specify which metabolite you want the spectrum for, or provide the exact HMDB ID."
        
        return prompt


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
        
        # If we have an HMDB ID, fetch spectra directly
        if intent.hmdb_id:
            return self._fetch_spectra_by_hmdb_id(intent.hmdb_id, intent)
        
        # If we have a metabolite name, try to resolve it
        if intent.metabolite_name:
            matches = self.detector.find_metabolite_matches(intent.metabolite_name)
            
            if len(matches) == 1 and matches[0]["confidence"] > 0.8:
                # High confidence single match - proceed automatically
                hmdb_id = matches[0]["hmdb_id"]
                logger.info(f"Auto-resolving to {hmdb_id} for '{intent.metabolite_name}'")
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
            spectra_result = self.hmdb_client.get_spectra_for_hmdb_id(hmdb_id)
            
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
                "error": f"Failed to fetch spectra for {hmdb_id}: {str(e)}",
                "original_intent": intent
            }