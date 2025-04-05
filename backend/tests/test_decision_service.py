import pytest
import sys
import os
from typing import List, Dict

# Add the parent directory to the path so we can import our modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from backend.services.decision_service import QueryDecisionService

class TestDecisionService:
    """Test suite for the QueryDecisionService"""
    
    def setup_method(self):
        """Set up for each test method"""
        self.decision_service = QueryDecisionService(memory_confidence_threshold=0.85)
        
        # Mock memory results
        self.memory_results_high_confidence = [
            {
                "user_query": "What is the molecular weight of citric acid?",
                "answer": "The molecular weight of citric acid is 192.12 g/mol.",
                "entities": ["citric acid", "C6H8O7"],
                "relevance_score": 0.92,
                "score_components": {"entity_match": 0.8, "semantic_similarity": 0.9}
            }
        ]
        
        self.memory_results_medium_confidence = [
            {
                "user_query": "Tell me about glucose",
                "answer": "Glucose is a simple sugar with the molecular formula C6H12O6.",
                "entities": ["glucose", "C6H12O6"],
                "relevance_score": 0.65,
                "score_components": {"entity_match": 0.6, "semantic_similarity": 0.7}
            }
        ]
        
        self.memory_results_empty = []
        
    def test_general_question_detection(self):
        """Test if general questions are correctly identified"""
        # General questions that should be identified
        general_questions = [
            "What is metabolomics?",
            "Can you explain how mass spectrometry works?",
            "Why do we use chromatography?",
            "Hello there",
            "Thanks for your help",
            "What is the difference between LC-MS and GC-MS?",
            "How does metabolism work in humans?"
        ]
        
        # DB questions that should NOT be identified as general
        db_questions = [
            "What is the structure of citric acid?",
            "Show me HMDB0000094",
            "What's the KEGG ID for glucose?",
            "Compare the pathways of glucose and fructose",
            "What is the molecular weight of ATP?",
            "Find metabolites related to citric acid cycle"
        ]
        
        # Test general questions
        for question in general_questions:
            assert self.decision_service.is_general_question(question), f"Failed to identify '{question}' as general"
            
        # Test DB questions
        for question in db_questions:
            assert not self.decision_service.is_general_question(question), f"Incorrectly identified '{question}' as general"
    
    def test_memory_confidence_decision(self):
        """Test if high-confidence memory matches are used"""
        # Test high confidence match
        high_conf_question = "What is the molecular weight of citric acid?"
        use_memory, memory_entry = self.decision_service.should_use_memory(
            high_conf_question, self.memory_results_high_confidence
        )
        assert use_memory, "Should use memory for high confidence match"
        assert memory_entry is not None, "Memory entry should be returned"
        
        # Test medium confidence match - shouldn't use memory directly
        medium_conf_question = "Tell me about glucose"
        use_memory, memory_entry = self.decision_service.should_use_memory(
            medium_conf_question, self.memory_results_medium_confidence
        )
        assert not use_memory, "Should not use memory for medium confidence match"
        
        # Test follow-up with medium confidence
        followup_question = "What is its molecular formula?"  # Follow-up about glucose
        # For follow-ups, we might use memory with lower confidence
        use_memory, memory_entry = self.decision_service.should_use_memory(
            followup_question, self.memory_results_medium_confidence
        )
        assert self.decision_service._is_likely_followup(followup_question), "Should detect as follow-up"
        # May still not use memory if confidence + followup doesn't meet threshold
        
    def test_query_path_decision(self):
        """Test the overall decision path selection"""
        # Test memory path selection
        memory_question = "What is the molecular weight of citric acid?"
        decision, entry = self.decision_service.decide_query_path(
            memory_question, self.memory_results_high_confidence
        )
        assert decision == "memory", "Should choose memory path for high confidence match"
        
        # Test general question path selection
        general_question = "What is metabolomics?"
        decision, entry = self.decision_service.decide_query_path(
            general_question, self.memory_results_empty
        )
        assert decision == "general", "Should choose general path for general question"
        
        # Test database pipeline path selection
        db_question = "What is the structure of HMDB0000094?"
        decision, entry = self.decision_service.decide_query_path(
            db_question, self.memory_results_empty
        )
        assert decision == "pipeline", "Should choose pipeline path for database question"
    
    def test_context_preparation(self):
        """Test the preparation of context from memory results"""
        context = self.decision_service.prepare_context_from_memory(self.memory_results_high_confidence)
        assert "Previous Q:" in context, "Context should contain previous questions"
        assert "Previous A:" in context, "Context should contain previous answers"
        assert "citric acid" in context, "Context should contain relevant entities"
        
        # Test with empty memory
        empty_context = self.decision_service.prepare_context_from_memory([])
        assert empty_context == "", "Context should be empty for empty memory"
        
        # Test with multiple memory entries
        multiple_memory = self.memory_results_high_confidence + self.memory_results_medium_confidence
        multi_context = self.decision_service.prepare_context_from_memory(multiple_memory)
        assert "citric acid" in multi_context, "Multi-context should contain first entry"
        assert "glucose" in multi_context, "Multi-context should contain second entry"


if __name__ == "__main__":
    pytest.main(["-xvs", __file__]) 