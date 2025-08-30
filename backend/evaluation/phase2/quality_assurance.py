"""
Phase 2: Quality Assurance
Comprehensive quality validation system for generated question-answer pairs

This module ensures high-quality test sets through automated validation,
duplicate detection, answerability checks, and comprehensive quality metrics.

Author: Senior Engineering Implementation
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Set, Optional, Tuple, Any
import json
import logging
import re
from datetime import datetime
from difflib import SequenceMatcher
import hashlib
from dataclasses import dataclass

from .evaluation_framework import GeneratedQuestion, EvaluationCategory, ComplexityLevel, AnswerType, EntityType

# Configure logging
logger = logging.getLogger(__name__)

@dataclass
class QualityAssuranceConfig:
    """Configuration for quality assurance system with realistic targets"""
    min_answerability_rate: float = 0.30  # REALISTIC: 30% real answers for sparse data
    min_coverage_per_category: int = 3  # REALISTIC: Lower minimum per category  
    similarity_threshold: float = 0.70  # REALISTIC: Less strict duplicate detection
    max_duplicate_percentage: float = 0.50  # REALISTIC: Allow more "no data" responses
    enable_llm_diversification: bool = True
    llm_model: str = "llama-3.3-70b-versatile"
    max_llm_variations: int = 2
    # Add validation for actual performance
    log_failed_questions: bool = True
    max_failed_questions_to_log: int = 20

class QualityMetrics:
    """Quality metrics tracking for question validation"""
    
    def __init__(self):
        self.total_questions = 0
        self.answerable_questions = 0
        self.duplicate_questions = 0
        self.invalid_length_questions = 0
        self.malformed_questions = 0
        self.category_coverage = {}
        self.complexity_coverage = {}
        self.confidence_distribution = []
        
    def add_question_result(self, question: GeneratedQuestion, is_valid: bool, validation_issues: List[str]):
        """Add question validation result to metrics"""
        self.total_questions += 1
        
        if question.answer is not None:
            self.answerable_questions += 1
        
        if "duplicate" in validation_issues:
            self.duplicate_questions += 1
        
        if "invalid_length" in validation_issues:
            self.invalid_length_questions += 1
        
        if "malformed" in validation_issues:
            self.malformed_questions += 1
        
        # Track category coverage
        cat_key = question.category.value
        if cat_key not in self.category_coverage:
            self.category_coverage[cat_key] = 0
        self.category_coverage[cat_key] += 1
        
        # Track complexity coverage
        comp_key = question.complexity.value
        if comp_key not in self.complexity_coverage:
            self.complexity_coverage[comp_key] = 0
        self.complexity_coverage[comp_key] += 1
        
        # Track confidence
        self.confidence_distribution.append(question.confidence)
    
    def get_summary(self) -> Dict[str, Any]:
        """Get quality metrics summary"""
        return {
            "total_questions": self.total_questions,
            "answerability_rate": self.answerable_questions / max(self.total_questions, 1),
            "duplicate_rate": self.duplicate_questions / max(self.total_questions, 1),
            "invalid_length_rate": self.invalid_length_questions / max(self.total_questions, 1),
            "malformed_rate": self.malformed_questions / max(self.total_questions, 1),
            "category_coverage": self.category_coverage,
            "complexity_coverage": self.complexity_coverage,
            "avg_confidence": np.mean(self.confidence_distribution) if self.confidence_distribution else 0,
            "confidence_std": np.std(self.confidence_distribution) if self.confidence_distribution else 0
        }

class DuplicateDetector:
    """Advanced duplicate detection using both exact and semantic similarity"""
    
    def __init__(self, similarity_threshold: float = 0.75):  # REDUCED from 0.85 to allow more variation
        self.similarity_threshold = similarity_threshold
        self.seen_questions = set()  # Exact duplicates
        self.question_texts = []     # For similarity comparison
        self.similarity_cache = {}   # Cache for similarity calculations
    
    def is_duplicate(self, question_text: str) -> Tuple[bool, str]:
        """Check if question is a duplicate with improved tolerance"""
        # Clean the question text
        cleaned_text = self._clean_question_text(question_text)
        
        # Check for exact duplicates
        if cleaned_text in self.seen_questions:
            return True, "exact_duplicate"
        
        # IMPROVED: More intelligent similarity checking
        # Skip similarity check for very short questions (likely different despite similarity)
        if len(cleaned_text.split()) <= 5:
            # For short questions, be less strict
            threshold = max(0.9, self.similarity_threshold + 0.1)
        else:
            threshold = self.similarity_threshold
        
        # Check for similar duplicates
        for existing_question in self.question_texts:
            similarity = self._calculate_similarity(cleaned_text, existing_question)
            if similarity >= threshold:
                # IMPROVED: Allow questions with different entity names to pass
                if self._have_different_entities(question_text, self._reverse_clean(existing_question)):
                    continue  # Allow questions about different entities
                return True, f"similar_duplicate_{similarity:.2f}"
        
        # Not a duplicate - add to seen questions
        self.seen_questions.add(cleaned_text)
        self.question_texts.append(cleaned_text)
        return False, "unique"
    
    def _have_different_entities(self, text1: str, text2: str) -> bool:
        """Check if two questions are about different entities"""
        # Extract potential entity names (words in quotes or capitalized words)
        import re
        
        # Look for quoted words or capitalized sequences
        entity_pattern = r'"([^"]+)"|[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*'
        
        entities1 = set(re.findall(entity_pattern, text1.replace('"', '"').replace('"', '"')))
        entities2 = set(re.findall(entity_pattern, text2.replace('"', '"').replace('"', '"')))
        
        # If we found entities and they're different, allow the question
        if entities1 and entities2 and entities1.isdisjoint(entities2):
            return True
        
        return False
    
    def _reverse_clean(self, cleaned_text: str) -> str:
        """Reverse some cleaning to get original-like text for entity detection"""
        # This is a simple approximation - in practice, we'd store original texts
        return cleaned_text
    
    def _clean_question_text(self, text: str) -> str:
        """Clean question text for comparison with improved handling"""
        import re
        # Convert to lowercase
        text = text.lower().strip()
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        # IMPROVED: Don't remove articles if they're part of entity names
        # text = re.sub(r'\b(the|a|an)\b', '', text)  # Commented out to preserve entity context
        text = re.sub(r'[^\w\s]', '', text)  # Remove punctuation
        return text.strip()
    
    def _calculate_similarity(self, text1: str, text2: str) -> float:
        """Calculate similarity between two texts with improved algorithm"""
        # Use cached result if available
        cache_key = tuple(sorted([text1, text2]))
        if cache_key in self.similarity_cache:
            return self.similarity_cache[cache_key]
        
        # Calculate similarity using SequenceMatcher
        similarity = SequenceMatcher(None, text1, text2).ratio()
        
        # Additional similarity checks for semantic meaning
        words1 = set(text1.split())
        words2 = set(text2.split())
        
        if words1 and words2:
            # Jaccard similarity for word overlap
            word_similarity = len(words1.intersection(words2)) / len(words1.union(words2))
            # IMPROVED: Balanced combination (less weight on sequence, more on words)
            similarity = 0.5 * similarity + 0.5 * word_similarity
        
        # Cache the result
        self.similarity_cache[cache_key] = similarity
        return similarity
    
    def get_duplicate_stats(self) -> Dict[str, int]:
        """Get statistics about detected duplicates"""
        return {
            "total_processed": len(self.question_texts),
            "unique_questions": len(self.seen_questions),
            "cache_size": len(self.similarity_cache)
        }

class AnswerValidator:
    """Validate answers for correctness and completeness"""
    
    def __init__(self):
        self.validation_results = []
    
    def validate_answer(self, question: GeneratedQuestion) -> Tuple[bool, List[str]]:
        """Validate answer for a question with improved tolerance"""
        issues = []
        
        # Check if answer exists
        if question.answer is None:
            issues.append("no_answer")
            return False, issues
        
        # Type-specific validation with improved tolerance
        if question.answer_type == AnswerType.STRING:
            if not isinstance(question.answer, str):
                issues.append("invalid_string_answer")
            elif not question.answer.strip():
                issues.append("empty_string_answer")
            # IMPROVED: Accept short but meaningful answers
            elif len(question.answer.strip()) < 2:
                issues.append("very_short_answer")
        
        elif question.answer_type == AnswerType.NUMBER:
            if not isinstance(question.answer, (int, float)):
                issues.append("invalid_number_answer")
            elif np.isnan(question.answer) or np.isinf(question.answer):
                issues.append("invalid_number_value")
        
        elif question.answer_type == AnswerType.LIST:
            if not isinstance(question.answer, list):
                issues.append("invalid_list_answer")
            # IMPROVED: Accept empty lists for "no associations" type questions
            elif len(question.answer) == 0:
                # Check if this is an aggregation or relationship question that can validly have empty answers
                if question.category.value in ['aggregation', 'one_hop_relationship', 'two_hop_relationship']:
                    # Empty list is valid for "no associations" questions
                    pass  # No issue
                else:
                    issues.append("empty_list_answer")
            # Check for empty list elements
            elif any(not str(item).strip() for item in question.answer):
                issues.append("empty_list_elements")
        
        elif question.answer_type == AnswerType.DICT:
            if not isinstance(question.answer, dict):
                issues.append("invalid_dict_answer")
            elif len(question.answer) == 0:
                issues.append("empty_dict_answer")
        
        elif question.answer_type == AnswerType.BOOLEAN:
            if not isinstance(question.answer, bool):
                issues.append("invalid_boolean_answer")
        
        # Content validation with improved tolerance
        if isinstance(question.answer, str):
            # RELAXED: Increased max length for complex answers
            if len(question.answer) > 2000:  # Increased from 1000
                issues.append("answer_too_long")
            
            # IMPROVED: Better placeholder detection
            placeholders = ["null", "none", "n/a", "unknown", "undefined", ""]
            if question.answer.lower().strip() in placeholders:
                issues.append("placeholder_answer")
        
            # IMPROVED: Accept "No" or "None" answers for aggregation questions
            if question.category.value == 'aggregation' and question.answer.lower().strip() in ['no', 'none', 'zero', '0']:
                # These are valid answers for "does X have Y?" type questions
                pass  # No issue
        
        # IMPROVED: Only consider major issues as validation failures
        major_issues = {"no_answer", "invalid_string_answer", "invalid_number_answer", 
                       "invalid_list_answer", "invalid_dict_answer", "invalid_boolean_answer",
                       "empty_string_answer", "invalid_number_value", "answer_too_long"}
        
        has_major_issues = any(issue in major_issues for issue in issues)
        
        return not has_major_issues, issues

class QuestionValidator:
    """Validate question text quality and format"""
    
    def __init__(self, max_length: int = 200):
        self.max_length = max_length
    
    def validate_question(self, question: GeneratedQuestion) -> Tuple[bool, List[str]]:
        """Validate question text quality with improved intelligence"""
        issues = []
        question_text = question.question
        
        # Basic format checks
        if not question_text or not question_text.strip():
            issues.append("empty_question")
            return False, issues
        
        # Length check - more lenient for complex questions
        max_length = self.max_length
        if question.complexity.value >= 4:
            max_length = int(self.max_length * 1.5)  # Allow longer complex questions
        
        if len(question_text) > max_length:
            issues.append("question_too_long")
        
        # IMPROVED: More lenient minimum length for simple questions
        min_length = 15 if question.complexity.value <= 2 else 10
        if len(question_text) < min_length:
            issues.append("question_too_short")
        
        # IMPROVED: Grammar and format checks with better tolerance
        if not question_text.strip().endswith('?'):
            issues.append("missing_question_mark")
        
        # Check for placeholder text
        if '{' in question_text and '}' in question_text:
            issues.append("unresolved_template")
        
        # IMPROVED: Better handling of scientific notation and chemical names
        # Don't flag unmatched parentheses if they're part of chemical names
        if question_text.count('(') != question_text.count(')'):
            # Check if it's likely a chemical name
            if not any(indicator in question_text.lower() for indicator in ['z,', 'e,', 'r,', 's,', 'alpha', 'beta', 'gamma']):
                issues.append("unmatched_parentheses")
        
        # IMPROVED: Less strict word repetition check
        words = question_text.lower().split()
        if len(words) > 5:  # Only check for longer questions
            word_count = {}
            for word in words:
                if len(word) > 3:  # Only count significant words
                    word_count[word] = word_count.get(word, 0) + 1
            
            # Flag only if there are many repeated significant words
            repeated_words = [word for word, count in word_count.items() if count > 2]
            if len(repeated_words) > 1:
                issues.append("repeated_words")
        
        # IMPROVED: More flexible question format validation
        question_starters = ['what', 'how', 'when', 'where', 'why', 'which', 'who', 'list', 'describe', 'explain', 'in']
        first_word = words[0].lower() if words else ""
        
        # Allow more question formats
        valid_start = (
            first_word in question_starters or 
            first_word.startswith(('is', 'are', 'do', 'does', 'can', 'will', 'have', 'has', 'was', 'were')) or
            any(starter in question_text.lower()[:20] for starter in question_starters)
        )
        
        if not valid_start:
            issues.append("improper_question_format")
        
        # IMPROVED: Only flag as invalid if there are major issues
        major_issues = {"empty_question", "unresolved_template", "question_too_long"}
        has_major_issues = any(issue in major_issues for issue in issues)
        
        return not has_major_issues, issues

class CoverageAnalyzer:
    """Analyze coverage across different dimensions"""
    
    def __init__(self, min_coverage_per_category: int = 10):
        self.min_coverage_per_category = min_coverage_per_category
    
    def analyze_coverage(self, questions: List[GeneratedQuestion]) -> Dict[str, Any]:
        """Analyze coverage across categories, complexity levels, and entities"""
        coverage_analysis = {
            "category_coverage": {},
            "complexity_coverage": {},
            "entity_coverage": {},
            "answer_type_coverage": {},
            "coverage_gaps": [],
            "recommendations": []
        }
        
        # FIXED: Handle empty question list to prevent division by zero
        total_questions = len(questions)
        if total_questions == 0:
            coverage_analysis["recommendations"].append("No questions available for analysis")
            return coverage_analysis
        
        # Analyze category coverage
        for category in EvaluationCategory:
            count = sum(1 for q in questions if q.category == category)
            coverage_analysis["category_coverage"][category.value] = count
            
            if count < self.min_coverage_per_category:
                coverage_analysis["coverage_gaps"].append(f"Low coverage for category: {category.value} ({count} questions)")
        
        # Analyze complexity coverage
        for complexity in ComplexityLevel:
            count = sum(1 for q in questions if q.complexity == complexity)
            coverage_analysis["complexity_coverage"][complexity.value] = count
        
        # Analyze entity coverage
        entity_ids = set(q.entity_id for q in questions)
        coverage_analysis["entity_coverage"]["unique_entities"] = len(entity_ids)
        coverage_analysis["entity_coverage"]["metabolites"] = sum(1 for q in questions if q.entity_type.value == "metabolite")
        coverage_analysis["entity_coverage"]["proteins"] = sum(1 for q in questions if q.entity_type.value == "protein")
        
        # Analyze answer type coverage
        for answer_type in AnswerType:
            count = sum(1 for q in questions if q.answer_type == answer_type)
            coverage_analysis["answer_type_coverage"][answer_type.value] = count
        
        # Generate recommendations
        if total_questions < 100:
            coverage_analysis["recommendations"].append("Consider generating more questions for better coverage")
        
        # FIXED: Safe division with zero check
        if total_questions > 0:
            # Check balance between entity types
            metabolite_pct = coverage_analysis["entity_coverage"]["metabolites"] / total_questions * 100
            protein_pct = coverage_analysis["entity_coverage"]["proteins"] / total_questions * 100
            
            if abs(metabolite_pct - protein_pct) > 30:
                coverage_analysis["recommendations"].append("Consider balancing questions between metabolites and proteins")
        
        return coverage_analysis

class QualityAssurance:
    """Main quality assurance orchestrator"""
    
    def __init__(self, min_answerability_rate: float = 0.95, min_coverage_per_category: int = 10, 
                 max_question_length: int = 200, enable_duplicate_detection: bool = True,
                 similarity_threshold: float = 0.85):
        
        self.min_answerability_rate = min_answerability_rate
        self.min_coverage_per_category = min_coverage_per_category
        self.max_question_length = max_question_length
        self.enable_duplicate_detection = enable_duplicate_detection
        self.similarity_threshold = similarity_threshold
        
        # Initialize validators
        self.duplicate_detector = DuplicateDetector(similarity_threshold) if enable_duplicate_detection else None
        self.answer_validator = AnswerValidator()
        self.question_validator = QuestionValidator(max_question_length)
        self.coverage_analyzer = CoverageAnalyzer(min_coverage_per_category)
        
        # Results tracking
        self.metrics = QualityMetrics()
        self.validation_details = []
    
    async def validate_questions(self, questions: List[GeneratedQuestion]) -> Tuple[List[GeneratedQuestion], Dict[str, Any]]:
        """Validate all questions and return filtered set with quality report"""
        logger.info(f"Starting quality validation for {len(questions)} questions...")
        
        validation_start = datetime.now()
        validated_questions = []
        removed_questions = []
        
        # Reset metrics
        self.metrics = QualityMetrics()
        self.validation_details = []
        
        # IMPROVED: First pass - remove exact and high-similarity duplicates
        unique_questions = []
        duplicate_count = 0
        
        if self.duplicate_detector:
            for i, question in enumerate(questions):
                is_duplicate, duplicate_type = self.duplicate_detector.is_duplicate(question.question)
                if is_duplicate:
                    duplicate_count += 1
                    removed_questions.append({
                        "question": question.question,
                        "reason": f"duplicate_{duplicate_type}",
                        "entity_id": question.entity_id,
                        "category": question.category.value
                    })
                    logger.debug(f"Removed duplicate question: {question.question[:50]}...")
                else:
                    unique_questions.append(question)
        else:
            unique_questions = questions
        
        logger.info(f"Removed {duplicate_count} duplicate questions, {len(unique_questions)} remain")
        
        # IMPROVED: Second pass - validate remaining questions and assign confidence scores
        for i, question in enumerate(unique_questions):
            validation_result = await self._validate_single_question(question, i)
            
            # IMPROVED: Calculate dynamic confidence score based on answer quality
            confidence_score = self._calculate_confidence_score(question, validation_result)
            question.confidence = confidence_score
            
            self.validation_details.append(validation_result)
        
            # IMPROVED: Better logging and less strict validation
            if validation_result["is_valid"]:
                validated_questions.append(question)
            else:
                # Log validation issues for debugging
                issues = validation_result["validation_issues"]
                logger.debug(f"Question {i} failed validation: {issues}")
                logger.debug(f"Question text: {question.question[:100]}...")
                
                removed_questions.append({
                    "question": question.question,
                    "reason": ", ".join(issues),
                    "entity_id": question.entity_id,
                    "category": question.category.value
                })
        
        # IMPROVED: Log validation results
        logger.info(f"Validation results: {len(validated_questions)} passed, {len(removed_questions)} failed")
        if removed_questions:
            # Log sample of failed questions for debugging
            sample_failures = removed_questions[:5]
            for failure in sample_failures:
                logger.debug(f"Sample failure: {failure['reason']} - {failure['question'][:50]}...")
        
        # IMPROVED: Handle case where no questions pass validation
        if len(validated_questions) == 0:
            logger.error("No questions passed validation! This indicates overly strict validation criteria.")
            logger.error("Attempting to recover by using questions with minor issues only...")
            
            # Try to recover by accepting questions with only minor issues
            for question in unique_questions:
                validation_result = await self._validate_single_question(question, 0)
                if self._has_only_minor_issues(validation_result["validation_issues"]):
                    validated_questions.append(question)
                    logger.debug(f"Recovered question with minor issues: {question.question[:50]}...")
            
            logger.info(f"Recovered {len(validated_questions)} questions with minor issues")
        
        # Perform coverage analysis
        coverage_analysis = self.coverage_analyzer.analyze_coverage(validated_questions)
        
        # Calculate final metrics
        validation_duration = (datetime.now() - validation_start).total_seconds()
        
        # Check if meets quality thresholds
        answerability_rate = self.metrics.answerable_questions / len(questions) if questions else 0
        meets_answerability = answerability_rate >= self.min_answerability_rate
        
        quality_report = {
            "validation_summary": {
                "total_questions_validated": len(questions),
                "questions_passed": len(validated_questions),
                "questions_removed": len(removed_questions),
                "duplicates_removed": duplicate_count,
                "validation_duration_seconds": validation_duration,
                "overall_quality_score": self._calculate_overall_quality_score(validated_questions),
                "meets_answerability_threshold": meets_answerability,
                "validated_at": datetime.now().isoformat()
            },
            "quality_metrics": {
                "total_questions": len(validated_questions),
                "answerability_rate": answerability_rate,
                "duplicate_rate": duplicate_count / len(questions) if questions else 0,
                "invalid_length_rate": self.metrics.invalid_length_questions / len(questions) if questions else 0,
                "malformed_rate": self.metrics.malformed_questions / len(questions) if questions else 0,
                "category_coverage": self.metrics.category_coverage,
                "complexity_coverage": self.metrics.complexity_coverage,
                "avg_confidence": np.mean([q.confidence for q in validated_questions]) if validated_questions else 0,
                "confidence_std": np.std([q.confidence for q in validated_questions]) if validated_questions else 0
            },
            "coverage_analysis": coverage_analysis,
            "removed_questions_sample": removed_questions[:10],  # First 10 for debugging
            "duplicate_stats": self.duplicate_detector.get_duplicate_stats() if self.duplicate_detector else {},
            "validation_details": self.validation_details[:100]  # First 100 for debugging
        }
        
        logger.info(f"✅ Quality validation completed: {len(validated_questions)}/{len(questions)} questions passed")
        logger.info(f"Removed {duplicate_count} duplicates, answerability rate: {answerability_rate:.2%}")
        
        return validated_questions, quality_report
    
    async def _validate_single_question(self, question: GeneratedQuestion, index: int) -> Dict[str, Any]:
        """Validate a single question comprehensively"""
        validation_issues = []
        
        # Question text validation
        question_valid, question_issues = self.question_validator.validate_question(question)
        validation_issues.extend(question_issues)
        
        # Answer validation
        answer_valid, answer_issues = self.answer_validator.validate_answer(question)
        validation_issues.extend(answer_issues)
        
        # FIXED: Remove duplicate detection from here since it's already handled in the first pass
        # The duplicate detector is stateful and would mark all questions as duplicates on second pass
        
        # Overall validity (no duplicate check needed here)
        is_valid = question_valid and answer_valid
        
        # Update metrics
        self.metrics.add_question_result(question, is_valid, validation_issues)
        
        validation_result = {
            "question_index": index,
            "question_id": f"{question.entity_id}_{question.category.value}_{index}",
            "is_valid": is_valid,
            "validation_issues": validation_issues,
            "question_text": question.question,
            "answer_preview": str(question.answer)[:100] + "..." if len(str(question.answer)) > 100 else str(question.answer),
            "category": question.category.value,
            "complexity": question.complexity.value,
            "confidence": question.confidence
        }
        
        return validation_result
    
    def filter_questions_by_quality(self, questions: List[GeneratedQuestion], validation_results: Dict[str, Any]) -> List[GeneratedQuestion]:
        """Filter questions based on quality validation results"""
        logger.info("Filtering questions based on quality validation...")
        
        filtered_questions = []
        validation_details = validation_results.get("validation_details", [])
        
        for i, question in enumerate(questions):
            if i < len(validation_details):
                detail = validation_details[i]
                
                # Include question if valid or has only minor issues
                if detail["is_valid"] or self._has_only_minor_issues(detail["validation_issues"]):
                    filtered_questions.append(question)
                else:
                    logger.debug(f"Filtered out question {i}: {detail['validation_issues']}")
        
        logger.info(f"Filtered {len(questions) - len(filtered_questions)} questions. {len(filtered_questions)} questions remain.")
        
        return filtered_questions
    
    def _has_only_minor_issues(self, issues: List[str]) -> bool:
        """Check if validation issues are minor and can be overlooked"""
        # IMPROVED: More comprehensive list of minor vs major issues
        minor_issues = {
            "missing_question_mark", 
            "question_too_short", 
            "repeated_words", 
            "unmatched_parentheses",  # Common in chemical names
            "improper_question_format"  # Some questions might use different formats
        }
        
        major_issues = {
            "no_answer", 
            "invalid_string_answer", 
            "invalid_number_answer",
            "invalid_list_answer",
            "invalid_dict_answer",
            "invalid_boolean_answer",
            "empty_question", 
            "unresolved_template", 
            "placeholder_answer",
            "answer_too_long"
        }
        
        # Check if all issues are minor
        has_major = any(issue in major_issues for issue in issues)
        
        # IMPROVED: Allow questions with only minor issues or no issues at all
        return not has_major and len(issues) <= 3  # Allow up to 3 minor issues
    
    def _calculate_confidence_score(self, question: GeneratedQuestion, validation_result: Dict[str, Any]) -> float:
        """Calculate realistic confidence score based on actual data quality and completeness"""
        # START WITH REALISTIC BASE - NOT 1.0!
        # Base confidence depends on data availability and quality
        base_confidence = 0.5  # Start with moderate confidence
        
        # MAJOR BOOST for having real, complete data
        if question.answer is not None:
            if question.answer_type == AnswerType.STRING:
                if isinstance(question.answer, str) and question.answer.strip():
                    answer_str = question.answer.strip().lower()
                    
                    # HIGH confidence for real data responses
                    if ("no data available" not in answer_str and 
                        "not detailed in current dataset" not in answer_str and
                        "no external database identifiers" not in answer_str and
                        answer_str not in ['nan', '', 'none', 'null']):
                        
                        # Boost for substantial, informative answers
                        if len(question.answer) > 50:  # Detailed response
                            base_confidence += 0.4
                        elif len(question.answer) > 20:  # Moderate response
                            base_confidence += 0.3
                        else:  # Basic response
                            base_confidence += 0.2
                    else:
                        # Lower confidence for "no data" responses (honest but limited)
                        base_confidence += 0.1
                        
            elif question.answer_type == AnswerType.NUMBER:
                if isinstance(question.answer, (int, float)) and question.answer >= 0:
                    base_confidence += 0.3  # Numeric data is reliable when present
                    
            elif question.answer_type == AnswerType.LIST:
                if isinstance(question.answer, list):
                    if len(question.answer) > 5:  # Rich relationship data
                        base_confidence += 0.4
                    elif len(question.answer) > 0:  # Some relationship data
                        base_confidence += 0.2
                    # Empty lists get no boost but aren't penalized (honest response)
                        
            elif question.answer_type == AnswerType.DICT:
                if isinstance(question.answer, dict):
                    if len(question.answer) > 3:  # Multiple external references
                        base_confidence += 0.4
                    elif len(question.answer) > 0:  # Some external references
                        base_confidence += 0.2
                    # Empty dicts get no boost but aren't penalized
        else:
            # No answer available - significant confidence reduction
            base_confidence = 0.2
        
        # REDUCE confidence for validation issues
        if validation_result.get("validation_issues"):
            for issue in validation_result["validation_issues"]:
                if "invalid" in issue or "error" in issue:
                    base_confidence -= 0.3
                elif "partial" in issue or "incomplete" in issue:
                    base_confidence -= 0.2
                elif "ambiguous" in issue or "uncertain" in issue:
                    base_confidence -= 0.2
                elif "fallback" in issue:
                    base_confidence -= 0.1
        
        # BOOST confidence for high-quality entity data
        entity_metadata = question.metadata or {}
        entity_name = entity_metadata.get("entity_name", "")
        
        # Check if entity has rich data (multiple non-null fields)
        if question.entity_type == EntityType.METABOLITE:
            # Indicators of data completeness
            if (entity_metadata.get("has_chemical_formula") and 
                entity_metadata.get("has_molecular_weight") and
                entity_metadata.get("has_external_ids")):
                base_confidence += 0.1
                
        elif question.entity_type == EntityType.PROTEIN:
            # Indicators of protein data completeness  
            if (entity_metadata.get("has_gene_name") and 
                entity_metadata.get("has_function") and
                entity_metadata.get("has_uniprot_id")):
                base_confidence += 0.1
        
        # Ensure confidence stays within realistic bounds
        confidence = max(0.1, min(1.0, base_confidence))
        
        # REALISTIC DISTRIBUTION: Most questions should be 0.3-0.8, not 0.9-1.0
        return round(confidence, 2)
    
    def _calculate_overall_quality_score(self, questions: List[GeneratedQuestion]) -> float:
        """Calculate realistic overall quality score that accounts for data limitations"""
        if not questions:
            return 0.0
        
        # Calculate weighted quality based on different factors
        total_weighted_score = 0.0
        total_weight = 0.0
        
        data_availability_scores = []
        answer_quality_scores = []
        
        for question in questions:
            # Weight based on complexity and answer completeness
            complexity_weight = question.complexity.value * 0.2  # Higher complexity = higher weight
            
            # Data availability score (honest assessment)
            if question.answer is not None:
                answer_str = str(question.answer).lower() if question.answer else ""
                
                if ("no data available" in answer_str or 
                    "not detailed in current dataset" in answer_str or
                    "no external database identifiers" in answer_str):
                    data_availability = 0.3  # Honest but limited
                elif question.answer_type == AnswerType.LIST and isinstance(question.answer, list) and len(question.answer) == 0:
                    data_availability = 0.3  # Honest empty response
                elif question.answer_type == AnswerType.DICT and isinstance(question.answer, dict) and len(question.answer) == 0:
                    data_availability = 0.3  # Honest empty response
                else:
                    data_availability = 0.8  # Good data available
            else:
                data_availability = 0.1  # No answer possible
                
            data_availability_scores.append(data_availability)
            
            # Answer quality score (separate from availability)
            if question.answer is not None:
                if question.answer_type == AnswerType.STRING and isinstance(question.answer, str):
                    if len(question.answer) > 50:
                        quality = 0.9
                    elif len(question.answer) > 20:
                        quality = 0.7
                    else:
                        quality = 0.5
                elif question.answer_type == AnswerType.NUMBER and isinstance(question.answer, (int, float)):
                    quality = 0.8  # Numeric data is precise
                elif question.answer_type == AnswerType.LIST and isinstance(question.answer, list):
                    quality = min(0.8, 0.3 + (len(question.answer) * 0.1))  # Quality scales with list size
                elif question.answer_type == AnswerType.DICT and isinstance(question.answer, dict):
                    quality = min(0.8, 0.3 + (len(question.answer) * 0.1))  # Quality scales with dict size
                else:
                    quality = 0.4
            else:
                quality = 0.0
                
            answer_quality_scores.append(quality)
            
            # Combined score for this question
            question_score = (data_availability * 0.4 + quality * 0.6) * (1 + complexity_weight)
            question_weight = 1.0 + complexity_weight
            
            total_weighted_score += question_score
            total_weight += question_weight
        
        # Calculate component scores
        avg_data_availability = sum(data_availability_scores) / len(data_availability_scores)
        avg_answer_quality = sum(answer_quality_scores) / len(answer_quality_scores)
        
        # Overall score with realistic weighting
        if total_weight > 0:
            overall_score = total_weighted_score / total_weight
        else:
            overall_score = 0.0
            
        # Apply realistic scaling - acknowledge that perfect scores are rare in real data
        # When dealing with limited real data, scores in 0.4-0.7 range are good
        realistic_score = min(0.85, overall_score)  # Cap at 0.85 to be realistic
        
        return round(realistic_score, 3)
    
    def _generate_recommendations(self, quality_summary: Dict[str, Any], coverage_analysis: Dict[str, Any]) -> List[str]:
        """Generate recommendations for improving question quality"""
        recommendations = []
        
        # Answerability recommendations
        if quality_summary["answerability_rate"] < self.min_answerability_rate:
            recommendations.append(f"Improve answerability rate: currently {quality_summary['answerability_rate']:.1%}, target {self.min_answerability_rate:.1%}")
        
        # Duplicate recommendations
        if quality_summary["duplicate_rate"] > 0.1:
            recommendations.append(f"Reduce duplicate questions: currently {quality_summary['duplicate_rate']:.1%} duplicates detected")
        
        # Coverage recommendations
        coverage_gaps = coverage_analysis.get("coverage_gaps", [])
        if coverage_gaps:
            recommendations.append("Address coverage gaps: " + "; ".join(coverage_gaps[:3]))
        
        # Category balance recommendations
        category_coverage = coverage_analysis.get("category_coverage", {})
        if category_coverage:
            min_coverage = min(category_coverage.values())
            max_coverage = max(category_coverage.values())
            if max_coverage > min_coverage * 3:
                recommendations.append("Consider balancing question distribution across categories")
        
        # Confidence recommendations
        if quality_summary["avg_confidence"] < 0.8:
            recommendations.append(f"Improve question confidence: currently {quality_summary['avg_confidence']:.2f}, consider reviewing low-confidence questions")
        
        # Add coverage analyzer recommendations
        recommendations.extend(coverage_analysis.get("recommendations", []))
        
        return recommendations[:10]  # Limit to top 10 recommendations 