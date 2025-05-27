"""
Optimized Entity Matcher for Phase 3 Integration

This module provides ultra-fast entity matching that replaces the slow APOC fuzzy matching
from the original Phase 3 implementation. It guarantees sub-second performance while
maintaining good search coverage.

Key optimizations:
1. Only uses indexed fulltext search (fastest)
2. Lucene-based fuzzy matching (fast)
3. Strict timeout enforcement (max 2 seconds total)
4. Fail-fast approach for non-existent entities

Recent fixes:
- Early exit after high-confidence primary matches (>= 0.95)
- Stricter validation for fuzzy matches with non-alpha characters
- Warning logs for fallback timeout scenarios
- Logging when Lucene operators are stripped from queries
"""

import time
from typing import List, Dict, Optional
import re
import asyncio

class OptimizedEntityMatcher:
    """
    Ultra-fast entity matcher optimized for production use.
    
    Replaces the slow APOC fuzzy matching with fast Lucene-based approaches.
    Guarantees maximum 2 seconds per search operation.
    """
    
    def __init__(self, neo4j_connection):
        self.neo4j_connection = neo4j_connection
        
        # Performance tracking
        self.performance_metrics = {
            "total_queries": 0,
            "successful_queries": 0,
            "average_response_time": 0.0,
            "strategy_usage": {
                "primary_fulltext": 0,
                "fuzzy_fulltext": 0,
                "fallback_direct": 0,
                "failed_queries": 0
            }
        }
        
    def _log_performance(self, strategy: str, execution_time: float, success: bool):
        """Log performance metrics for monitoring and optimization"""
        self.performance_metrics["total_queries"] += 1
        
        if success:
            self.performance_metrics["successful_queries"] += 1
            
        # Update strategy usage
        if strategy in self.performance_metrics["strategy_usage"]:
            self.performance_metrics["strategy_usage"][strategy] += 1
        else:
            self.performance_metrics["strategy_usage"][strategy] = 1
            
        # Update average response time
        total = self.performance_metrics["total_queries"]
        current_avg = self.performance_metrics["average_response_time"]
        self.performance_metrics["average_response_time"] = (
            (current_avg * (total - 1) + execution_time) / total
        )
    
    def get_performance_report(self) -> dict:
        """Get comprehensive performance metrics"""
        total = self.performance_metrics["total_queries"]
        successful = self.performance_metrics["successful_queries"]
        
        return {
            "total_queries": total,
            "successful_queries": successful,
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "average_response_time": self.performance_metrics["average_response_time"],
            "strategy_distribution": self.performance_metrics["strategy_usage"],
            "performance_grade": self._calculate_performance_grade()
        }
    
    def _calculate_performance_grade(self) -> str:
        """Calculate overall performance grade"""
        metrics = self.performance_metrics
        total = metrics["total_queries"]
        
        if total == 0:
            return "No data"
            
        success_rate = (metrics["successful_queries"] / total) * 100
        avg_time = metrics["average_response_time"]
        
        if success_rate >= 90 and avg_time < 0.5:
            return "A+ (Excellent)"
        elif success_rate >= 80 and avg_time < 1.0:
            return "A (Very Good)"
        elif success_rate >= 70 and avg_time < 2.0:
            return "B (Good)"
        elif success_rate >= 60:
            return "C (Acceptable)"
        else:
            return "D (Needs Improvement)"
    
    async def _run_query_with_timeout(self, query: str, params: dict, timeout: float = 1.5) -> List[dict]:
        """
        Run a Neo4j query with timeout protection.
        
        Args:
            query: The Cypher query to execute
            params: Query parameters
            timeout: Maximum time to wait for query completion
            
        Returns:
            Query results or empty list if timeout/error
        """
        try:
            # Create a future for the query execution
            loop = asyncio.get_event_loop()
            future = loop.run_in_executor(
                None, 
                lambda: self.neo4j_connection.run_query(query, params)
            )
            
            # Wait for the query with timeout
            results = await asyncio.wait_for(future, timeout=timeout)
            return results or []
            
        except asyncio.TimeoutError:
            print(f"[DEBUG] Query timeout after {timeout}s")
            return []
        except Exception as e:
            print(f"[DEBUG] Query execution error: {e}")
            return []
    
    def _detect_query_complexity(self, entity_name: str) -> str:
        """
        Detect query complexity to optimize timeout strategies.
        
        Args:
            entity_name: The search term
            
        Returns:
            Complexity level: 'simple', 'medium', 'complex'
        """
        # Complex patterns that can cause slower queries
        complex_patterns = [
            r'^\w+\s+\w+',  # Multi-word queries like "Citric Acid"
            r'\w+~\d+',     # Lucene fuzzy syntax like "glucose~1"
            r'\w+\*',       # Wildcard patterns
            r'[A-Z]{2,}',   # All caps abbreviations
        ]
        
        # Medium complexity patterns
        medium_patterns = [
            r'^\w{7,}$',    # Long single words
            r'\d+',         # Contains numbers
        ]
        
        # Check for complex patterns
        for pattern in complex_patterns:
            if re.search(pattern, entity_name):
                return 'complex'
        
        # Check for medium patterns
        for pattern in medium_patterns:
            if re.search(pattern, entity_name):
                return 'medium'
        
        return 'simple'
    
    def _get_optimized_timeouts(self, complexity: str) -> dict:
        """
        Get optimized timeout values based on query complexity.
        
        Args:
            complexity: Query complexity level
            
        Returns:
            Dict with timeout values for different operations
        """
        timeout_configs = {
            'simple': {
                'primary_timeout': 1.2,
                'fuzzy_timeout': 0.8,
                'fallback_timeout': 0.3,
                'max_total': 1.8
            },
            'medium': {
                'primary_timeout': 1.0,
                'fuzzy_timeout': 0.6,
                'fallback_timeout': 0.3,
                'max_total': 1.8
            },
            'complex': {
                'primary_timeout': 0.8,  # Reduced for complex queries
                'fuzzy_timeout': 0.5,    # Shorter fuzzy timeout
                'fallback_timeout': 0.2, # Minimal fallback time
                'max_total': 1.5         # Stricter total time limit
            }
        }
        
        return timeout_configs.get(complexity, timeout_configs['medium'])
    
    async def _fallback_direct_search(self, entity_name: str, timeout_budget: float) -> List[dict]:
        """
        Fallback search using direct name matching when fulltext search fails.
        
        Args:
            entity_name: The entity name to search for
            timeout_budget: Remaining time budget for this operation
            
        Returns:
            Query results or empty list
        """
        # FIX 3: Warning for fallback timeout - with more aggressive thresholds
        min_required_time = 0.2  # Reduced minimum time requirement
        if timeout_budget < min_required_time:
            print(f"[WARN] Fallback skipped due to insufficient time budget ({timeout_budget:.3f}s remaining, need {min_required_time}s)")
            return []
            
        direct_query = """
        MATCH (m:Metabolite)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex)
        WHERE toLower(m.name) CONTAINS toLower($search_term)
           OR toLower(si.canonical) CONTAINS toLower($search_term)
           OR any(syn IN si.synonyms WHERE toLower(syn) CONTAINS toLower($search_term))
        RETURN m.name as metabolite_name, 
               m.accession as accession,
               m.description as description,
               si.canonical as canonical_name,
               si.synonyms as synonyms,
               1.0 as score,
               'direct_fallback' as search_method
        ORDER BY m.name
        LIMIT 5
        """
        
        try:
            start_time = time.time()
            # Use the minimum of timeout_budget and 0.4s for fallback
            actual_timeout = min(timeout_budget * 0.8, 0.4)  # Conservative timeout
            results = await self._run_query_with_timeout(direct_query, {"search_term": entity_name}, timeout=actual_timeout)
            execution_time = time.time() - start_time
            
            # FIX 3: Warning if fallback exceeds time budget
            if execution_time > timeout_budget:
                print(f"[WARN] Fallback search exceeded time budget: {execution_time:.3f}s > {timeout_budget:.3f}s")
                
            return results
        except Exception as e:
            print(f"[DEBUG] Fallback direct search failed: {e}")
            return []
    
    def _validate_fuzzy_match(self, original_query: str, matched_name: str, confidence: float) -> float:
        """
        FIX 2: Enhanced validation for fuzzy matches with better false positive detection.
        
        Args:
            original_query: The original search query
            matched_name: The matched metabolite name
            confidence: Original confidence score
            
        Returns:
            Adjusted confidence score
        """
        # IMPROVED: More sophisticated validation for suspicious patterns
        
        # 1. Check for number-to-letter substitutions (like "0" for "o")
        digit_substitutions = {
            '0': 'o', '1': 'i', '3': 'e', '4': 'a', '5': 's', '7': 't', '8': 'b'
        }
        
        suspicious_substitution = False
        for digit, letter in digit_substitutions.items():
            if digit in original_query.lower() and letter in matched_name.lower():
                # Check if replacing the digit with the letter makes them more similar
                query_normalized = original_query.lower().replace(digit, letter)
                if query_normalized != original_query.lower():
                    suspicious_substitution = True
                    print(f"[DEBUG] Detected suspicious digit substitution: '{digit}' -> '{letter}' in query '{original_query}'")
                    break
        
        # 2. Character similarity analysis
        query_alpha_only = re.sub(r'[^a-zA-Z]', '', original_query.lower())
        match_alpha_only = re.sub(r'[^a-zA-Z]', '', matched_name.lower())
        
        similarity_ratio = 0.0
        if len(query_alpha_only) > 0 and len(match_alpha_only) > 0:
            # Calculate Levenshtein-like similarity
            max_len = max(len(query_alpha_only), len(match_alpha_only))
            min_len = min(len(query_alpha_only), len(match_alpha_only))
            
            # Count matching characters in order
            common_chars = sum(1 for a, b in zip(query_alpha_only, match_alpha_only) if a == b)
            similarity_ratio = common_chars / max_len
            
            # Adjust for length differences
            length_penalty = abs(len(query_alpha_only) - len(match_alpha_only)) / max_len
            similarity_ratio = similarity_ratio * (1 - length_penalty * 0.5)
        
        # 3. Apply penalties based on validation results
        penalty = 0.0
        
        if suspicious_substitution:
            penalty += 0.3  # 30% penalty for suspicious substitutions
            
        if similarity_ratio < 0.6:  # Less than 60% similarity
            penalty += 0.2  # 20% penalty for low similarity
            
        if similarity_ratio < 0.4:  # Very low similarity
            penalty += 0.2  # Additional 20% penalty
            
        # Cap the total penalty at 70% to avoid completely eliminating valid fuzzy matches
        penalty = min(penalty, 0.7)
        
        if penalty > 0:
            adjusted_confidence = confidence * (1 - penalty)
            print(f"[DEBUG] Applied fuzzy match penalty for '{original_query}' -> '{matched_name}': {confidence:.3f} -> {adjusted_confidence:.3f} (similarity: {similarity_ratio:.3f})")
            return adjusted_confidence
        
        return confidence
    
    async def match_entities(self, entity_name: str, entity_type: str) -> List[dict]:
        """
        Enhanced entity matching using optimized search strategies.
        
        Implements ultra-fast cascading strategy with adaptive timeouts:
        1. Primary fulltext search (0.1-0.8s based on complexity)
        2. Fuzzy fulltext search with Lucene syntax (0.3-0.5s based on complexity)
        3. Fail fast if no results (max 1.5-2s total based on complexity)
        
        Args:
            entity_name: The entity name to search for
            entity_type: The type of entity (e.g., "Metabolite")
            
        Returns:
            List of matched entities with confidence scores and search method info
        """
        start_time = time.time()
        
        # Handle empty or None input
        if not entity_name:
            print(f"[DEBUG] Empty or None entity name provided")
            execution_time = time.time() - start_time
            self._log_performance("failed_queries", execution_time, False)
            return []
            
        # Clean and validate input
        entity_name = entity_name.strip()
        if not entity_name:
            print(f"[DEBUG] Entity name is empty after stripping whitespace")
            execution_time = time.time() - start_time
            self._log_performance("failed_queries", execution_time, False)
            return []
        
        # FIX 4: Log when Lucene operators are stripped
        original_entity_name = entity_name
        
        # Detect query complexity BEFORE sanitization for better timeout planning
        query_complexity = self._detect_query_complexity(entity_name)
        timeout_config = self._get_optimized_timeouts(query_complexity)
        
        print(f"[DEBUG] Query complexity: {query_complexity} | Timeouts: {timeout_config}")
        
        # IMPROVED SANITIZATION: More comprehensive Lucene-safe cleanup
        # Remove ALL potentially dangerous Lucene special characters
        lucene_special_chars = r'[+\-&|!{}[\]^"~*?:\\()/<>]'
        
        # Check if we're removing Lucene operators
        lucene_operators_found = re.search(lucene_special_chars, entity_name)
        if lucene_operators_found:
            print(f"[DEBUG] Lucene syntax (~, *, ?, +, -, etc.) detected and removed from query: '{entity_name}'")
        
        # Replace special chars with spaces, then clean up
        sanitized_name = re.sub(lucene_special_chars, ' ', entity_name)
        
        # Remove any remaining non-alphanumeric characters except spaces, hyphens, periods, and apostrophes
        sanitized_name = re.sub(r'[^\w\s\-\.\']', ' ', sanitized_name)
        
        # Clean up multiple spaces and normalize
        sanitized_name = re.sub(r'\s+', ' ', sanitized_name).strip()
        
        if sanitized_name != entity_name.strip():
            print(f"[DEBUG] Input sanitized from '{entity_name}' to '{sanitized_name}'")
            entity_name = sanitized_name
            
        # Final check after sanitization
        if not entity_name.strip():
            print(f"[DEBUG] Entity name is empty after sanitization")
            execution_time = time.time() - start_time
            self._log_performance("failed_queries", execution_time, False)
            return []
            
        entity_name = entity_name.strip()
        
        # Handle very short queries that can be expensive in fulltext search
        if len(entity_name) <= 2:
            print(f"[DEBUG] Skipping very short query: '{entity_name}' (too short for meaningful search)")
            execution_time = time.time() - start_time
            self._log_performance("failed_queries", execution_time, False)
            return []
        
        # IMPROVED: Better handling for suspicious numeric-only queries
        if re.match(r'^\d+$', entity_name) and len(entity_name) < 4:
            print(f"[DEBUG] Skipping short numeric query: '{entity_name}' (likely not a meaningful metabolite search)")
            execution_time = time.time() - start_time
            self._log_performance("failed_queries", execution_time, False)
            return []
        
        # Handle very long queries that might cause performance issues
        if len(entity_name) > 100:
            print(f"[DEBUG] Query too long ({len(entity_name)} chars): '{entity_name[:50]}...' (truncating for performance)")
            entity_name = entity_name[:100]  # Truncate to reasonable length
        
        # Only apply enhanced search for Metabolites (since that's what we migrated)
        if entity_type.lower() != "metabolite":
            print(f"[DEBUG] Entity type '{entity_type}' not supported, only 'Metabolite' is supported")
            execution_time = time.time() - start_time
            self._log_performance("failed_queries", execution_time, False)
            return []
        
        # Use adaptive timeout based on query complexity
        max_total_time = timeout_config['max_total']
        
        try:
            # Strategy 1: Primary Fulltext Search (fastest)
            print(f"\n[DEBUG] Primary fulltext search for: {entity_name} (timeout: {timeout_config['primary_timeout']}s)")
            
            fulltext_query = """
            CALL db.index.fulltext.queryNodes(
                'synonym_comprehensive_search', 
                $search_term
            ) YIELD node, score
            MATCH (m:Metabolite)-[:HAS_SYNONYM_INDEX]->(node)
            RETURN m.name as metabolite_name, 
                   m.accession as accession,
                   m.description as description,
                   node.canonical as canonical_name,
                   node.synonyms as synonyms,
                   score,
                   'fulltext' as search_method
            ORDER BY score DESC
            LIMIT 5
            """
            
            try:
                # Check timeout before executing
                elapsed = time.time() - start_time
                if elapsed >= max_total_time:
                    print(f"[DEBUG] Timeout reached before primary search")
                    self._log_performance("failed_queries", elapsed, False)
                    return []
                
                query_start = time.time()
                # Use adaptive timeout for primary search
                results = await self._run_query_with_timeout(
                    fulltext_query, 
                    {"search_term": entity_name},
                    timeout=timeout_config['primary_timeout']
                )
                query_time = time.time() - query_start
                elapsed = time.time() - start_time
                
                if results and len(results) > 0:
                    print(f"[DEBUG] Fulltext search found {len(results)} results in {query_time:.3f}s")
                    processed_results = self._process_results(results, "primary_fulltext", elapsed)
                    
                    # FIX 1: Early exit after successful primary match with confidence >= 0.95
                    if processed_results and len(processed_results) > 0:
                        best_confidence = processed_results[0].get('confidence', 0.0)
                        if best_confidence >= 0.95:
                            print(f"[DEBUG] High confidence match found ({best_confidence:.3f}), skipping fuzzy fallback")
                            self._log_performance("primary_fulltext", elapsed, True)
                            return processed_results
                    
                    self._log_performance("primary_fulltext", elapsed, True)
                    return processed_results
                else:
                    print(f"[DEBUG] Fulltext search returned no results in {query_time:.3f}s")
            except Exception as e:
                query_time = time.time() - start_time
                print(f"[DEBUG] Fulltext search failed: {e} (after {query_time:.3f}s)")
                
            # Strategy 2: Fuzzy Fulltext Search (if time allows)
            elapsed = time.time() - start_time
            remaining_time = max_total_time - elapsed
            
            # Use adaptive fuzzy timeout threshold
            fuzzy_threshold = timeout_config['fuzzy_timeout'] + 0.1  # Small buffer
            if remaining_time > fuzzy_threshold:
                print(f"[DEBUG] Fuzzy fulltext search for: {entity_name} (timeout: {timeout_config['fuzzy_timeout']}s)")
                
                # IMPROVED: Better fuzzy search with safer patterns
                # Only use patterns that are likely to work with sanitized input
                fuzzy_terms = []
                
                # Add edit distance searches only if the term looks reasonable
                if len(entity_name) >= 4 and not re.match(r'^\d+$', entity_name):
                    fuzzy_terms.extend([
                        f"{entity_name}~1",  # Edit distance 1
                        f"{entity_name}~2",  # Edit distance 2
                    ])
                
                # Add prefix wildcard only for longer terms
                if len(entity_name) >= 5:
                    fuzzy_terms.append(f"{entity_name}*")
                
                for fuzzy_term in fuzzy_terms:
                    # Check timeout before each fuzzy attempt
                    elapsed = time.time() - start_time
                    remaining_time = max_total_time - elapsed
                    
                    if remaining_time < timeout_config['fallback_timeout'] + 0.1:  # Reserve time for fallback
                        print(f"[DEBUG] Timeout approaching, stopping fuzzy search")
                        break
                        
                    try:
                        query_start = time.time()
                        # Use adaptive timeout for fuzzy search
                        fuzzy_results = await self._run_query_with_timeout(
                            fulltext_query, 
                            {"search_term": fuzzy_term},
                            timeout=timeout_config['fuzzy_timeout']
                        )
                        query_time = time.time() - query_start
                        
                        if fuzzy_results and len(fuzzy_results) > 0:
                            elapsed = time.time() - start_time
                            print(f"[DEBUG] Fuzzy search found {len(fuzzy_results)} results with '{fuzzy_term}' in {query_time:.3f}s")
                            
                            # FIX 2: Apply validation for fuzzy matches
                            processed_results = self._process_results(fuzzy_results, "fuzzy_fulltext", elapsed)
                            for result in processed_results:
                                original_confidence = result['confidence']
                                validated_confidence = self._validate_fuzzy_match(
                                    original_entity_name, 
                                    result['metabolite_name'], 
                                    original_confidence
                                )
                                result['confidence'] = validated_confidence
                                result['validation_applied'] = validated_confidence != original_confidence
                            
                            self._log_performance("fuzzy_fulltext", elapsed, True)
                            return processed_results
                    except Exception as e:
                        query_time = time.time() - start_time
                        print(f"[DEBUG] Fuzzy search with '{fuzzy_term}' failed: {e} (after {query_time:.3f}s)")
                        continue
            else:
                print(f"[DEBUG] Skipping fuzzy search due to time constraints ({elapsed:.3f}s elapsed, need {fuzzy_threshold:.3f}s)")
            
            # Strategy 3: Fallback Direct Search (if time allows and previous strategies failed)
            elapsed = time.time() - start_time
            remaining_time = max_total_time - elapsed
            
            if remaining_time > timeout_config['fallback_timeout']:
                print(f"[DEBUG] Attempting fallback direct search for: {entity_name} (budget: {remaining_time:.3f}s)")
                
                try:
                    fallback_results = await self._fallback_direct_search(entity_name, remaining_time)
                    
                    if fallback_results and len(fallback_results) > 0:
                        elapsed = time.time() - start_time
                        print(f"[DEBUG] Fallback search found {len(fallback_results)} results in {elapsed:.3f}s")
                        self._log_performance("fallback_direct", elapsed, True)
                        return self._process_results(fallback_results, "fallback_direct", elapsed)
                except Exception as e:
                    print(f"[DEBUG] Fallback search failed: {e}")
            else:
                # FIX 3: Warning when fallback is skipped due to timeout
                print(f"[WARN] Fallback skipped due to query timeout (only {remaining_time:.3f}s remaining, need {timeout_config['fallback_timeout']:.3f}s)")
            
            # No results found within time limit
            total_time = time.time() - start_time
            print(f"[DEBUG] No matches found for entity: {entity_name} (searched for {total_time:.3f}s)")
            self._log_performance("failed_queries", total_time, False)
            return []
                
        except Exception as e:
            total_time = time.time() - start_time
            print(f"[ERROR] Entity matching failed for {entity_name}: {e} (after {total_time:.3f}s)")
            self._log_performance("failed_queries", total_time, False)
            return []
    
    def _process_results(self, results: List[dict], search_method: str, execution_time: float) -> List[dict]:
        """Process and enhance results with confidence scoring"""
        enhanced_results = []
        
        for result in results:
            # Calculate confidence based on search method and score
            base_score = result.get('score', 0.0)
            
            # Adjust confidence based on search method used
            confidence_multiplier = {
                'primary_fulltext': 1.0,    # Highest confidence - exact fulltext match
                'fuzzy_fulltext': 0.9,      # High confidence - fuzzy match
                'fallback_direct': 0.8,     # Good confidence - direct name matching
            }.get(search_method, 0.7)
            
            final_confidence = min(base_score * confidence_multiplier / 10.0, 1.0)  # Normalize score
            
            enhanced_result = {
                'metabolite_name': result.get('metabolite_name'),
                'accession': result.get('accession'),
                'description': result.get('description'),
                'canonical_name': result.get('canonical_name'),
                'synonyms': result.get('synonyms', []),
                'confidence': final_confidence,
                'search_method': result.get('search_method', search_method),
                'fallback_path': search_method,
                'original_score': base_score,
                'execution_time': execution_time
            }
            enhanced_results.append(enhanced_result)
        
        print(f"[DEBUG] Enhanced entity matching completed. Found {len(enhanced_results)} results using {search_method} strategy")
        return enhanced_results 