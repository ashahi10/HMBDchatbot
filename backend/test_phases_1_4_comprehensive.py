"""
Comprehensive Test Suite for Phases 1-4: Spectra Functionality System

This test script validates the complete spectra functionality pipeline including:
- Phase 1: Trigger Detection Service (spectra command recognition)
- Phase 2: Data Processing Service (HMDB API integration and validation)
- Phase 3: Entity Matching Integration (optimized search for metabolites)
- Phase 4: Graph Generation Service (interactive visualizations)

Test Coverage:
- Unit tests for individual services
- Integration tests for end-to-end pipeline
- Performance and reliability testing
- Error handling and edge cases
- Visualization output validation

Author: Senior Engineering Implementation
Version: 1.0.0
Test Suite: Phases 1-4 Complete
"""

import os
import sys
import time
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
import json

# Add backend to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

# Phase imports
from backend.services.spectra_trigger_service import SpectraTriggerDetector, SpectraPipelineIntegrator, SpectraTriggerType
from backend.services.spectra_service import SpectraProcessor, SpectraValidator, ProcessedSpectrum, SpectrumType
from backend.services.spectra_graph_service import SpectraVisualizationPipeline, PlotFormat, PlotStyle, GraphConfig
from backend.pipeline.hmdb_api import HMDBApiClient, RateLimiter
from backend.utils.neo4j_connection import Neo4jConnection
from backend.pipeline.optimized_entity_matcher import OptimizedEntityMatcher

# Load environment variables
load_dotenv()

class ComprehensiveSpectraTestSuite:
    """
    Complete test suite for the spectra functionality system.
    Tests all phases from trigger detection to visualization generation.
    """
    
    def __init__(self):
        """Initialize test environment and connections"""
        self.neo4j_connection = None
        self.hmdb_client = None
        self.entity_matcher = None
        self.test_results = {
            'phase_1': {'passed': 0, 'failed': 0, 'tests': []},
            'phase_2': {'passed': 0, 'failed': 0, 'tests': []},
            'phase_3': {'passed': 0, 'failed': 0, 'tests': []},
            'phase_4': {'passed': 0, 'failed': 0, 'tests': []},
            'integration': {'passed': 0, 'failed': 0, 'tests': []}
        }
        
    async def setup(self) -> bool:
        """Initialize all services and connections"""
        print("🔧 Setting up comprehensive test environment...")
        
        try:
            # Setup Neo4j connection
            neo4j_uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
            neo4j_user = os.getenv("NEO4J_USERNAME")
            neo4j_password = os.getenv("NEO4J_PASSWORD")
            
            self.neo4j_connection = Neo4jConnection(neo4j_uri, neo4j_user, neo4j_password)
            
            # Setup HMDB client
            rate_limiter = RateLimiter()
            self.hmdb_client = HMDBApiClient(rate_limiter=rate_limiter, use_cache=True)
            
            # Setup entity matcher
            self.entity_matcher = OptimizedEntityMatcher(self.neo4j_connection)
            
            print("✅ Test environment ready")
            return True
            
        except Exception as e:
            print(f"❌ Setup failed: {e}")
            return False
    
    def cleanup(self):
        """Clean up test environment"""
        if self.neo4j_connection:
            try:
                self.neo4j_connection.close()
                print("🧹 Cleanup completed")
            except Exception as e:
                print(f"🧹 Cleanup completed with warnings: {e}")
    
    def _record_test_result(self, phase: str, test_name: str, passed: bool, details: str = "", execution_time: float = 0.0):
        """Record test result"""
        result = {
            'test_name': test_name,
            'passed': passed,
            'details': details,
            'execution_time': execution_time,
            'timestamp': datetime.now().isoformat()
        }
        
        self.test_results[phase]['tests'].append(result)
        if passed:
            self.test_results[phase]['passed'] += 1
        else:
            self.test_results[phase]['failed'] += 1
        
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"   {status}: {test_name} ({execution_time:.3f}s)")
        if details and not passed:
            print(f"      Details: {details}")
    
    async def test_phase_1_trigger_detection(self):
        """Test Phase 1: Spectra Trigger Detection Service"""
        print("\n🧪 PHASE 1: TRIGGER DETECTION TESTS")
        print("=" * 60)
        
        detector = SpectraTriggerDetector(self.neo4j_connection)
        
        # Test cases for trigger detection
        test_cases = [
            # Explicit commands
            {
                "query": "show spectra for glucose",
                "should_detect": True,
                "expected_type": SpectraTriggerType.EXPLICIT_COMMAND,
                "description": "Explicit command detection"
            },
            {
                "query": "generate spectrum of HMDB0000927",
                "should_detect": True,
                "expected_type": SpectraTriggerType.BUTTON_TRIGGER,
                "description": "HMDB ID with command"
            },
            {
                "query": "glucose spectrum",
                "should_detect": True,
                "expected_type": SpectraTriggerType.METABOLITE_NAME_WITH_SPECTRA,
                "description": "Metabolite name with spectrum"
            },
            {
                "query": "mass spectrum analysis of citric acid",
                "should_detect": True,
                "expected_type": SpectraTriggerType.IMPLICIT_SCIENTIFIC,
                "description": "Scientific analysis term"
            },
            {
                "query": "what is the molecular weight of glucose",
                "should_detect": False,
                "expected_type": None,
                "description": "Non-spectrum query"
            },
            # Edge cases
            {
                "query": "spectrum",
                "should_detect": True,
                "expected_type": SpectraTriggerType.METABOLITE_NAME_WITH_SPECTRA,
                "description": "Single word spectrum"
            },
            {
                "query": "show me the GC-MS for ethanol",
                "should_detect": True,
                "expected_type": SpectraTriggerType.IMPLICIT_SCIENTIFIC,
                "description": "GC-MS instrument mention"
            }
        ]
        
        for test_case in test_cases:
            start_time = time.time()
            
            try:
                intent = detector.detect_spectra_intent(test_case["query"])
                execution_time = time.time() - start_time
                
                if test_case["should_detect"]:
                    if intent and intent.trigger_type == test_case["expected_type"]:
                        self._record_test_result(
                            "phase_1", 
                            test_case["description"], 
                            True, 
                            f"Detected {intent.trigger_type.value} with confidence {intent.confidence:.2f}",
                            execution_time
                        )
                    else:
                        self._record_test_result(
                            "phase_1", 
                            test_case["description"], 
                            False, 
                            f"Expected {test_case['expected_type']}, got {intent.trigger_type if intent else None}",
                            execution_time
                        )
                else:
                    if intent is None:
                        self._record_test_result(
                            "phase_1", 
                            test_case["description"], 
                            True, 
                            "Correctly detected no spectrum intent",
                            execution_time
                        )
                    else:
                        self._record_test_result(
                            "phase_1", 
                            test_case["description"], 
                            False, 
                            f"False positive: detected {intent.trigger_type.value}",
                            execution_time
                        )
                        
            except Exception as e:
                execution_time = time.time() - start_time
                self._record_test_result(
                    "phase_1", 
                    test_case["description"], 
                    False, 
                    f"Exception: {str(e)}",
                    execution_time
                )
        
        # Test HMDB ID extraction
        start_time = time.time()
        try:
            hmdb_query = "show spectrum for HMDB0000927"
            intent = detector.detect_spectra_intent(hmdb_query)
            execution_time = time.time() - start_time
            
            if intent and intent.hmdb_id == "HMDB0000927":
                self._record_test_result(
                    "phase_1", 
                    "HMDB ID extraction", 
                    True, 
                    f"Correctly extracted {intent.hmdb_id}",
                    execution_time
                )
            else:
                self._record_test_result(
                    "phase_1", 
                    "HMDB ID extraction", 
                    False, 
                    f"Expected HMDB0000927, got {intent.hmdb_id if intent else None}",
                    execution_time
                )
        except Exception as e:
            execution_time = time.time() - start_time
            self._record_test_result(
                "phase_1", 
                "HMDB ID extraction", 
                False, 
                f"Exception: {str(e)}",
                execution_time
            )
    
    async def test_phase_2_data_processing(self):
        """Test Phase 2: Spectra Data Processing Service"""
        print("\n🧪 PHASE 2: DATA PROCESSING TESTS")
        print("=" * 60)
        
        # Test spectra API call
        start_time = time.time()
        try:
            # Test with a known HMDB ID that actually has spectra data
            test_hmdb_id = "HMDB0000927"  # Valerylglycine - confirmed to have spectra
            spectra_response = self.hmdb_client.get_spectra_for_hmdb_id(test_hmdb_id)
            execution_time = time.time() - start_time
            
            if spectra_response.get("success", False):
                self._record_test_result(
                    "phase_2", 
                    "HMDB API spectra fetch", 
                    True, 
                    f"Retrieved spectra for {test_hmdb_id}",
                    execution_time
                )
                
                # Test data validation
                start_time = time.time()
                data = spectra_response.get("data", {})
                is_valid, errors = SpectraValidator.validate_raw_response(data)
                execution_time = time.time() - start_time
                
                self._record_test_result(
                    "phase_2", 
                    "Spectra data validation", 
                    is_valid, 
                    f"Validation {'passed' if is_valid else 'failed'}: {errors}",
                    execution_time
                )
                
                # Test spectrum processing
                if is_valid and data:
                    start_time = time.time()
                    processed_spectrum = SpectraProcessor.process_raw_spectrum(test_hmdb_id, data)
                    execution_time = time.time() - start_time
                    
                    if processed_spectrum:
                        self._record_test_result(
                            "phase_2", 
                            "Spectrum data processing", 
                            True, 
                            f"Processed {len(processed_spectrum.peaks)} peaks, quality: {processed_spectrum.quality_score:.2f}",
                            execution_time
                        )
                        
                        # Store for later tests
                        self.test_processed_spectrum = processed_spectrum
                        
                    else:
                        self._record_test_result(
                            "phase_2", 
                            "Spectrum data processing", 
                            False, 
                            "Failed to process valid spectrum data",
                            execution_time
                        )
                        
            else:
                self._record_test_result(
                    "phase_2", 
                    "HMDB API spectra fetch", 
                    False, 
                    f"API call failed: {spectra_response.get('errors', 'Unknown error')}",
                    execution_time
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            self._record_test_result(
                "phase_2", 
                "HMDB API spectra fetch", 
                False, 
                f"Exception: {str(e)}",
                execution_time
            )
        
        # Test error handling with invalid HMDB ID
        start_time = time.time()
        try:
            invalid_response = self.hmdb_client.get_spectra_for_hmdb_id("INVALID_ID")
            execution_time = time.time() - start_time
            
            if not invalid_response.get("success", True):
                self._record_test_result(
                    "phase_2", 
                    "Invalid HMDB ID handling", 
                    True, 
                    "Correctly handled invalid HMDB ID",
                    execution_time
                )
            else:
                self._record_test_result(
                    "phase_2", 
                    "Invalid HMDB ID handling", 
                    False, 
                    "Should have failed with invalid HMDB ID",
                    execution_time
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            self._record_test_result(
                "phase_2", 
                "Invalid HMDB ID handling", 
                True, 
                f"Exception correctly raised: {str(e)}",
                execution_time
            )
    
    async def test_phase_3_entity_integration(self):
        """Test Phase 3: Integration with Entity Matching"""
        print("\n🧪 PHASE 3: ENTITY INTEGRATION TESTS")
        print("=" * 60)
        
        # Test entity matching for metabolites
        test_metabolites = [
            ("glucose", "Should find D-Glucose"),
            ("citric acid", "Should find citric acid"),
            ("ATP", "Should find adenosine triphosphate"),
            ("nonexistent_metabolite_xyz123", "Should handle unknown metabolite")
        ]
        
        for metabolite_name, description in test_metabolites:
            start_time = time.time()
            
            try:
                results = await self.entity_matcher.match_entities(metabolite_name, 'Metabolite')
                execution_time = time.time() - start_time
                
                if metabolite_name == "nonexistent_metabolite_xyz123":
                    # This should not find results
                    if not results or len(results) == 0:
                        self._record_test_result(
                            "phase_3", 
                            f"Entity matching: {description}", 
                            True, 
                            "Correctly found no matches for nonexistent metabolite",
                            execution_time
                        )
                    else:
                        self._record_test_result(
                            "phase_3", 
                            f"Entity matching: {description}", 
                            False, 
                            f"Should not have found matches, but got {len(results)}",
                            execution_time
                        )
                else:
                    # These should find results
                    if results and len(results) > 0:
                        best_match = results[0]
                        self._record_test_result(
                            "phase_3", 
                            f"Entity matching: {description}", 
                            True, 
                            f"Found {best_match['metabolite_name']} with confidence {best_match.get('confidence', 0):.2f}",
                            execution_time
                        )
                    else:
                        self._record_test_result(
                            "phase_3", 
                            f"Entity matching: {description}", 
                            False, 
                            "No matches found for known metabolite",
                            execution_time
                        )
                        
            except Exception as e:
                execution_time = time.time() - start_time
                self._record_test_result(
                    "phase_3", 
                    f"Entity matching: {description}", 
                    False, 
                    f"Exception: {str(e)}",
                    execution_time
                )
        
        # Test performance
        start_time = time.time()
        try:
            performance_results = []
            test_queries = ["glucose", "ATP", "citric acid", "pyruvate", "lactate"]
            
            for query in test_queries:
                query_start = time.time()
                results = await self.entity_matcher.match_entities(query, 'Metabolite')
                query_time = time.time() - query_start
                performance_results.append(query_time)
            
            execution_time = time.time() - start_time
            avg_time = sum(performance_results) / len(performance_results)
            max_time = max(performance_results)
            
            # Performance should be under 2 seconds per query
            if max_time < 2.0 and avg_time < 1.0:
                self._record_test_result(
                    "phase_3", 
                    "Entity matching performance", 
                    True, 
                    f"Average: {avg_time:.3f}s, Max: {max_time:.3f}s",
                    execution_time
                )
            else:
                self._record_test_result(
                    "phase_3", 
                    "Entity matching performance", 
                    False, 
                    f"Too slow - Average: {avg_time:.3f}s, Max: {max_time:.3f}s",
                    execution_time
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            self._record_test_result(
                "phase_3", 
                "Entity matching performance", 
                False, 
                f"Exception: {str(e)}",
                execution_time
            )
    
    async def test_phase_4_graph_generation(self):
        """Test Phase 4: Graph Generation Service"""
        print("\n🧪 PHASE 4: GRAPH GENERATION TESTS")
        print("=" * 60)
        
        # We need processed spectrum data for this test
        if not hasattr(self, 'test_processed_spectrum') or not self.test_processed_spectrum:
            print("⚠️  Skipping Phase 4 tests - no processed spectrum data available")
            self._record_test_result("phase_4", "Graph generation setup", False, "No test data available", 0.0)
            return
        
        visualization_pipeline = SpectraVisualizationPipeline()
        
        # Test different output formats
        formats_to_test = [
            (PlotFormat.STATIC_PNG, "Static PNG generation"),
            (PlotFormat.STATIC_SVG, "Static SVG generation"),
            (PlotFormat.EMBEDDED_BASE64, "Base64 embedded generation")
        ]
        
        # Only test interactive HTML if plotly is available
        try:
            import plotly
            formats_to_test.append((PlotFormat.INTERACTIVE_HTML, "Interactive HTML generation"))
        except ImportError:
            print("   Note: Plotly not available, skipping interactive tests")
        
        for plot_format, description in formats_to_test:
            start_time = time.time()
            
            try:
                result = visualization_pipeline.create_spectrum_visualization(
                    self.test_processed_spectrum,
                    output_format=plot_format,
                    style=PlotStyle.SCIENTIFIC
                )
                execution_time = time.time() - start_time
                
                if result.get("success", False):
                    graph_data = result.get("graph_data", {})
                    content = graph_data.get("content") or graph_data.get("base64")
                    
                    if content:
                        self._record_test_result(
                            "phase_4", 
                            description, 
                            True, 
                            f"Generated plot with {len(str(content))} characters",
                            execution_time
                        )
                    else:
                        self._record_test_result(
                            "phase_4", 
                            description, 
                            False, 
                            "No content in generated plot",
                            execution_time
                        )
                else:
                    error = result.get("error", "Unknown error")
                    self._record_test_result(
                        "phase_4", 
                        description, 
                        False, 
                        f"Visualization failed: {error}",
                        execution_time
                    )
                    
            except Exception as e:
                execution_time = time.time() - start_time
                self._record_test_result(
                    "phase_4", 
                    description, 
                    False, 
                    f"Exception: {str(e)}",
                    execution_time
                )
        
        # Test different styles
        styles_to_test = [
            PlotStyle.SCIENTIFIC,
            PlotStyle.DARK_MODE,
            PlotStyle.PUBLICATION,
            PlotStyle.MINIMAL
        ]
        
        for style in styles_to_test:
            start_time = time.time()
            
            try:
                result = visualization_pipeline.create_spectrum_visualization(
                    self.test_processed_spectrum,
                    output_format=PlotFormat.STATIC_PNG,
                    style=style
                )
                execution_time = time.time() - start_time
                
                if result.get("success", False):
                    self._record_test_result(
                        "phase_4", 
                        f"Style test: {style.value}", 
                        True, 
                        f"Generated plot with {style.value} style",
                        execution_time
                    )
                else:
                    self._record_test_result(
                        "phase_4", 
                        f"Style test: {style.value}", 
                        False, 
                        f"Failed to generate plot with {style.value} style",
                        execution_time
                    )
                    
            except Exception as e:
                execution_time = time.time() - start_time
                self._record_test_result(
                    "phase_4", 
                    f"Style test: {style.value}", 
                    False, 
                    f"Exception: {str(e)}",
                    execution_time
                )
        
        # Test data export
        start_time = time.time()
        try:
            from backend.services.spectra_graph_service import SpectraGraphGenerator
            generator = SpectraGraphGenerator()
            exported_data = generator.export_data(self.test_processed_spectrum, "json")
            execution_time = time.time() - start_time
            
            if exported_data and "hmdb_id" in exported_data:
                self._record_test_result(
                    "phase_4", 
                    "Data export (JSON)", 
                    True, 
                    f"Exported data with {len(exported_data.get('peaks', []))} peaks",
                    execution_time
                )
            else:
                self._record_test_result(
                    "phase_4", 
                    "Data export (JSON)", 
                    False, 
                    "Export failed or incomplete data",
                    execution_time
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            self._record_test_result(
                "phase_4", 
                "Data export (JSON)", 
                False, 
                f"Exception: {str(e)}",
                execution_time
            )
    
    async def test_integration_pipeline(self):
        """Test end-to-end integration of all phases"""
        print("\n🧪 INTEGRATION: END-TO-END PIPELINE TESTS")
        print("=" * 60)
        
        integrator = SpectraPipelineIntegrator(self.neo4j_connection, self.hmdb_client)
        
        # Test full pipeline with various queries
        test_queries = [
            {
                "query": "show spectrum for glucose",
                "description": "Complete pipeline: metabolite name to spectrum",
                "should_succeed": True
            },
            {
                "query": "spectrum of HMDB0000927",
                "description": "Complete pipeline: HMDB ID to spectrum",
                "should_succeed": True
            },
            {
                "query": "generate mass spectrum for citric acid",
                "description": "Complete pipeline: scientific analysis request",
                "should_succeed": True
            },
            {
                "query": "what is the weather today",
                "description": "Non-spectrum query handling",
                "should_succeed": False
            }
        ]
        
        for test_case in test_queries:
            start_time = time.time()
            
            try:
                result = integrator.process_spectra_query(test_case["query"])
                execution_time = time.time() - start_time
                
                is_spectra_query = result.get("is_spectra_query", False)
                
                if test_case["should_succeed"]:
                    if is_spectra_query:
                        # Check for success conditions directly in result
                        if result.get("success", False) or result.get("requires_disambiguation", False) or result.get("spectra_data", {}).get("success", False):
                            self._record_test_result(
                                "integration", 
                                test_case["description"], 
                                True, 
                                "Pipeline successfully processed spectrum query",
                                execution_time
                            )
                        else:
                            self._record_test_result(
                                "integration", 
                                test_case["description"], 
                                False, 
                                f"Pipeline failed: {result.get('error', 'Unknown error')}",
                                execution_time
                            )
                    else:
                        self._record_test_result(
                            "integration", 
                            test_case["description"], 
                            False, 
                            "Failed to detect spectrum intent",
                            execution_time
                        )
                else:
                    # Should not be detected as spectrum query
                    if not is_spectra_query:
                        self._record_test_result(
                            "integration", 
                            test_case["description"], 
                            True, 
                            "Correctly ignored non-spectrum query",
                            execution_time
                        )
                    else:
                        self._record_test_result(
                            "integration", 
                            test_case["description"], 
                            False, 
                            "False positive: detected spectrum intent",
                            execution_time
                        )
                        
            except Exception as e:
                execution_time = time.time() - start_time
                self._record_test_result(
                    "integration", 
                    test_case["description"], 
                    False, 
                    f"Exception: {str(e)}",
                    execution_time
                )
        
        # Test performance of complete pipeline
        start_time = time.time()
        try:
            performance_query = "show spectrum for glucose"
            result = integrator.process_spectra_query(performance_query)
            execution_time = time.time() - start_time
            
            # Complete pipeline should run in reasonable time
            if execution_time < 10.0:  # 10 seconds for complete pipeline
                self._record_test_result(
                    "integration", 
                    "Complete pipeline performance", 
                    True, 
                    f"Pipeline completed in {execution_time:.3f}s",
                    execution_time
                )
            else:
                self._record_test_result(
                    "integration", 
                    "Complete pipeline performance", 
                    False, 
                    f"Pipeline too slow: {execution_time:.3f}s",
                    execution_time
                )
                
        except Exception as e:
            execution_time = time.time() - start_time
            self._record_test_result(
                "integration", 
                "Complete pipeline performance", 
                False, 
                f"Exception: {str(e)}",
                execution_time
            )
    
    def generate_test_report(self) -> Dict[str, Any]:
        """Generate comprehensive test report"""
        total_tests = 0
        total_passed = 0
        total_failed = 0
        
        for phase, results in self.test_results.items():
            total_tests += results['passed'] + results['failed']
            total_passed += results['passed']
            total_failed += results['failed']
        
        success_rate = (total_passed / total_tests * 100) if total_tests > 0 else 0
        
        report = {
            'summary': {
                'total_tests': total_tests,
                'total_passed': total_passed,
                'total_failed': total_failed,
                'success_rate': success_rate,
                'timestamp': datetime.now().isoformat()
            },
            'phases': self.test_results,
            'recommendations': []
        }
        
        # Add recommendations based on results
        for phase, results in self.test_results.items():
            if results['failed'] > 0:
                phase_success_rate = (results['passed'] / (results['passed'] + results['failed']) * 100)
                if phase_success_rate < 80:
                    report['recommendations'].append(
                        f"Phase {phase.upper()}: {results['failed']} tests failed ({phase_success_rate:.1f}% success rate). Requires attention."
                    )
        
        if success_rate < 90:
            report['recommendations'].append("Overall success rate below 90%. System needs improvement before production use.")
        elif success_rate < 95:
            report['recommendations'].append("Good overall performance. Minor improvements recommended.")
        else:
            report['recommendations'].append("Excellent performance. System ready for production use.")
        
        return report
    
    async def run_all_tests(self):
        """Run complete test suite"""
        print("🚀 STARTING COMPREHENSIVE SPECTRA SYSTEM TEST SUITE")
        print("=" * 80)
        print(f"Test Environment: {os.getenv('NEO4J_URI', 'Not configured')}")
        print(f"Timestamp: {datetime.now().isoformat()}")
        print("=" * 80)
        
        # Setup
        if not await self.setup():
            print("❌ Test setup failed. Aborting test suite.")
            return None
        
        try:
            # Run all test phases
            await self.test_phase_1_trigger_detection()
            await self.test_phase_2_data_processing()
            await self.test_phase_3_entity_integration()
            await self.test_phase_4_graph_generation()
            await self.test_integration_pipeline()
            
            # Generate report
            report = self.generate_test_report()
            
            # Print summary
            print("\n" + "=" * 80)
            print("📊 TEST SUITE SUMMARY")
            print("=" * 80)
            
            summary = report['summary']
            print(f"Total Tests: {summary['total_tests']}")
            print(f"Passed: {summary['total_passed']} ✅")
            print(f"Failed: {summary['total_failed']} ❌")
            print(f"Success Rate: {summary['success_rate']:.1f}%")
            
            # Phase breakdown
            print("\n📋 PHASE BREAKDOWN:")
            for phase, results in self.test_results.items():
                total_phase = results['passed'] + results['failed']
                if total_phase > 0:
                    phase_rate = (results['passed'] / total_phase * 100)
                    print(f"  {phase.upper()}: {results['passed']}/{total_phase} ({phase_rate:.1f}%)")
            
            # Recommendations
            if report['recommendations']:
                print("\n💡 RECOMMENDATIONS:")
                for rec in report['recommendations']:
                    print(f"  • {rec}")
            
            # Overall assessment
            print("\n🎯 OVERALL ASSESSMENT:")
            if summary['success_rate'] >= 95:
                print("  ✅ EXCELLENT: System ready for production deployment")
            elif summary['success_rate'] >= 90:
                print("  ✅ GOOD: System mostly ready, minor improvements needed")
            elif summary['success_rate'] >= 80:
                print("  ⚠️  FAIR: System needs improvements before production")
            else:
                print("  ❌ POOR: System requires significant work before deployment")
            
            return report
            
        finally:
            self.cleanup()

async def main():
    """Main test execution"""
    test_suite = ComprehensiveSpectraTestSuite()
    report = await test_suite.run_all_tests()
    
    # Save detailed report
    if report:
        try:
            with open("test_results_phases_1_4.json", "w") as f:
                json.dump(report, f, indent=2)
            print(f"\n📄 Detailed report saved to: test_results_phases_1_4.json")
        except Exception as e:
            print(f"\n⚠️  Could not save report: {e}")

if __name__ == "__main__":
    asyncio.run(main())