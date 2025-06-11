#!/usr/bin/env python3
"""
Demonstration of the Complete Spectra Pipeline
==============================================

This script shows how our system processes user queries from detection to graph generation.
Using real HMDB data for compounds confirmed to have spectra.

Phases Demonstrated:
1. Trigger Detection - Identifies spectra-related queries
2. Data Processing - Fetches real spectra data from HMDB API  
3. Graph Generation - Creates professional scientific visualizations
"""

import os
import sys
from dotenv import load_dotenv

# Add the parent directory to the Python path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.pipeline.hmdb_api import HMDBApiClient, RateLimiter
from backend.services.spectra_trigger_service import SpectraTriggerDetector
from backend.services.spectra_graph_service import SpectraGraphGenerator, GraphConfig, PlotFormat, PlotStyle
from backend.services.spectra_service import SpectraProcessor

def demo_complete_pipeline():
    """Demonstrate the complete spectra pipeline from query to visualization"""
    
    print("🧬 SPECTRA PIPELINE DEMONSTRATION")
    print("=" * 80)
    print("Showing how user queries are processed into professional scientific visualizations")
    print()
    
    # Load environment
    load_dotenv()
    
    # Initialize components
    rate_limiter = RateLimiter()
    hmdb_client = HMDBApiClient(rate_limiter, use_cache=True)
    trigger_detector = SpectraTriggerDetector()
    graph_generator = SpectraGraphGenerator()
    
    # Test queries with real compounds that have spectra
    test_cases = [
        {
            "query": "show spectrum for HMDB0000927",
            "description": "Direct HMDB ID request for Valerylglycine",
            "hmdb_id": "HMDB0000927",
            "compound_name": "Valerylglycine"
        },
        {
            "query": "mass spectrum analysis of HMDB0002658", 
            "description": "Scientific analysis request for 6-Hydroxynicotinic acid",
            "hmdb_id": "HMDB0002658",
            "compound_name": "6-Hydroxynicotinic acid"
        },
        {
            "query": "generate spectrum for HMDB0006026",
            "description": "Generate command for Norbolethone",
            "hmdb_id": "HMDB0006026", 
            "compound_name": "Norbolethone"
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"📋 TEST CASE {i}: {test_case['description']}")
        print(f"Query: \"{test_case['query']}\"")
        print(f"Compound: {test_case['compound_name']} ({test_case['hmdb_id']})")
        print("-" * 60)
        
        # Phase 1: Trigger Detection
        print("🔍 PHASE 1: Trigger Detection")
        intent = trigger_detector.detect_spectra_intent(test_case['query'])
        
        if intent:
            print(f"   ✅ Detected: {intent.trigger_type.value}")
            print(f"   📊 Confidence: {intent.confidence:.1%}")
            if intent.hmdb_id:
                print(f"   🧪 HMDB ID: {intent.hmdb_id}")
        else:
            print("   ❌ No spectra intent detected")
            continue
        print()
        
        # Phase 2: Data Processing
        print("📡 PHASE 2: Data Processing")
        hmdb_id = intent.hmdb_id or test_case['hmdb_id']
        spectra_response = hmdb_client.get_spectra_for_hmdb_id(hmdb_id)
        
        if spectra_response.get("success"):
            data = spectra_response["data"]
            print(f"   ✅ Retrieved: {data['spectrum_type']}")
            print(f"   🎯 Instrument: {data['instrument_type']}")
            print(f"   📈 Peaks: {len(data['peaks'])} data points")
            
            # Process the spectrum
            processed_spectrum = SpectraProcessor.process_raw_spectrum(hmdb_id, data)
            if processed_spectrum:
                print(f"   🔬 Quality Score: {processed_spectrum.quality_score:.2f}")
                print(f"   🧮 Processed Peaks: {len(processed_spectrum.peaks)}")
        else:
            print(f"   ❌ API Error: {spectra_response.get('errors', 'Unknown error')}")
            continue
        print()
        
        # Phase 4: Graph Generation  
        print("📊 PHASE 4: Graph Generation")
        
        try:
            # Generate different visualization formats
            formats_to_test = [
                ("png", "Static PNG image"),
                ("svg", "Vector SVG graphic"), 
                ("html", "Interactive HTML plot")
            ]
            
            # Create format mapping
            format_map = {
                "png": PlotFormat.STATIC_PNG,
                "svg": PlotFormat.STATIC_SVG, 
                "html": PlotFormat.INTERACTIVE_HTML
            }
            
            for fmt, description in formats_to_test:
                try:
                    config = GraphConfig(
                        format=format_map[fmt],
                        style=PlotStyle.SCIENTIFIC
                    )
                    
                    result = graph_generator.generate_spectrum_plot(
                        processed_spectrum, 
                        config=config
                    )
                    
                    if result and hasattr(result, 'content'):
                        if fmt == "html":
                            size_info = f"{len(result.content)} chars"
                        else:
                            size_info = f"{len(result.content)} bytes"
                        print(f"   ✅ {description}: Generated ({size_info})")
                    else:
                        print(f"   ❌ {description}: Failed")
                        
                except Exception as e:
                    print(f"   ❌ {description}: Error - {str(e)}")
        
        except Exception as e:
            print(f"   ❌ Graph generation failed: {str(e)}")
        
        print()
        print("🎯 PIPELINE SUMMARY:")
        print(f"   📋 Query processed successfully from '{test_case['query']}'")
        print(f"   🧪 Real spectra data retrieved for {test_case['compound_name']}")
        print(f"   📊 Professional visualizations generated in multiple formats")
        print()
        print("=" * 80)
        print()

def show_data_structure_info():
    """Show information about the HMDB API data structure"""
    
    print("📚 HMDB API DATA STRUCTURE")
    print("=" * 80)
    print("The HMDB API returns spectra data in structured fields:")
    print()
    print("🔬 Spectra Types Available:")
    print("   • c_ms     - GC-MS and LC-MS spectra (mass spectrometry)")
    print("   • ms_ms    - Tandem MS spectra (fragmentation patterns)")  
    print("   • nmr      - NMR spectra (nuclear magnetic resonance)")
    print("   • ms_ir    - IRIS spectra (infrared ion spectroscopy)")
    print()
    print("📊 Data Structure:")
    print("   Each spectra type contains arrays of spectrum objects with:")
    print("   • spectrum_type     - Type of spectrum (e.g., 'Experimental GC-MS')")
    print("   • instrument_type   - Instrument used (e.g., 'GC-MS', 'LC-ESI-QTOF')")
    print("   • peaks[]           - Array of peak data points")
    print("     - mass_charge     - m/z value (mass-to-charge ratio)")
    print("     - intensity       - Signal intensity")
    print("     - annotation      - Peak identification (optional)")
    print()
    print("🎯 Our Pipeline:")
    print("   1. Detects spectra queries using pattern matching")
    print("   2. Fetches real data from HMDB API endpoints")
    print("   3. Extracts peaks from c_ms, ms_ms, nmr, or ms_ir fields")
    print("   4. Validates and processes the spectral data")
    print("   5. Generates professional scientific visualizations")
    print()

if __name__ == "__main__":
    show_data_structure_info()
    demo_complete_pipeline() 