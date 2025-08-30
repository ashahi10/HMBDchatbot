"""
Comprehensive Metabolite Data Extractor for LLM Question Generation

This module leverages the existing HMDB API client to extract rich metabolite information
for 20 selected metabolites, formatted in JSON for subsequent LLM processing.

Features:
- Utilizes existing HMDB API infrastructure
- Comprehensive field extraction (basic info, concentrations, pathways, etc.)
- Error handling and rate limiting
- JSON output optimized for LLM consumption
- Progress tracking and logging

Author: Senior Engineering Implementation
"""

import os
import sys
import json
import time
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from pathlib import Path

# Add project root to path for imports
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, project_root)

from dotenv import load_dotenv
from backend.pipeline.hmdb_api import HMDBApiClient, RateLimiter, ApiFallbackCoordinator

# Load environment variables
load_dotenv()

class MetaboliteDataExtractor:
    """
    Comprehensive metabolite data extractor using HMDB API
    
    This class orchestrates the extraction of detailed metabolite information
    from the HMDB database using the existing API client infrastructure.
    """
    
    def __init__(self, output_dir: str = None):
        """
        Initialize the metabolite data extractor
        
        Args:
            output_dir: Directory to save extracted data (default: current directory)
        """
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.output_dir.mkdir(exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize HMDB API components
        self.rate_limiter = RateLimiter()
        self.hmdb_client = HMDBApiClient(self.rate_limiter, use_cache=True)
        self.fallback_coordinator = ApiFallbackCoordinator(self.hmdb_client, max_retries=3)
        
        # Define comprehensive fields to extract for each metabolite
        self.comprehensive_fields = [
            # Basic identification
            "hmdb_id", "name", "description", "synonyms", "status",
            
            # Chemical properties
            "moldb_formula", "cas", "moldb_average_mass", "moldb_mono_mass",
            "moldb_smiles", "moldb_inchi", "moldb_inchikey",
            
            # Physical/Chemical properties
            "moldb_alogps_solubility", "moldb_alogps_logp", "moldb_alogps_logs",
            "moldb_pka_strongest_acidic", "moldb_pka_strongest_basic",
            "moldb_physiological_charge", "moldb_acceptor_count", "moldb_donor_count",
            "moldb_polar_surface_area", "moldb_rotatable_bond_count",
            
            # Biological information
            "chemical_taxonomy", "biospecimen_normal", "biospecimen_abnormal",
            
            # Concentration data
            "normal_concentrations", "abnormal_concentrations",
            
            # Pathway and enzyme information
            "smpdb_id", "associated_proteins", "enzyme_name", "gene_name",
            
            # Ontology information
            "health_effect", "physiological_effect", "biological_role",
            "environmental_role", "natural_process", "industrial_process",
            
            # External references
            "external_links", "synthesis_reference", "general_references"
        ]
        
        # Curated list of 20 interesting metabolites for question generation
        self.target_metabolites = [
            "HMDB0000001",  # 1-Methylhistidine
            "HMDB0000122",  # D-Glucose
            "HMDB0000190",  # Lactate
            "HMDB0000687",  # Creatinine
            "HMDB0000094",  # Citric acid
            "HMDB0000158",  # Alanine
            "HMDB0000161",  # L-Leucine
            "HMDB0000172",  # Isoleucine
            "HMDB0000517",  # Caffeine
            "HMDB0000929",  # Cholesterol
            "HMDB0000148",  # L-Glutamic acid
            "HMDB0000214",  # Adenosine
            "HMDB0000195",  # Inosine
            "HMDB0000293",  # Choline
            "HMDB0000357",  # 3-Hydroxybutyric acid
            "HMDB0000925",  # Glucose-6-phosphate
            "HMDB0000243",  # Pyruvic acid
            "HMDB0000251",  # Taurine
            "HMDB0000929",  # Cholesterol
            "HMDB0000267"   # Uric acid
        ]
        
        self.logger.info(f"Initialized MetaboliteDataExtractor with {len(self.target_metabolites)} target metabolites")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Extracting {len(self.comprehensive_fields)} fields per metabolite")
    
    def _setup_logging(self):
        """Setup comprehensive logging for the extraction process"""
        log_file = self.output_dir / f"metabolite_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def extract_single_metabolite(self, hmdb_id: str) -> Dict[str, Any]:
        """
        Extract comprehensive data for a single metabolite
        
        Args:
            hmdb_id: HMDB identifier for the metabolite
            
        Returns:
            Dictionary containing all extracted metabolite data
        """
        self.logger.info(f"Starting extraction for metabolite: {hmdb_id}")
        start_time = time.time()
        
        try:
            # Use the fallback coordinator to get comprehensive data
            # Start with minimal existing data (just the HMDB ID)
            minimal_data = {"hmdb_id": hmdb_id}
            
            # Extract all comprehensive fields using the fallback coordinator
            metabolite_data = self.fallback_coordinator.integrate_with_pipeline(
                minimal_data, hmdb_id, self.comprehensive_fields
            )
            
            # Add extraction metadata
            metabolite_data["extraction_metadata"] = {
                "extraction_timestamp": datetime.now().isoformat(),
                "extraction_duration_seconds": round(time.time() - start_time, 2),
                "fields_requested": len(self.comprehensive_fields),
                "fields_extracted": len([k for k in metabolite_data.keys() if k != "extraction_metadata"]),
                "api_rate_limit_remaining": self.rate_limiter.get_request_limit - self.rate_limiter.get_requests_made
            }
            
            # Log success
            extraction_time = time.time() - start_time
            fields_extracted = len([k for k in metabolite_data.keys() if k != "extraction_metadata"])
            self.logger.info(f"Successfully extracted {fields_extracted}/{len(self.comprehensive_fields)} fields for {hmdb_id} in {extraction_time:.2f}s")
            
            return metabolite_data
            
        except Exception as e:
            self.logger.error(f"Failed to extract data for {hmdb_id}: {str(e)}")
            return {
                "hmdb_id": hmdb_id,
                "extraction_error": str(e),
                "extraction_metadata": {
                    "extraction_timestamp": datetime.now().isoformat(),
                    "extraction_duration_seconds": round(time.time() - start_time, 2),
                    "status": "failed"
                }
            }
    
    def extract_all_metabolites(self) -> Dict[str, Any]:
        """
        Extract data for all target metabolites
        
        Returns:
            Dictionary containing data for all metabolites plus summary statistics
        """
        self.logger.info(f"Starting bulk extraction for {len(self.target_metabolites)} metabolites")
        extraction_start = time.time()
        
        all_metabolite_data = {}
        successful_extractions = 0
        failed_extractions = 0
        
        for i, hmdb_id in enumerate(self.target_metabolites, 1):
            self.logger.info(f"Processing metabolite {i}/{len(self.target_metabolites)}: {hmdb_id}")
            
            # Extract data for this metabolite
            metabolite_data = self.extract_single_metabolite(hmdb_id)
            all_metabolite_data[hmdb_id] = metabolite_data
            
            # Track success/failure
            if "extraction_error" in metabolite_data:
                failed_extractions += 1
            else:
                successful_extractions += 1
            
            # Add delay between requests to respect rate limits
            if i < len(self.target_metabolites):  # Don't delay after the last request
                delay = 0.5  # Half second delay between requests
                self.logger.debug(f"Adding {delay}s delay before next request")
                time.sleep(delay)
        
        # Calculate summary statistics
        total_extraction_time = time.time() - extraction_start
        summary_stats = {
            "extraction_summary": {
                "total_metabolites_requested": len(self.target_metabolites),
                "successful_extractions": successful_extractions,
                "failed_extractions": failed_extractions,
                "success_rate_percent": round((successful_extractions / len(self.target_metabolites)) * 100, 2),
                "total_extraction_time_seconds": round(total_extraction_time, 2),
                "average_time_per_metabolite_seconds": round(total_extraction_time / len(self.target_metabolites), 2),
                "extraction_timestamp": datetime.now().isoformat(),
                "fields_per_metabolite": len(self.comprehensive_fields),
                "api_requests_made": self.rate_limiter.get_requests_made,
                "rate_limit_remaining": self.rate_limiter.get_request_limit - self.rate_limiter.get_requests_made
            }
        }
        
        # Combine metabolite data with summary
        result = {**summary_stats, **all_metabolite_data}
        
        self.logger.info(f"Bulk extraction completed: {successful_extractions}/{len(self.target_metabolites)} successful")
        self.logger.info(f"Total time: {total_extraction_time:.2f}s, Average: {total_extraction_time/len(self.target_metabolites):.2f}s per metabolite")
        
        return result
    
    def format_for_llm(self, metabolite_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Format extracted metabolite data for optimal LLM consumption
        
        Args:
            metabolite_data: Raw extracted metabolite data
            
        Returns:
            LLM-optimized formatted data
        """
        # Create a structured format optimized for question generation
        llm_formatted = {
            "dataset_info": {
                "purpose": "Metabolite information for LLM question generation",
                "source": "HMDB Database via API",
                "extraction_method": "Comprehensive field extraction with fallback coordination",
                "intended_use": "Training data for Groq API-based question generation"
            },
            "metabolites": {}
        }
        
        # Copy summary if it exists
        if "extraction_summary" in metabolite_data:
            llm_formatted["dataset_info"]["extraction_summary"] = metabolite_data["extraction_summary"]
        
        # Process each metabolite
        for hmdb_id, data in metabolite_data.items():
            if hmdb_id == "extraction_summary":
                continue
                
            # Structure metabolite data for LLM
            if "extraction_error" not in data:
                formatted_metabolite = {
                    "basic_info": {
                        "hmdb_id": data.get("hmdb_id", hmdb_id),
                        "name": data.get("name", "Unknown"),
                        "status": data.get("status", "Unknown"),
                        "description": data.get("description", "No description available")[:500] + "..." if data.get("description", "") and len(data.get("description", "")) > 500 else data.get("description", ""),
                        "synonyms": data.get("synonyms", [])[:10]  # Limit to first 10 synonyms
                    },
                    "chemical_properties": {
                        "molecular_formula": data.get("moldb_formula", "Unknown"),
                        "molecular_weight": data.get("moldb_average_mass", "Unknown"),
                        "monoisotopic_mass": data.get("moldb_mono_mass", "Unknown"),
                        "smiles": data.get("moldb_smiles", "Unknown"),
                        "inchi": data.get("moldb_inchi", "Unknown"),
                        "cas_number": data.get("cas", "Unknown"),
                        "solubility": data.get("moldb_alogps_solubility", "Unknown"),
                        "logp": data.get("moldb_alogps_logp", "Unknown")
                    },
                    "biological_info": {
                        "taxonomy": data.get("chemical_taxonomy", {}),
                        "normal_biospecimens": data.get("biospecimen_normal", []),
                        "abnormal_biospecimens": data.get("biospecimen_abnormal", []),
                        "normal_concentrations": data.get("normal_concentrations", []),
                        "health_effects": data.get("health_effect", []),
                        "biological_roles": data.get("biological_role", [])
                    },
                    "pathways_and_enzymes": {
                        "associated_proteins": data.get("associated_proteins", []),
                        "enzyme_names": data.get("enzyme_name", []),
                        "gene_names": data.get("gene_name", []),
                        "pathway_ids": data.get("smpdb_id", [])
                    },
                    "metadata": {
                        "extraction_timestamp": data.get("extraction_metadata", {}).get("extraction_timestamp"),
                        "fields_extracted": data.get("extraction_metadata", {}).get("fields_extracted", 0)
                    }
                }
                
                llm_formatted["metabolites"][hmdb_id] = formatted_metabolite
            else:
                # Include error information for debugging
                llm_formatted["metabolites"][hmdb_id] = {
                    "error": data["extraction_error"],
                    "hmdb_id": hmdb_id,
                    "status": "extraction_failed"
                }
        
        return llm_formatted
    
    def save_data(self, data: Dict[str, Any], filename_prefix: str = "metabolite_data") -> tuple[str, str]:
        """
        Save extracted data in both raw and LLM-formatted versions
        
        Args:
            data: Extracted metabolite data
            filename_prefix: Prefix for output filenames
            
        Returns:
            Tuple of (raw_file_path, llm_file_path)
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save raw data
        raw_filename = f"{filename_prefix}_raw_{timestamp}.json"
        raw_filepath = self.output_dir / raw_filename
        
        with open(raw_filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Save LLM-formatted data
        llm_data = self.format_for_llm(data)
        llm_filename = f"{filename_prefix}_llm_formatted_{timestamp}.json"
        llm_filepath = self.output_dir / llm_filename
        
        with open(llm_filepath, 'w', encoding='utf-8') as f:
            json.dump(llm_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"Data saved to:")
        self.logger.info(f"  Raw data: {raw_filepath}")
        self.logger.info(f"  LLM formatted: {llm_filepath}")
        
        return str(raw_filepath), str(llm_filepath)
    
    def run_extraction(self) -> tuple[str, str]:
        """
        Run the complete extraction process
        
        Returns:
            Tuple of (raw_data_file, llm_formatted_file)
        """
        self.logger.info("=" * 80)
        self.logger.info("STARTING COMPREHENSIVE METABOLITE DATA EXTRACTION")
        self.logger.info("=" * 80)
        
        # Extract all metabolite data
        all_data = self.extract_all_metabolites()
        
        # Save the data
        raw_file, llm_file = self.save_data(all_data)
        
        # Print summary
        summary = all_data.get("extraction_summary", {})
        self.logger.info("=" * 80)
        self.logger.info("EXTRACTION COMPLETE - SUMMARY")
        self.logger.info("=" * 80)
        self.logger.info(f"Total metabolites: {summary.get('total_metabolites_requested', 0)}")
        self.logger.info(f"Successful extractions: {summary.get('successful_extractions', 0)}")
        self.logger.info(f"Failed extractions: {summary.get('failed_extractions', 0)}")
        self.logger.info(f"Success rate: {summary.get('success_rate_percent', 0)}%")
        self.logger.info(f"Total time: {summary.get('total_extraction_time_seconds', 0)}s")
        self.logger.info(f"API requests made: {summary.get('api_requests_made', 0)}")
        self.logger.info(f"Rate limit remaining: {summary.get('rate_limit_remaining', 0)}")
        self.logger.info("=" * 80)
        
        return raw_file, llm_file


def main():
    """Main execution function"""
    print("🧬 HMDB Metabolite Data Extractor for LLM Question Generation")
    print("=" * 80)
    
    # Initialize extractor
    output_dir = Path(__file__).parent / "extracted_data"
    extractor = MetaboliteDataExtractor(output_dir=str(output_dir))
    
    # Run extraction
    try:
        raw_file, llm_file = extractor.run_extraction()
        
        print(f"\n✅ Extraction completed successfully!")
        print(f"📁 Raw data: {raw_file}")
        print(f"🤖 LLM data: {llm_file}")
        print(f"\n📝 Next steps:")
        print(f"   1. Review the LLM-formatted JSON file")
        print(f"   2. Use the structured data with Groq API for question generation")
        print(f"   3. Validate the generated questions for quality")
        
    except Exception as e:
        print(f"\n❌ Extraction failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
