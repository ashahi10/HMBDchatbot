# Description: This script contains a class for interacting with the HMDB API. It includes methods for making all GET requests to various endpoints and testing all endpoints. The results of the tests are saved to a text file.
import os
import requests
import json
import time
from typing import Any, Dict, Optional
from dotenv import load_dotenv
from backend.utils.cache_manager import CacheManager
# from requests_cache import CachedSession

load_dotenv()

# # Setting up requests-cache for caching
# cached_session = CachedSession('hmdb_cache', backend='sqlite', expire_after=3600)# Cache expires after 1 hour


#cache will store the full URL of the request as the key and the response data as the value.
# if a request was made less than an hour ago, the cached result will be returned instead of making a new API call.
#The TTL (Time-to-Live) is working correctly and will delete expired entries after 1 hour.

# Comprehensive mapping of HMDB API endpoints to their returned fields
# This map is used to intelligently route requests to the appropriate endpoint
# based on what fields are needed, avoiding unnecessary or overly broad data fetching
endpoint_map = {
    # Main metabolite info endpoint - provides core metabolite data
    "metabolites": [
        "hmdb_id", "status", "created_at", "updated_at", "name", "description", 
        "synonyms", "moldb_formula", "cas", "moldb_average_mass", "moldb_mono_mass", 
        "moldb_smiles", "moldb_inchi", "moldb_inchikey", "chemical_taxonomy", 
        "moldb_alogps_solubility", "moldb_alogps_logp", "moldb_alogps_logs", 
        "moldb_pka_strongest_acidic", "moldb_pka_strongest_basic", 
        "moldb_physiological_charge", "moldb_acceptor_count", "moldb_donor_count", 
        "moldb_polar_surface_area", "moldb_rotatable_bond_count", "moldb_refractivity", 
        "moldb_polarizability", "retention_indices", "biospecimen_normal", "biospecimen_abnormal",
        "external_links", "synthesis_reference", "general_references"
    ],
    
    # Concentration data endpoint
    "concentrations": [
        "normal_concentrations", "abnormal_concentrations", "biospecimen", "status", 
        "condition", "value", "age", "sex", "publications"
    ],
    
    # Enzyme data endpoint
    "enzymes": [
        "enzyme", "uniprot_id", "enzyme_id", "enzyme_name", "gene_name", "protein_name", 
        "genecard_id", "theoretical_pi", "molecular_weight", "num_residues", "reactions"
    ],
    
    # Ontology information
    "ontology": [
        "hmdb_id", "status", "created_at", "updated_at", "name", "moldb_formula", 
        "moldb_smiles", "functional_ontology"
    ],
    
    # Detailed health effect ontology
    "ontology/effect/health": [
        "health_effect", "physiological_effect", "name", "category", "description", 
        "source_id", "external_source", "role_type"
    ],
    
    # Detailed organoleptic effect ontology
    "ontology/effect/organoleptic": [
        "organoleptic_effect", "name", "category", "description", "source_id", 
        "external_source", "role_type"
    ],
    
    # Detailed disposition route ontology
    "ontology/disposition/route": [
        "disposition", "route", "name", "source_id"
    ],
    
    # Detailed disposition source ontology
    "ontology/disposition/source": [
        "disposition", "source", "name", "category", "external_source", "source_id"
    ],
    
    # Detailed disposition location ontology
    "ontology/disposition/location": [
        "disposition", "biological_location", "name", "source_id"
    ],
    
    # Detailed natural process ontology
    "ontology/process/natural": [
        "process", "natural_process", "name", "category", "source_id", "external_source",
        "role_type", "description"
    ],
    
    # Detailed industrial process ontology
    "ontology/process/industrial": [
        "process", "industrial_process", "name", "category", "source_id", "external_source",
        "role_type", "description"
    ],
    
    # Detailed environmental role ontology
    "ontology/role/environmental": [
        "role", "environmental_role", "name", "category", "source_id", "external_source",
        "role_type", "description"
    ],
    
    # Detailed biological role ontology
    "ontology/role/biological": [
        "role", "biological_role", "name", "category", "description", "source_id", 
        "external_source", "role_type", "external_sources"
    ],
    
    # Detailed indirect role ontology
    "ontology/role/indirect": [
        "role", "indirect_effect", "name", "category", "source_id", "external_source",
        "role_type", "description", "external_sources"
    ],
    
    # Detailed industrial role ontology
    "ontology/role/industrial": [
        "role", "industrial_application", "name", "category", "source_id", "external_source",
        "role_type", "description", "external_sources"
    ],
    
    # Detailed biomarker role ontology
    "ontology/role/biomarker": [
        "role", "biomarker", "name", "source_id", "category", "role_type", "description",
        "external_sources"
    ],
    
    # Metabolic pathway information
    "pathways": [
        "smpdb_id", "name", "associated_proteins"
    ],
    
    # Spectral data endpoint
    "spectra": [
        "nmr_spectra", "ms_spectra", "nmr_type", "sample_concentration", "solvent", 
        "sample_mass", "sample_assessment", "spectra_assessment", "instrument_type", 
        "nucleus", "frequency", "sample_ph", "sample_temperature", "chemical_shift_reference", 
        "peaks", "ppm", "intensity", "references"
    ],
    
    # Ion search endpoint
    "ion": [
        "hmdb_id", "name", "status", "moldb_inchi", "moldb_inchikey", "moldb_smiles", 
        "synonyms", "biospecimen_normal", "biospecimen_abnormal", "publications"
    ],
    
    # Paginated metabolites list
    "metabolites/page": [
        "hmdb_id", "name", "status", "chemical_formula", "average_molecular_weight", 
        "monisotopic_molecular_weight", "iupac_name", "traditional_iupac", "cas_registry_number", 
        "smiles", "inchi", "inchikey", "kingdom", "super_class", "class", "sub_class", 
        "direct_parent", "molecular_framework", "role", "state", "cellular_locations", 
        "biospecimen_locations", "tissue_locations", "normal_concentrations", "disease_associations", 
        "pathway_associations", "source", "chemspider_id", "drugbank_id", "pubchem_compound_id",
        "next_page", "total_page", "biospecimen_location"
    ],
    
    # Search endpoint for metabolite lookup
    "search": [
        "hmdb_id", "name", "status", "moldb_inchi", "moldb_inchikey", "moldb_smiles", 
        "synonyms", "biospecimen_normal", "biospecimen_abnormal", "publications"
    ]
}


class RateLimiter:
    def __init__(self):
        self.get_requests_made = 0
        self.get_request_limit = 4000  # Per day
        self.get_request_reset_time = 24 * 60 * 60  # 24 hours in seconds
        self.last_reset_time = time.time()

    def _reset_get_limit(self):
        if time.time() - self.last_reset_time > self.get_request_reset_time:
            self.get_requests_made = 0
            self.last_reset_time = time.time()

    def can_make_get_request(self) -> bool:
        self._reset_get_limit()
        return self.get_requests_made < self.get_request_limit

    def record_get_request(self):
        self.get_requests_made += 1


class HMDBApiClient:
    def __init__(self, rate_limiter: RateLimiter, use_cache: bool = True):
        self.api_key = os.getenv("HMDB_API_KEY")
        self.base_url = os.getenv("HMDB_BASE_URL")
        self.headers = {"Content-Type": "application/json"}
        self.rate_limiter = rate_limiter
        
        # Initialize cache
        self._use_cache = use_cache
        if self._use_cache:
            self._cache_manager = CacheManager()

    def _build_url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint}/?api-key={self.api_key}"

    def get(self, endpoint: str) -> Optional[Dict[str, Any]]:
        # Check cache first if enabled
        if self._use_cache:
            cached_response = self._cache_manager.get_cached_api_response(endpoint, {})
            if cached_response is not None:
                print(f"Using cached response for endpoint: {endpoint}")
                return cached_response
        
        # Proceed with API call if no cache or no cached data
        if not self.rate_limiter.can_make_get_request():
            print("GET request limit reached. Try again later.")
            return None

        url = self._build_url(endpoint)
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            self.rate_limiter.record_get_request()
            
            # Cache the response
            if self._use_cache:
                self._cache_manager.cache_api_response(endpoint, {}, response.json())
            
            return response.json()
        except requests.RequestException as e:
            print(f"GET request failed: {e}")
            return None
     
    def post(self, endpoint: str, payload: dict) -> Optional[Dict[str, Any]]:
        # Check cache first if enabled
        if self._use_cache:
            cached_response = self._cache_manager.get_cached_api_response(endpoint, payload)
            if cached_response is not None:
                print(f"Using cached response for {endpoint} with payload: {payload}")
                return cached_response
                
        # Proceed with API call if no cache or no cached data
        if not self.rate_limiter.can_make_get_request():
            return None

        url = f"{self.base_url}/{endpoint}/?api-key={self.api_key}"
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            response.raise_for_status()
            self.rate_limiter.record_get_request()
            
            # Cache the response
            if self._use_cache:
                self._cache_manager.cache_api_response(endpoint, payload, response.json())
                
            return response.json()
        except requests.RequestException as e:
            print(f"POST request failed: {e}")
            return None
            

    def test_all_endpoints(self):

        formula = "C6H12O6"  # Example formula for ion endpoint
        test_endpoints = {
            "Metabolite Info": "metabolites/HMDB0000001",
            "Concentrations": "metabolites/HMDB0000001/concentrations",
            "Enzymes": "metabolites/HMDB0000001/enzymes",
            "Ontology": "metabolites/HMDB0000001/ontology",
            "Pathways": "metabolites/HMDB0000001/pathways",
            "Spectra": "metabolites/HMDB0000001/spectra",
            "Ion": f"metabolites/ion/{formula}",
            "Ontology Effect Health": "metabolites/HMDB0000001/ontology/effect/health",
            "Ontology Effect Organoleptic": "metabolites/HMDB0000001/ontology/effect/organoleptic",
            "Ontology Disposition Route": "metabolites/HMDB0000001/ontology/disposition/route",
            "Ontology Disposition Source": "metabolites/HMDB0000001/ontology/disposition/source",
            "Ontology Disposition Location": "metabolites/HMDB0000001/ontology/disposition/location",
            "Ontology Process Natural": "metabolites/HMDB0000001/ontology/process/natural",
            "Ontology Process Industrial": "metabolites/HMDB0000001/ontology/process/industrial",
            "Ontology Role Environmental": "metabolites/HMDB0000001/ontology/role/environmental",
            "Ontology Role Biological": "metabolites/HMDB0000001/ontology/role/biological",
            "Ontology Role Indirect": "metabolites/HMDB0000001/ontology/role/indirect",
            "Ontology Role Industrial": "metabolites/HMDB0000001/ontology/role/industrial",
            "Ontology Role Biomarker": "metabolites/HMDB0000001/ontology/role/biomarker",
            "Paginated Metabolites": "metabolites/page/1"
        }

        results = {}
        for name, endpoint in test_endpoints.items():
            print(f"Testing {name}...")
            result = self.get(endpoint)
            results[name] = result
            if result:
                print(f"{name}: Success!")
            else:
                print(f"{name}: Failed or No Data Retrieved")
        return results


if __name__ == "__main__":
    rate_limiter = RateLimiter()
    client = HMDBApiClient(rate_limiter)

    # # Run tests for all endpoints
    # results = client.test_all_endpoints()
    
    # # Print results for each endpoint
    # for name, result in results.items():
    #     print(f"\n--- {name} Response ---")
    #     print(result)

    # # Save results to a text file in a nicely formatted way
    # with open("hmdb_results_with_cache.txt", "w") as f:
    #     for name, result in results.items():
    #         f.write(f"\n--- {name} Response ---\n")
    #         if result:
    #             f.write(json.dumps(result, indent=2))
    #         else:
    #             f.write("No Data Retrieved\n")
    #         f.write("\n")