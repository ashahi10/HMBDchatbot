# Description: This script contains a class for interacting with the HMDB API. It includes methods for making all GET requests to various endpoints and testing all endpoints. The results of the tests are saved to a text file.
import os
import requests
import json
import time
from typing import Any, Dict, Optional, List
from dotenv import load_dotenv
from backend.utils.cache_manager import CacheManager
# from requests_cache import CachedSession

load_dotenv()

# # Setting up requests-cache for caching
# cached_session = CachedSession('hmdb_cache', backend='sqlite', expire_after=3600)# Cache expires after 1 hour


#cache will store the full URL of the request as the key and the response data as the value.
# if a request was made less than an hour ago, the cached result will be returned instead of making a new API call.
#The TTL (Time-to-Live) is working correctly and will delete expired entries after 1 hour.

# Field name normalization map - maps logical field names to actual API response field names
# This helps handle inconsistencies in field naming between what callers request and what the API returns
field_alias_map = {
    # Singular/plural inconsistencies
    "normal_concentrations": ["normal_concentration"],
    "abnormal_concentrations": ["abnormal_concentration"],
    
    # Alternative field names for the same concept
    "monoisotopic_molecular_weight": ["moldb_mono_mass", "monisotopic_molecular_weight"],
    "ions": ["ion", "ion_results", "ion_data"],
    
    # Common field aliases
    "chemical_formula": ["moldb_formula"],
    "molecular_weight": ["moldb_average_mass", "average_molecular_weight"],
    "smiles": ["moldb_smiles"],
    "inchi": ["moldb_inchi"],
    "inchikey": ["moldb_inchikey"],
}

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
        "hmdb_id", "name", "status", "chemical_formula", "moldb_inchi", "moldb_inchikey", "moldb_smiles", 
        "biospecimen_normal", "biospecimen_abnormal", "moldb_mono_mass", "monoisotopic_molecular_weight",
        "synonyms"  # Add back, will be handled with better alias mapping
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
    # "search": [
    #     "hmdb_id", "name", "status", "moldb_inchi", "moldb_inchikey", "moldb_smiles", 
    #     "synonyms", "biospecimen_normal", "biospecimen_abnormal", "publications"
    # ]
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
        # Remove '/api/hmdb' from base_url if present to avoid duplication
        server_url = self.base_url.replace('/api/hmdb', '')
        return f"{server_url}/api/hmdb/{endpoint}?api-key={self.api_key}"

    def get(self, endpoint: str, timeout: int = 30) -> Optional[Dict[str, Any]]:
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
            response = requests.get(url, headers=self.headers, timeout=timeout)
            response.raise_for_status()
            self.rate_limiter.record_get_request()
            
            # Cache the response
            if self._use_cache:
                self._cache_manager.cache_api_response(endpoint, {}, response.json())
            
            return response.json()
        except requests.exceptions.Timeout as e:
            print(f"GET request timed out after {timeout}s: {e}")
            return {"timeout_error": True, "message": "HMDB server is busy. Please try again in some time."}
        except requests.exceptions.ConnectionError as e:
            print(f"GET request connection failed: {e}")
            return {"connection_error": True, "message": "Unable to connect to HMDB server. Please try again later."}
        except requests.RequestException as e:
            print(f"GET request failed: {e}")
            return {"request_error": True, "message": "HMDB server error. Please try again in some time."}
     
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

        # Remove '/api/hmdb' from base_url if present to avoid duplication  
        server_url = self.base_url.replace('/api/hmdb', '')
        url = f"{server_url}/api/hmdb/{endpoint}?api-key={self.api_key}"
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            response.raise_for_status()
            self.rate_limiter.record_get_request()
            
            # Cache the response
            if self._use_cache:
                self._cache_manager.cache_api_response(endpoint, payload, response.json())
                
            return response.json()
        except requests.exceptions.Timeout as e:
            print(f"POST request timed out: {e}")
            return {"timeout_error": True, "message": "HMDB server is busy. Please try again in some time."}
        except requests.exceptions.ConnectionError as e:
            print(f"POST request connection failed: {e}")
            return {"connection_error": True, "message": "Unable to connect to HMDB server. Please try again later."}
        except requests.RequestException as e:
            print(f"POST request failed: {e}")
            return {"request_error": True, "message": "HMDB server error. Please try again in some time."}
            
    def select_endpoints_for_fields(self, required_fields: list) -> list:
        """
        Identifies which endpoints need to be called to retrieve the required fields.
        Optimizes to avoid calling the /metabolites endpoint if the fields can be retrieved from other endpoints.
        Takes field aliases into account when mapping fields to endpoints.
        
        Args:
            required_fields: List of field names that need to be retrieved
            
        Returns:
            List of endpoint paths that need to be called
        """
        if not required_fields:
            return []
        
        # Create a mapping from field (and its aliases) to endpoints that provide it
        field_to_endpoints = {}
        for endpoint, fields in endpoint_map.items():
            for field in fields:
                # Add the direct field mapping
                if field not in field_to_endpoints:
                    field_to_endpoints[field] = []
                field_to_endpoints[field].append(endpoint)
                
                # Map any aliases to the same endpoints
                for alias_field, aliases in field_alias_map.items():
                    if field in aliases:
                        if alias_field not in field_to_endpoints:
                            field_to_endpoints[alias_field] = []
                        if endpoint not in field_to_endpoints[alias_field]:
                            field_to_endpoints[alias_field].append(endpoint)
        
        # Determine which endpoints are needed for the required fields
        needed_endpoints = set()
        fields_not_found = []
        
        for field in required_fields:
            # Check for the field directly
            if field in field_to_endpoints:
                # Add all endpoints that provide this field
                endpoints_for_field = field_to_endpoints[field]
                for endpoint in endpoints_for_field:
                    needed_endpoints.add(endpoint)
            # Check for aliases of the field
            elif field in field_alias_map:
                alias_found = False
                for alias in field_alias_map[field]:
                    if alias in field_to_endpoints:
                        # Add endpoints that provide this alias
                        for endpoint in field_to_endpoints[alias]:
                            needed_endpoints.add(endpoint)
                        alias_found = True
                if not alias_found:
                    fields_not_found.append(field)
            else:
                fields_not_found.append(field)
        
        if fields_not_found:
            print(f"Warning: No endpoints found for fields: {fields_not_found}")
        
        # If metabolites endpoint is in the list but all fields can be retrieved from other endpoints,
        # we can potentially remove it to optimize API calls
        if "metabolites" in needed_endpoints and len(needed_endpoints) > 1:
            # Check if all fields available from metabolites endpoint are also available elsewhere
            metabolite_specific_fields = set()
            for field in required_fields:
                field_key = field
                # If this is an alias, use the canonical field name
                for canon_field, aliases in field_alias_map.items():
                    if field in aliases:
                        field_key = canon_field
                        break
                
                if field_key in field_to_endpoints:
                    # If this field is only available from the metabolites endpoint
                    if field_to_endpoints[field_key] == ["metabolites"]:
                        metabolite_specific_fields.add(field)
            
            # If there are no fields exclusive to the metabolites endpoint, we can remove it
            if not metabolite_specific_fields:
                needed_endpoints.remove("metabolites")
        
        return list(needed_endpoints)

    def fetch_fields_for_hmdb_id(self, hmdb_id: str, fields: list) -> Dict[str, Any]:
        """
        Fetches the requested fields for a specific HMDB ID by calling the appropriate endpoints.
        
        Args:
            hmdb_id: The HMDB ID for the metabolite
            fields: List of fields to retrieve
            
        Returns:
            Dictionary containing the requested fields and their values
        """
        # Determine which endpoints we need to call
        endpoints_to_call = self.select_endpoints_for_fields(fields)
        
        # If no valid endpoints found, return empty result
        if not endpoints_to_call:
            print(f"No endpoints found for fields: {fields}")
            return {}
        
        # Track fields that will be skipped due to endpoint restrictions
        skipped_fields = []
        
        # Collect data from each endpoint
        result = {}
        for endpoint_base in endpoints_to_call:
            # Construct the full endpoint path with the HMDB ID
            if endpoint_base in ["metabolites/page", "ion", "search"]:
                # These endpoints have different structures and can't be called
                # directly with an HMDB ID in this simple implementation
                print(f"Skipping endpoint {endpoint_base} as it requires special handling")
                
                # Identify which fields are only available from this skipped endpoint
                if endpoint_base in endpoint_map:
                    endpoint_fields = endpoint_map[endpoint_base]
                    
                    # Find fields that are only available from this endpoint
                    for field in fields:
                        if field in endpoint_fields:
                            # Check if this field is available from other non-skipped endpoints
                            available_elsewhere = False
                            for other_endpoint in endpoints_to_call:
                                if other_endpoint != endpoint_base and other_endpoint not in ["metabolites/page", "ion", "search"]:
                                    if field in endpoint_map[other_endpoint]:
                                        available_elsewhere = True
                                        break
                            
                            # If not available elsewhere, add to skipped fields
                            if not available_elsewhere:
                                skipped_fields.append(field)
                                print(f"Warning: Field '{field}' is only available from skipped endpoint '{endpoint_base}'")
                
                continue
            elif endpoint_base == "metabolites":
                endpoint = f"metabolites/{hmdb_id}"
            elif endpoint_base.startswith("ontology/"):
                # Handle ontology endpoints which have a specific path structure
                endpoint = f"metabolites/{hmdb_id}/{endpoint_base}"
            else:
                # For other endpoints like concentrations, enzymes, etc.
                endpoint = f"metabolites/{hmdb_id}/{endpoint_base}"
            
            # Make the API call
            print(f"Calling endpoint: {endpoint}")
            response = self.get(endpoint)
            
            # If we got a response, extract the requested fields
            if response:
                self._extract_fields_from_response(response, fields, result)
        
        # Check for missing fields considering aliases
        missing_fields = []
        for field in fields:
            # Skip fields that we already know couldn't be fetched
            if field in skipped_fields:
                continue
                
            # Check if field is directly present
            if field in result:
                continue
                
            # Check if any aliases are present
            alias_found = False
            if field in field_alias_map:
                for alias in field_alias_map[field]:
                    if alias in result:
                        # Copy the value from the alias to the requested field name
                        result[field] = result[alias]
                        alias_found = True
                        break
                
            # If no aliases found, the field is truly missing
            if not alias_found:
                missing_fields.append(field)
        
        # Report missing fields
        if missing_fields:
            print(f"Warning: Could not find fields: {missing_fields}")
        
        # Add a flag for skipped fields so callers know these weren't even attempted
        if skipped_fields:
            print(f"Some fields were skipped because they're only available from endpoints that can't be used: {skipped_fields}")
        
        return result

    def _extract_fields_from_response(self, response: Any, fields: list, result: dict):
        """
        Extract specified fields from an API response.
        
        Args:
            response: The API response to extract fields from
            fields: List of fields to extract
            result: Dictionary to update with the extracted fields
        """
        if response is None:
            return
        
        # Add debug info to help diagnose missing fields
        if isinstance(response, dict) and len(response) < 10:
            print(f"DEBUG: Response keys: {list(response.keys())}")
        
        # Handle dictionary responses
        if isinstance(response, dict):
            for field in fields:
                # Skip if field already exists in result (avoid overwriting)
                if field in result:
                    continue
                    
                # Direct match at top level
                if field in response:
                    # Normalize the field value
                    result[field] = self._normalize_field_value(field, response[field])
                    continue
                
                # Check for aliases at top level
                field_found = False
                if field in field_alias_map:
                    for alias in field_alias_map[field]:
                        if alias in response:
                            result[field] = self._normalize_field_value(field, response[alias])
                            field_found = True
                            break
                
                if field_found:
                    continue
                
                # Search in nested dictionaries if not found at top level
                for key, value in response.items():
                    if self._extract_field_from_value(value, field, result):
                        # Normalize the field after extraction if it exists
                        if field in result:
                            result[field] = self._normalize_field_value(field, result[field])
                        break
        
        # Handle list responses
        elif isinstance(response, list):
            for item in response:
                self._extract_fields_from_response(item, fields, result)
                
        # After attempting to extract all fields directly, perform a final alias check for any missing fields
        self._check_and_apply_aliases(fields, result)

    def _extract_field_from_value(self, value: Any, field: str, result: dict) -> bool:
        """
        Recursively extract a field from a nested value structure.
        
        Args:
            value: The value to search in
            field: The field to find
            result: Dictionary to update with the field if found
            
        Returns:
            True if the field was found and extracted, False otherwise
        """
        # Direct match in dictionary
        if isinstance(value, dict):
            # Check for exact field match
            if field in value:
                result[field] = value[field]
                return True
            
            # Check for field aliases
            if field in field_alias_map:
                for alias in field_alias_map[field]:
                    if alias in value:
                        result[field] = value[alias]
                        return True
            
            # Recursively search in nested dictionaries
            for k, v in value.items():
                if self._extract_field_from_value(v, field, result):
                    return True
        
        # Search in list items
        elif isinstance(value, list):
            found = False
            for item in value:
                if isinstance(item, dict):
                    # Check for exact field match
                    if field in item:
                        if field not in result:
                            result[field] = []
                        elif isinstance(result[field], str):
                            result[field] = [result[field]]  # Convert existing str to list
                        
                        # Convert item[field] to list if it's not already a list
                        if isinstance(result[field], list):
                            result[field].append(item[field])
                        else:
                            result[field] = [item[field]]
                        found = True
                    
                    # Check for field aliases
                    elif field in field_alias_map:
                        for alias in field_alias_map[field]:
                            if alias in item:
                                if field not in result:
                                    result[field] = []
                                elif isinstance(result[field], str):
                                    result[field] = [result[field]]  # Convert existing str to list
                                
                                if isinstance(result[field], list):
                                    result[field].append(item[alias])
                                else:
                                    result[field] = [item[alias]]
                                found = True
                                break
                
                # Continue recursive search
                elif self._extract_field_from_value(item, field, result):
                    found = True
            return found
        
        return False

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

    def search_by_name(self, name: str) -> List[Dict[str, Any]]:
        """
        Search for metabolites by name using the HMDB search endpoint.
        This method is used for metabolite discovery when an HMDB ID is not known.
        
        Args:
            name: The name or partial name of the metabolite to search for
            
        Returns:
            List of matching metabolites with their basic information
        """
        if not name:
            print("Error: Name parameter is required for search")
            return []
        
        # Prepare the search payload with required format
        payload = {
            "query": name,
            "field": "name",  # Add field parameter to specify search by name
            "filters": {},
            "page": 1,
            "per_page": 5
        }
        
        # Call the search endpoint
        response = self.post("metabolites/search", payload)
        
        if not response:
            print(f"No search results found for '{name}'")
            return []
        
        # Extract the list of matching metabolites
        matches = []
        
        # Handle different response structures
        if isinstance(response, list):
            matches = response
        elif isinstance(response, dict) and "data" in response:
            matches = response["data"]
        elif isinstance(response, dict) and "metabolites" in response:
            matches = response["metabolites"]
        
        print(f"Found {len(matches)} metabolite(s) matching '{name}'")
        return matches

    def fetch_fields_for_formula(self, formula: str, fields: list) -> Dict[str, Any]:
        """
        Fetch metabolite information based on a chemical formula using the ion endpoint.
        This is used as a fallback when a chemical formula is available but no HMDB ID.
        
        Args:
            formula: The chemical formula (e.g., "C6H12O6")
            fields: List of fields to retrieve
            
        Returns:
            Dictionary containing the requested fields and their values
        """
        if not formula:
            print("Error: Formula parameter is required")
            return {}
        
        # Build the ion endpoint URL
        endpoint = f"metabolites/ion/{formula}"
        
        # Make the API call
        print(f"Calling ion endpoint with formula: {formula}")
        response = self.get(endpoint)
        
        if not response:
            print(f"No ion data found for formula: {formula}")
            return {}
        
        # Extract requested fields from the response
        result = {}
        self._extract_fields_from_response(response, fields, result)
        
        # Check for missing fields considering aliases
        missing_fields = []
        for field in fields:
            # Check if field is directly present
            if field in result:
                continue
                
            # Check if any aliases are present
            if field in field_alias_map:
                alias_found = False
                for alias in field_alias_map[field]:
                    if alias in result:
                        # Copy the value from the alias to the requested field name
                        result[field] = result[alias]
                        alias_found = True
                        break
                if alias_found:
                    continue
            
            # If we get here, the field is truly missing
            missing_fields.append(field)
        
        if missing_fields:
            print(f"Warning: Could not find ion fields: {missing_fields}")
        
        return result

    def get_all_spectra_for_hmdb_id(self, hmdb_id: str) -> Dict[str, Any]:
        """
        Retrieve ALL available spectra for the given HMDB ID
        
        Args:
            hmdb_id: HMDB identifier (e.g. "HMDB0000001")
            
        Returns:
            Dictionary containing all spectra data or error information
        """
        endpoint = f"metabolites/{hmdb_id}/spectra"
        
        # Use GET request for spectra retrieval with extended timeout for spectra
        response = self.get(endpoint, timeout=60)  # Longer timeout for spectra endpoint
        
        # Handle timeout and connection errors FIRST
        if isinstance(response, dict):
            if response.get("timeout_error"):
                return {
                    "success": False,
                    "data": None,
                    "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                    "errors": [response.get("message", "HMDB server is busy. Please try again in some time.")],
                    "error_type": "timeout"
                }
            elif response.get("connection_error"):
                return {
                    "success": False,
                    "data": None,
                    "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                    "errors": [response.get("message", "Unable to connect to HMDB server. Please try again later.")],
                    "error_type": "connection"
                }
            elif response.get("request_error"):
                return {
                    "success": False,
                    "data": None,
                    "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                    "errors": [response.get("message", "HMDB server error. Please try again in some time.")],
                    "error_type": "request"
                }
        
        # Handle None response (shouldn't happen now, but keeping as fallback)
        if response is None:
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": ["Failed to retrieve data from HMDB API"]
            }

        if not isinstance(response, (dict, list)):
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": [f"Invalid response format: expected dict or list, got {type(response)}"]
            }
        
        all_spectra = []
        spectrum_types_found = []
        
        if isinstance(response, dict):
            # Look for spectra data in all HMDB fields
            for spectra_field in ['c_ms', 'ms_ms', 'nmr', 'ms_ir']:
                if spectra_field in response and response[spectra_field]:
                    spectra_list = response[spectra_field]
                    if isinstance(spectra_list, list) and len(spectra_list) > 0:
                        spectrum_types_found.append(f"{spectra_field} ({len(spectra_list)} spectra)")
                        
                        # Process ALL spectra in this category
                        for i, spectrum_data in enumerate(spectra_list):
                            processed_spectrum = self._process_single_spectrum(
                                spectrum_data, hmdb_id, endpoint, spectra_field, i + 1, len(spectra_list)
                            )
                            if processed_spectrum["success"]:
                                all_spectra.append(processed_spectrum)
                            else:
                                print(f"[SPECTRA] Failed to process {spectra_field} spectrum {i+1}: {processed_spectrum['errors']}")
                                
        elif isinstance(response, list):
            if len(response) == 0:
                return {
                    "success": False,
                    "data": None,
                    "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                    "errors": ["Empty spectra list returned"]
                }
            
            # Process all spectra in the list
            for i, spectrum_data in enumerate(response):
                processed_spectrum = self._process_single_spectrum(
                    spectrum_data, hmdb_id, endpoint, "direct_list", i + 1, len(response)
                )
                if processed_spectrum["success"]:
                    all_spectra.append(processed_spectrum)
        
        if not all_spectra:
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": ["No valid spectra found in any of the expected fields (c_ms, ms_ms, nmr, ms_ir)"]
            }
        
        print(f"[SPECTRA] Successfully processed {len(all_spectra)} spectra for {hmdb_id}")
        print(f"[SPECTRA] Found spectrum types: {', '.join(spectrum_types_found)}")
        
        return {
            "success": True,
            "data": all_spectra,
            "metadata": {
                "hmdb_id": hmdb_id,
                "endpoint": endpoint,
                "total_spectra": len(all_spectra),
                "spectrum_types": spectrum_types_found,
                "processing_timestamp": time.time()
            },
            "errors": None
        }

    def _process_single_spectrum(self, spectrum_data: dict, hmdb_id: str, endpoint: str, 
                                spectrum_type: str, index: int, total: int) -> Dict[str, Any]:
        """
        Process a single spectrum from the collection
        
        Args:
            spectrum_data: Individual spectrum data
            hmdb_id: HMDB identifier
            endpoint: API endpoint used
            spectrum_type: Type of spectrum (c_ms, ms_ms, etc.)
            index: Index of this spectrum in its category
            total: Total number of spectra in this category
            
        Returns:
            Processed spectrum data
        """
        # Extract and validate peaks data
        peaks = spectrum_data.get('peaks', [])
        if not isinstance(peaks, list):
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint, "spectrum_type": spectrum_type, "index": index},
                "errors": ["Peaks data is not a list"]
            }
        
        if len(peaks) == 0:
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint, "spectrum_type": spectrum_type, "index": index},
                "errors": ["No peaks data found in spectrum"]
            }
        
        # Validate peak structure
        validated_peaks = []
        validation_errors = []
        
        for i, peak in enumerate(peaks):
            if not isinstance(peak, dict):
                validation_errors.append(f"Peak {i} is not a dictionary")
                continue
            
            # Check for required fields
            if 'mass_charge' not in peak:
                validation_errors.append(f"Peak {i} missing mass_charge")
                continue
            
            if 'intensity' not in peak:
                validation_errors.append(f"Peak {i} missing intensity")
                continue
            
            # Validate and convert data types
            try:
                mass_charge = float(peak['mass_charge'])
                intensity = float(peak['intensity'])
                
                if mass_charge < 0:
                    validation_errors.append(f"Peak {i} has negative mass_charge: {mass_charge}")
                    continue
                
                if intensity < 0:
                    validation_errors.append(f"Peak {i} has negative intensity: {intensity}")
                    continue
                
                validated_peaks.append({
                    'mass_charge': mass_charge,
                    'intensity': intensity,
                    'annotation': peak.get('annotation', None)
                })
                
            except (ValueError, TypeError) as e:
                validation_errors.append(f"Peak {i} has invalid numeric data: {e}")
                continue
        
        # Check if we have any valid peaks after validation
        if len(validated_peaks) == 0:
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint, "spectrum_type": spectrum_type, "index": index},
                "errors": ["No valid peaks found after validation"] + validation_errors
            }
        
        # Extract metadata
        metadata_fields = {
            "spectrum_type": spectrum_data.get('spectrum_type', 'Unknown'),
            "instrument_type": spectrum_data.get('instrument_type', 'Unknown'),
            "chromatography_type": spectrum_data.get('chromatography_type', None),
            "sample_concentration": spectrum_data.get('sample_concentration', None),
            "solvent": spectrum_data.get('solvent', None),
            "sample_temperature": spectrum_data.get('sample_temperature', None),
            "sample_ph": spectrum_data.get('sample_ph', None),
            "chemical_shift_reference": spectrum_data.get('chemical_shift_reference', None),
            "nucleus": spectrum_data.get('nucleus', None),
            "frequency": spectrum_data.get('frequency', None),
            "splash_key": spectrum_data.get('splash_key', None),
            "spectrum_url": spectrum_data.get('spectrum_url', None)  # NEW: Extract spectrum URL
        }
        
        # Prepare successful response
        processed_data = {
            "peaks": validated_peaks,
            "spectrum_type": metadata_fields["spectrum_type"],
            "instrument_type": metadata_fields["instrument_type"],
            "chromatography_type": metadata_fields["chromatography_type"],
            "sample_conditions": {
                "concentration": metadata_fields["sample_concentration"],
                "solvent": metadata_fields["solvent"],
                "temperature": metadata_fields["sample_temperature"],
                "ph": metadata_fields["sample_ph"]
            },
            "technical_details": {
                "nucleus": metadata_fields["nucleus"],
                "frequency": metadata_fields["frequency"],
                "chemical_shift_reference": metadata_fields["chemical_shift_reference"],
                "splash_key": metadata_fields["splash_key"]
            }
        }
        
        response_metadata = {
            "hmdb_id": hmdb_id,
            "endpoint": endpoint,
            "spectrum_category": spectrum_type,
            "spectrum_index": index,
            "total_in_category": total,
            "total_peaks": len(validated_peaks),
            "validation_warnings": validation_errors if validation_errors else None,
            "processing_timestamp": time.time()
        }
        
        return {
            "success": True,
            "data": processed_data,
            "metadata": response_metadata,
            "errors": validation_errors if validation_errors else None
        }

    def get_spectra_for_hmdb_id(self, hmdb_id: str) -> Dict[str, Any]:
        """
        Fetch spectral data for a specific HMDB ID using the GET /spectra/ endpoint.
        
        This method is specifically designed for the spectra functionality following
        the Phase 2 requirements with proper validation and error handling.
        
        Args:
            hmdb_id: The HMDB identifier (e.g., "HMDB0000001")
            
        Returns:
            Dictionary containing processed spectra data with the structure:
            {
                "success": bool,
                "data": {
                    "peaks": [...],
                    "spectrum_type": "...",
                    "instrument_type": "...",
                    "chromatography_type": "..."
                },
                "metadata": {...},
                "errors": [...]
            }
        """
        if not hmdb_id:
            return {
                "success": False,
                "data": None,
                "metadata": None,
                "errors": ["HMDB ID parameter is required"]
            }
        
        # Validate HMDB ID format
        if not hmdb_id.startswith('HMDB'):
            return {
                "success": False,
                "data": None,
                "metadata": None,
                "errors": [f"Invalid HMDB ID format: {hmdb_id}. Must start with 'HMDB'"]
            }
        
        # Build the spectra endpoint URL
        endpoint = f"metabolites/{hmdb_id}/spectra"
        
        # Make the API call with logging
        print(f"[SPECTRA] Fetching spectra data for: {hmdb_id}")
        response = self.get(endpoint, timeout=60)
        
        # No mock data - use real API responses only
        # If the API doesn't return data, we'll handle it as a proper error
        
        # Handle null or empty response
        if not response:
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": ["HMDB spectrum service is currently slow or unavailable. Please try again later."]
            }
        
        # Validate response structure
        if not isinstance(response, (dict, list)):
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": [f"Invalid response format: expected dict or list, got {type(response)}"]
            }
        
        # The HMDB API returns spectra data in specific fields: c_ms, ms_ms, nmr, ms_ir
        # Each field contains an array of spectra objects
        spectrum_data = None
        
        if isinstance(response, dict):
            # Look for spectra data in the standard HMDB fields
            for spectra_field in ['c_ms', 'ms_ms', 'nmr', 'ms_ir']:
                if spectra_field in response and response[spectra_field]:
                    spectra_list = response[spectra_field]
                    if isinstance(spectra_list, list) and len(spectra_list) > 0:
                        # Use the first spectrum from the first available spectra type
                        spectrum_data = spectra_list[0]
                        print(f"[SPECTRA] Using {spectra_field} spectrum (1 of {len(spectra_list)} available)")
                        break
        elif isinstance(response, list):
            if len(response) == 0:
                return {
                    "success": False,
                    "data": None,
                    "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                    "errors": ["Empty spectra list returned"]
                }
            
            # Use the first spectrum for now (could be enhanced to select best quality)
            spectrum_data = response[0]
        
        if not spectrum_data:
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": ["No spectrum data found in any of the expected fields (c_ms, ms_ms, nmr, ms_ir)"]
            }
        
        # Extract and validate peaks data
        peaks = spectrum_data.get('peaks', [])
        if not isinstance(peaks, list):
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": ["Peaks data is not a list"]
            }
        
        if len(peaks) == 0:
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": ["No peaks data found in spectrum"]
            }
        
        # Validate peak structure
        validated_peaks = []
        validation_errors = []
        
        for i, peak in enumerate(peaks):
            if not isinstance(peak, dict):
                validation_errors.append(f"Peak {i} is not a dictionary")
                continue
            
            # Check for required fields
            if 'mass_charge' not in peak:
                validation_errors.append(f"Peak {i} missing mass_charge")
                continue
            
            if 'intensity' not in peak:
                validation_errors.append(f"Peak {i} missing intensity")
                continue
            
            # Validate and convert data types
            try:
                mass_charge = float(peak['mass_charge'])
                intensity = float(peak['intensity'])
                
                if mass_charge < 0:
                    validation_errors.append(f"Peak {i} has negative mass_charge: {mass_charge}")
                    continue
                
                if intensity < 0:
                    validation_errors.append(f"Peak {i} has negative intensity: {intensity}")
                    continue
                
                validated_peaks.append({
                    'mass_charge': mass_charge,
                    'intensity': intensity,
                    'annotation': peak.get('annotation', None)
                })
                
            except (ValueError, TypeError) as e:
                validation_errors.append(f"Peak {i} has invalid numeric data: {e}")
                continue
        
        # Check if we have any valid peaks after validation
        if len(validated_peaks) == 0:
            return {
                "success": False,
                "data": None,
                "metadata": {"hmdb_id": hmdb_id, "endpoint": endpoint},
                "errors": ["No valid peaks found after validation"] + validation_errors
            }
        
        # Extract metadata using the existing spectra endpoint mapping
        metadata_fields = {
            "spectrum_type": spectrum_data.get('spectrum_type', 'Unknown'),
            "instrument_type": spectrum_data.get('instrument_type', 'Unknown'),
            "chromatography_type": spectrum_data.get('chromatography_type', None),
            "sample_concentration": spectrum_data.get('sample_concentration', None),
            "solvent": spectrum_data.get('solvent', None),
            "sample_temperature": spectrum_data.get('sample_temperature', None),
            "sample_ph": spectrum_data.get('sample_ph', None),
            "chemical_shift_reference": spectrum_data.get('chemical_shift_reference', None),
            "nucleus": spectrum_data.get('nucleus', None),
            "frequency": spectrum_data.get('frequency', None),
            "splash_key": spectrum_data.get('splash_key', None),
            "spectrum_url": spectrum_data.get('spectrum_url', None)  # Extract spectrum URL
        }
        
        # Prepare successful response
        processed_data = {
            "peaks": validated_peaks,
            "spectrum_type": metadata_fields["spectrum_type"],
            "instrument_type": metadata_fields["instrument_type"],
            "chromatography_type": metadata_fields["chromatography_type"],
            "sample_conditions": {
                "concentration": metadata_fields["sample_concentration"],
                "solvent": metadata_fields["solvent"],
                "temperature": metadata_fields["sample_temperature"],
                "ph": metadata_fields["sample_ph"]
            },
            "technical_details": {
                "nucleus": metadata_fields["nucleus"],
                "frequency": metadata_fields["frequency"],
                "chemical_shift_reference": metadata_fields["chemical_shift_reference"],
                "splash_key": metadata_fields["splash_key"]
            }
        }
        
        response_metadata = {
            "hmdb_id": hmdb_id,
            "endpoint": endpoint,
            "total_peaks": len(validated_peaks),
            "validation_warnings": validation_errors if validation_errors else None,
            "processing_timestamp": time.time()
        }
        
        print(f"[SPECTRA] Successfully processed {len(validated_peaks)} peaks for {hmdb_id}")
        
        return {
            "success": True,
            "data": processed_data,
            "metadata": response_metadata,
            "errors": validation_errors if validation_errors else None
        }
    
    # Mock data generation method removed - using real API responses only
    
# End of HMDBApiClient class
