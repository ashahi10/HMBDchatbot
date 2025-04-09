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

    def _normalize_field_value(self, field_name: str, field_value: Any) -> Any:
        """
        Normalize field values to handle inconsistent API responses.
        
        Args:
            field_name: The name of the field
            field_value: The value to normalize
            
        Returns:
            Normalized value that's consistent across API calls
        """
        # If the field is None, return an empty value based on field type expectations
        if field_value is None:
            if field_name in ["synonyms", "ions"]:
                return []
            elif field_name in ["description", "name"]:
                return ""
            return None
            
        # Handle text fields that come as lists (especially descriptions)
        if field_name in ["description"] and isinstance(field_value, list):
            # Join all list items into a single string, handling potential nested structures
            if field_value and isinstance(field_value[0], dict) and "text" in field_value[0]:
                # Handle list of text objects
                return " ".join([item.get("text", "") for item in field_value if isinstance(item, dict)])
            else:
                # Handle simple list of strings
                return " ".join([str(item) for item in field_value if item])
        
        # Handle description as string that looks like a Python list representation
        # E.g. "['text part 1', 'text part 2']"
        if field_name == "description" and isinstance(field_value, str) and field_value.startswith("[") and field_value.endswith("]"):
            try:
                # Try to convert string representation of list back to actual list
                import ast
                list_value = ast.literal_eval(field_value)
                if isinstance(list_value, list):
                    return " ".join([str(item) for item in list_value if item])
            except (SyntaxError, ValueError):
                # If parsing fails, just return the original string
                pass
        
        # Handle synonyms - normalize to list of strings
        if field_name == "synonyms":
            if isinstance(field_value, list):
                # If list contains dictionaries with 'name' field
                if field_value and isinstance(field_value[0], dict) and "name" in field_value[0]:
                    return [item.get("name", "") for item in field_value if isinstance(item, dict) and "name" in item]
                # If list contains dictionaries with 'synonym' field
                elif field_value and isinstance(field_value[0], dict) and "synonym" in field_value[0]:
                    return [item.get("synonym", "") for item in field_value if isinstance(item, dict) and "synonym" in item]
                # Already a list of strings
                return field_value
            # Convert single string to list
            elif isinstance(field_value, str):
                # Handle string representations of lists
                if field_value.startswith("[") and field_value.endswith("]"):
                    try:
                        import ast
                        list_value = ast.literal_eval(field_value)
                        if isinstance(list_value, list):
                            return list_value
                    except (SyntaxError, ValueError):
                        pass
                return [field_value]
            # Empty or None
            return []
        
        # Handle description - always return as string
        elif field_name == "description":
            if isinstance(field_value, dict) and "text" in field_value:
                return field_value["text"]
            elif isinstance(field_value, str):
                return field_value
            return str(field_value) if field_value is not None else ""
        
        # Handle moldb_formula/chemical_formula - normalize format
        elif field_name in ["moldb_formula", "chemical_formula"]:
            if isinstance(field_value, dict) and "formula" in field_value:
                return field_value["formula"]
            return field_value
            
        # Default case - return as is
        return field_value

    def _check_and_apply_aliases(self, fields: list, result: dict):
        """
        Check for missing fields and apply aliases if needed.
        
        Args:
            fields: List of fields to check
            result: Dictionary to update with missing fields
        """
        if not fields:
            return
        
        for field in fields:
            # Skip if field already exists in result (avoid overwriting)
            if field in result:
                continue
                
            # Check for aliases
            if field in field_alias_map:
                for alias in field_alias_map[field]:
                    if alias in result:
                        result[field] = result[alias]
                        break


class ApiFallbackCoordinator:
    """
    Coordinates the fallback mechanism for HMDB API requests.
    Isolates and orchestrates the fallback system from the main pipeline logic.
    
    Uses a strategy pattern to:
    1. Decide which fields are missing
    2. Determine which endpoints to query based on the endpoint_map
    3. Fetch the missing fields using the hmdb_client
    4. Handle retries and rate limiting
    5. Merge and return the results
    """
    
    def __init__(self, hmdb_client: HMDBApiClient, max_retries: int = 3, backoff_factor: float = 1.5):
        """
        Initialize the ApiFallbackCoordinator.
        
        Args:
            hmdb_client: The HMDB API client to use for fetching data
            max_retries: Maximum number of retries for failed requests
            backoff_factor: Factor to increase delay between retries
        """
        self.hmdb_client = hmdb_client
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
    
    def decide_and_fetch(self, missing_fields: list, known_hmdb_id: str, existing_data: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Decide which endpoints to query and fetch the missing fields.
        
        Args:
            missing_fields: List of fields that need to be fetched
            known_hmdb_id: The HMDB ID of the metabolite
            existing_data: Optional dictionary of data that has already been fetched
            
        Returns:
            Dictionary with all data, including the newly fetched fields
        """
        if existing_data is None:
            existing_data = {}
        
        # Filter out fields that are already present in existing_data
        fields_to_fetch = [field for field in missing_fields if field not in existing_data]
        
        if not fields_to_fetch:
            print("No fields to fetch, all fields already present in existing data")
            return existing_data
            
        # Special handling for formula-based fields if chemical_formula is available
        if "chemical_formula" in existing_data and any(field in ["ions", "ion", "monoisotopic_molecular_weight", "smiles", "inchi", "inchikey"] for field in fields_to_fetch):
            formula = existing_data.get("chemical_formula")
            if formula:
                print(f"Using chemical formula '{formula}' for formula-based fallback")
                
                # Identify all fields that might be available through the formula/ion endpoint
                ion_endpoint_fields = endpoint_map.get("ion", [])
                
                # Include alias-mapped fields too
                for field in fields_to_fetch:
                    if field in field_alias_map:
                        for alias in field_alias_map[field]:
                            if alias in ion_endpoint_fields and alias not in ion_endpoint_fields:
                                ion_endpoint_fields.append(alias)
                
                # Filter fields_to_fetch to only those that might be in the ion endpoint
                formula_fields = [field for field in fields_to_fetch 
                                 if field in ion_endpoint_fields or
                                    any(alias in ion_endpoint_fields for alias in field_alias_map.get(field, []))]
                
                if formula_fields:
                    formula_data = self.hmdb_client.fetch_fields_for_formula(formula, formula_fields)
                    
                    # Merge formula data with existing data
                    if formula_data:
                        existing_data.update(formula_data)
                        # Update fields_to_fetch to remove fields we just got
                        fields_to_fetch = [field for field in fields_to_fetch if field not in formula_data]
        
        # If there are still fields to fetch, proceed with normal ID-based fallback
        if fields_to_fetch:
            # Attempt to fetch the missing fields with retries
            fetched_data = self._fetch_with_retries(fields_to_fetch, known_hmdb_id)
            
            # Merge the fetched data with the existing data
            existing_data.update(fetched_data)
        
        # Check if any fields are still missing
        final_missing_fields = [field for field in missing_fields if field not in existing_data]
        if final_missing_fields:
            print(f"Warning: Some fields could not be fetched: {final_missing_fields}")
        
        return existing_data
    
    def discover_hmdb_id_by_name(self, name: str) -> str:
        """
        Discover HMDB ID by name using the search endpoint.
        This should be used BEFORE field-based fallback when only a name is known.
        
        Args:
            name: The metabolite name to search for
            
        Returns:
            HMDB ID if a single match is found, empty string otherwise
        """
        if not name:
            return ""
            
        # Search for metabolites by name
        matches = self.hmdb_client.search_by_name(name)
        
        # If exactly one match, return its HMDB ID
        if len(matches) == 1:
            hmdb_id = matches[0].get("hmdb_id", "")
            if hmdb_id:
                print(f"Found HMDB ID {hmdb_id} for name '{name}'")
                return hmdb_id
        
        # If multiple matches, this is ambiguous
        elif len(matches) > 1:
            print(f"Found multiple matches for name '{name}'. Disambiguation needed.")
            # In a real implementation, you might return options for disambiguation
        
        # No matches found
        return ""
    
    def _fetch_with_retries(self, fields: list, hmdb_id: str) -> Dict[str, Any]:
        """
        Fetch fields with retry logic.
        
        Args:
            fields: List of fields to fetch
            hmdb_id: The HMDB ID of the metabolite
            
        Returns:
            Dictionary with fetched data
        """
        result = {}
        retry_count = 0
        total_attempts = 0  # Track total attempts to prevent infinite loops
        max_total_attempts = self.max_retries * 3  # Hard limit on total attempts
        
        # Make a copy of fields to avoid modifying the original list
        fields_to_fetch = fields.copy()
        
        while retry_count < self.max_retries and total_attempts < max_total_attempts:
            total_attempts += 1
            
            # Safety check - if we have no fields left to fetch, we're done
            if not fields_to_fetch:
                break
            
            try:
                # Use the client's fetch_fields_for_hmdb_id method to get the data
                fetched_data = self.hmdb_client.fetch_fields_for_hmdb_id(hmdb_id, fields_to_fetch)
                
                if fetched_data:
                    # If we got some data, merge it with our result
                    result.update(fetched_data)
                    
                    # Check if we got all the fields we needed
                    remaining_fields = [field for field in fields_to_fetch if field not in result]
                    
                    # If some fields were skipped due to endpoint restrictions, don't keep trying to fetch them
                    if total_attempts >= 2:  # Only start checking for impossible fields after a couple attempts
                        # If we're still trying for the same fields after multiple attempts, they might be impossible
                        if remaining_fields == fields_to_fetch:
                            print(f"Warning: After {total_attempts} attempts, still cannot fetch fields: {remaining_fields}")
                            print("These fields may be impossible to fetch in the current context. Stopping retries.")
                            break
                    
                    if not remaining_fields:
                        # If we got all fields, we're done
                        return result
                    
                    # Update fields to only include the remaining ones for the next attempt
                    fields_to_fetch = remaining_fields
                
                # If we didn't get all fields but made some progress, reset retry count
                if len(result) > 0:
                    retry_count = 0
                else:
                    # If we didn't make any progress, increment retry count
                    retry_count += 1
                    
                    # Add backoff delay
                    delay = self.backoff_factor ** retry_count
                    print(f"Retrying in {delay:.2f} seconds... (Attempt {retry_count + 1}/{self.max_retries})")
                    time.sleep(delay)
            
            except Exception as e:
                # If there was an exception, increment retry count
                retry_count += 1
                
                # Add backoff delay
                delay = self.backoff_factor ** retry_count
                print(f"Error fetching data: {e}. Retrying in {delay:.2f} seconds... (Attempt {retry_count + 1}/{self.max_retries})")
                time.sleep(delay)
        
        # If we've exhausted all retries or hit the total attempts limit
        if total_attempts >= max_total_attempts:
            print(f"Warning: Maximum total attempts ({max_total_attempts}) reached. Stopping to prevent infinite loops.")
        else:
            print(f"Warning: Maximum retries ({self.max_retries}) reached. Some fields may not have been fetched.")
        
        return result
    
    def analyze_missing_fields(self, data: Dict[str, Any], required_fields: list) -> list:
        """
        Analyze a data dictionary to determine which required fields are missing.
        Takes into account field aliases when determining if a field is present.
        
        Args:
            data: Dictionary of data to analyze
            required_fields: List of fields that are required
            
        Returns:
            List of fields that are missing from the data
        """
        if not data:
            return required_fields
        
        missing_fields = []
        for field in required_fields:
            # Field is directly present in data
            if field in data:
                continue
                
            # Check if any aliases of the field are present in data
            if field in field_alias_map:
                alias_found = False
                for alias in field_alias_map[field]:
                    if alias in data:
                        alias_found = True
                        break
                if alias_found:
                    continue
            
            # If we get here, neither the field nor any of its aliases were found
            missing_fields.append(field)
            
        return missing_fields
    
    def integrate_with_pipeline(self, pipeline_result: Dict[str, Any], hmdb_id: str, required_fields: list) -> Dict[str, Any]:
        """
        Integrate the fallback coordinator with the main pipeline.
        This is a high-level method that can be called directly from the pipeline.
        
        Args:
            pipeline_result: The current result from the pipeline (may be incomplete)
            hmdb_id: The HMDB ID for the metabolite
            required_fields: List of fields that are required for the result
            
        Returns:
            Dictionary with all required fields, using fallback mechanisms if needed
        """
        # First populate any missing fields from aliases that might already be present
        pipeline_result = self._populate_fields_from_aliases(pipeline_result)
        
        # If we don't have an HMDB ID but have a name, try to discover the ID
        if not hmdb_id and "name" in pipeline_result:
            name = pipeline_result.get("name")
            discovered_hmdb_id = self.discover_hmdb_id_by_name(name)
            
            if discovered_hmdb_id:
                hmdb_id = discovered_hmdb_id
                # Add the discovered ID to the result
                pipeline_result["hmdb_id"] = hmdb_id
        
        # If we still don't have an HMDB ID, we can't proceed with field-based fallback
        if not hmdb_id:
            print("Error: Cannot perform field-based fallback without an HMDB ID")
            return pipeline_result
        
        # Analyze what fields are missing from the current result
        missing_fields = self.analyze_missing_fields(pipeline_result, required_fields)
        
        if not missing_fields:
            # If nothing is missing, return the result as is
            return pipeline_result
        
        print(f"Pipeline result is missing {len(missing_fields)} fields: {missing_fields}")
        print(f"Using fallback mechanism to fetch missing fields...")
        
        # Use the fallback mechanism to fetch missing fields
        complete_result = self.decide_and_fetch(missing_fields, hmdb_id, pipeline_result)
        
        # Final step: populate any remaining fields from aliases
        complete_result = self._populate_fields_from_aliases(complete_result)
        
        # Verify all fields were obtained
        final_missing = self.analyze_missing_fields(complete_result, required_fields)
        
        if final_missing:
            print(f"Warning: After fallback, still missing {len(final_missing)} fields: {final_missing}")
        else:
            print("All required fields have been successfully fetched!")
        
        return complete_result

    def _populate_fields_from_aliases(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Populate fields in the data using their aliases when the primary field is missing.
        This ensures that callers can consistently access data by the field name they requested,
        even if the API returned it under a different name.
        
        Args:
            data: Dictionary of data to process
            
        Returns:
            Dictionary with fields populated from aliases where needed
        """
        if not data:
            return data
            
        # Copy to avoid modifying during iteration
        result = data.copy()
        
        # For each field alias mapping
        for field, aliases in field_alias_map.items():
            # If the field is missing, try to populate it from an alias
            if field not in result:
                for alias in aliases:
                    if alias in result:
                        result[field] = result[alias]
                        break
        
        return result


def create_test_client(with_cache=False):
    """
    Create a test client for HMDB API with optional caching.
    Use this function to test if missing fields are due to cached responses.
    
    Args:
        with_cache: Whether to use caching (default: False)
        
    Returns:
        HMDBApiClient instance configured according to parameters
    """
    rate_limiter = RateLimiter()
    return HMDBApiClient(rate_limiter, use_cache=with_cache)


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
    
    # Test the new dynamic endpoint selection functionality
    def test_dynamic_endpoint_selection():
        print("\n=== Testing Dynamic Endpoint Selection ===")
        
        # Test 1: Get basic fields from a metabolite
        test_hmdb_id = "HMDB0000001"  # Glucose
        test_fields = ["hmdb_id", "name", "description", "synonyms"]
        
        print(f"\nTest 1: Fetching basic fields for {test_hmdb_id}")
        print(f"Fields requested: {test_fields}")
        
        # First, see which endpoints are selected
        endpoints = client.select_endpoints_for_fields(test_fields)
        print(f"Endpoints selected: {endpoints}")
        
        # Now fetch the fields
        result = client.fetch_fields_for_hmdb_id(test_hmdb_id, test_fields)
        print("Result:")
        print(json.dumps(result, indent=2))
        
        # Test 2: Get fields from different endpoints
        test_fields_2 = ["normal_concentrations", "enzyme_name", "health_effect"]
        
        print(f"\nTest 2: Fetching fields from different endpoints for {test_hmdb_id}")
        print(f"Fields requested: {test_fields_2}")
        
        # See which endpoints are selected
        endpoints = client.select_endpoints_for_fields(test_fields_2)
        print(f"Endpoints selected: {endpoints}")
        
        # Fetch the fields
        result = client.fetch_fields_for_hmdb_id(test_hmdb_id, test_fields_2)
        print("Result:")
        print(json.dumps(result, indent=2))
        
        # Test 3: Test optimizing to avoid metabolites endpoint
        test_fields_3 = ["enzyme_name", "biological_role", "natural_process"]
        
        print(f"\nTest 3: Testing optimization to avoid metabolites endpoint for {test_hmdb_id}")
        print(f"Fields requested: {test_fields_3}")
        
        # See which endpoints are selected
        endpoints = client.select_endpoints_for_fields(test_fields_3)
        print(f"Endpoints selected: {endpoints}")
        
        # Fetch the fields
        result = client.fetch_fields_for_hmdb_id(test_hmdb_id, test_fields_3)
        print("Result:")
        print(json.dumps(result, indent=2))
    
    # Test the new ApiFallbackCoordinator
    def test_api_fallback_coordinator():
        print("\n=== Testing API Fallback Coordinator ===")
        
        # Create a fallback coordinator
        coordinator = ApiFallbackCoordinator(client, max_retries=2)
        
        # Test 1: Simple fallback with existing data
        test_hmdb_id = "HMDB0000001"  # Glucose
        existing_data = {
            "hmdb_id": "HMDB0000001",
            "name": "Glucose"
        }
        missing_fields = ["description", "synonyms", "moldb_formula"]
        
        print(f"\nTest 1: Fallback with existing data for {test_hmdb_id}")
        print(f"Existing data: {existing_data}")
        print(f"Missing fields: {missing_fields}")
        
        result = coordinator.decide_and_fetch(missing_fields, test_hmdb_id, existing_data)
        print("Result after fallback:")
        print(json.dumps(result, indent=2))
        
        # Test 2: Fallback with empty existing data
        test_hmdb_id_2 = "HMDB0000122"  # Another metabolite
        missing_fields_2 = ["name", "description", "normal_concentrations"]
        
        print(f"\nTest 2: Fallback with empty existing data for {test_hmdb_id_2}")
        print(f"Missing fields: {missing_fields_2}")
        
        result = coordinator.decide_and_fetch(missing_fields_2, test_hmdb_id_2)
        print("Result after fallback:")
        print(json.dumps(result, indent=2))
        
        # Test 3: Fallback with multiple endpoints and retries
        test_hmdb_id_3 = "HMDB0000001"  # Glucose
        missing_fields_3 = ["enzyme_name", "biological_role", "natural_process"]
        
        print(f"\nTest 3: Fallback with multiple endpoints for {test_hmdb_id_3}")
        print(f"Missing fields: {missing_fields_3}")
        
        result = coordinator.decide_and_fetch(missing_fields_3, test_hmdb_id_3)
        print("Result after fallback:")
        print(json.dumps(result, indent=2))
        
        # Test 4: Integration with pipeline
        pipeline_result = {
            "hmdb_id": "HMDB0000001",
            "name": "Glucose",
            "description": "An aldohexose that occurs naturally in the free state and is a constituent of many oligosaccharides and polysaccharides."
        }
        required_fields = ["hmdb_id", "name", "description", "synonyms", "moldb_formula", "enzyme_name"]
        
        print(f"\nTest 4: Integration with pipeline for {test_hmdb_id}")
        print(f"Pipeline result: {pipeline_result}")
        print(f"Required fields: {required_fields}")
        
        complete_result = coordinator.integrate_with_pipeline(pipeline_result, test_hmdb_id, required_fields)
        print("Complete result after pipeline integration:")
        print(json.dumps(complete_result, indent=2))
        
        # Test 5: Name-based discovery (when no HMDB ID is known)
        pipeline_result_no_id = {
            "name": "Glucose"
        }
        required_fields_5 = ["hmdb_id", "name", "description", "moldb_formula"]
        
        print(f"\nTest 5: Name-based discovery without HMDB ID")
        print(f"Pipeline result: {pipeline_result_no_id}")
        print(f"Required fields: {required_fields_5}")
        
        complete_result = coordinator.integrate_with_pipeline(pipeline_result_no_id, "", required_fields_5)
        print("Complete result after name-based discovery and fallback:")
        print(json.dumps(complete_result, indent=2))
        
        # Test 6: Formula-based fallback
        pipeline_result_with_formula = {
            "hmdb_id": "HMDB0000001",
            "name": "Glucose",
            "chemical_formula": "C6H12O6"  # Glucose formula
        }
        required_fields_6 = ["hmdb_id", "name", "chemical_formula", "monoisotopic_molecular_weight", "ions"]
        
        print(f"\nTest 6: Formula-based fallback")
        print(f"Pipeline result: {pipeline_result_with_formula}")
        print(f"Required fields: {required_fields_6}")
        
        complete_result = coordinator.integrate_with_pipeline(pipeline_result_with_formula, "HMDB0000001", required_fields_6)
        print("Complete result after formula-based fallback:")
        print(json.dumps(complete_result, indent=2))
    
    # Uncomment to run the tests
    # test_dynamic_endpoint_selection()
    test_api_fallback_coordinator()
    
    # Example of how to use this system in a pipeline
    def pipeline_usage_example():
        print("\n=== Pipeline Usage Example ===")
        
        # 1. Create the necessary components
        rate_limiter = RateLimiter()
        client = HMDBApiClient(rate_limiter)
        fallback_coordinator = ApiFallbackCoordinator(client)
        
        # 2. Scenario 1: We know the name but not the HMDB ID
        print("\nScenario 1: We know the metabolite name but not its HMDB ID")
        
        # This is what we might get from Neo4j or entity extraction
        extracted_data = {
            "name": "Glucose",
            "description": "A simple sugar"
        }
        
        # Define what fields we need for our application
        required_fields = ["hmdb_id", "name", "description", "synonyms", "moldb_formula", "normal_concentrations"]
        
        print(f"Extracted data: {extracted_data}")
        print(f"Required fields: {required_fields}")
        
        # Use the fallback coordinator to handle discovery and field fetching
        result = fallback_coordinator.integrate_with_pipeline(extracted_data, "", required_fields)
        
        print("\nFinal enriched data:")
        print(json.dumps(result, indent=2))
        
        # 3. Scenario 2: We know the HMDB ID but need additional fields
        print("\nScenario 2: We know the HMDB ID but need additional fields")
        
        # This is what we might get from Neo4j
        neo4j_data = {
            "hmdb_id": "HMDB0000042",  # Another metabolite
            "name": "Some Metabolite",
            "chemical_formula": "C5H10O5"
        }
        
        # Define what fields we need for our application
        required_fields = ["hmdb_id", "name", "description", "synonyms", "moldb_formula", 
                          "normal_concentrations", "biospecimen_normal"]
        
        print(f"Neo4j data: {neo4j_data}")
        print(f"Required fields: {required_fields}")
        
        # Use the fallback coordinator to fetch missing fields
        result = fallback_coordinator.integrate_with_pipeline(neo4j_data, neo4j_data["hmdb_id"], required_fields)
        
        print("\nFinal enriched data:")
        print(json.dumps(result, indent=2))
    
    # Comment/uncomment to run the pipeline example
    pipeline_usage_example()