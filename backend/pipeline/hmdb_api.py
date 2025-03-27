# Description: This script contains a class for interacting with the HMDB API. It includes methods for making all GET requests to various endpoints and testing all endpoints. The results of the tests are saved to a text file.
import os
import requests
import json
import time
from typing import Any, Dict, Optional
from dotenv import load_dotenv
# from requests_cache import CachedSession

load_dotenv()

# # Setting up requests-cache for caching
# cached_session = CachedSession('hmdb_cache', backend='sqlite', expire_after=3600)# Cache expires after 1 hour


#cache will store the full URL of the request as the key and the response data as the value.
# if a request was made less than an hour ago, the cached result will be returned instead of making a new API call.
#The TTL (Time-to-Live) is working correctly and will delete expired entries after 1 hour.


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
    def __init__(self, rate_limiter: RateLimiter):
        self.api_key = os.getenv("HMDB_API_KEY")
        self.base_url = os.getenv("HMDB_BASE_URL")
        self.headers = {"Content-Type": "application/json"}
        self.rate_limiter = rate_limiter

    def _build_url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint}/?api-key={self.api_key}"

    def get(self, endpoint: str) -> Optional[Dict[str, Any]]:
        if not self.rate_limiter.can_make_get_request():
            print("GET request limit reached. Try again later.")
            return None

        url = self._build_url(endpoint)
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            self.rate_limiter.record_get_request()
            return response.json()
        except requests.RequestException as e:
            print(f"GET request failed: {e}")
            return None
     
    def post(self, endpoint: str, payload: dict) -> Optional[Dict[str, Any]]:
        if not self.rate_limiter.can_make_get_request():
            return None

        url = f"{self.base_url}/{endpoint}/?api-key={self.api_key}"
        try:
            response = requests.post(url, json=payload, headers=self.headers)
            response.raise_for_status()
            self.rate_limiter.record_get_request()
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