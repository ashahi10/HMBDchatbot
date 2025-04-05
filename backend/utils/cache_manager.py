import os
import json
import time
import hashlib
from typing import Any, Dict, Optional, List, Union
from pathlib import Path
import pickle
from datetime import datetime, timedelta

class CacheManager:
    """
    Utility for managing different types of caches in the application.
    Handles schema caching, Neo4j query caching, and API response caching.
    """
    
    def __init__(self, 
                 base_path: str = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache"), 
                 schema_cache_ttl: int = 86400,      # 1 day in seconds
                 query_cache_ttl: int = 3600,        # 1 hour in seconds
                 api_cache_ttl: int = 43200):        # 12 hours in seconds
        
        self.base_path = Path(base_path)
        self.schema_cache_path = self.base_path / "schema"
        self.query_cache_path = self.base_path / "queries"
        self.api_cache_path = self.base_path / "api_responses"
        
        # Create cache directories if they don't exist
        self._create_cache_dirs()
        
        # Set time-to-live for different cache types
        self.ttl = {
            'schema': schema_cache_ttl,
            'query': query_cache_ttl,
            'api': api_cache_ttl
        }
    
    def _create_cache_dirs(self) -> None:
        """Create all required cache directories if they don't exist"""
        for path in [self.base_path, self.schema_cache_path, self.query_cache_path, self.api_cache_path]:
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)
    
    def _generate_key(self, data: Union[str, Dict]) -> str:
        """Generate a unique cache key based on the input data"""
        if isinstance(data, dict):
            data_str = json.dumps(data, sort_keys=True)
        else:
            data_str = str(data)
        
        return hashlib.md5(data_str.encode()).hexdigest()
    
    def _is_cache_valid(self, cache_path: Path, cache_type: str) -> bool:
        """Check if a cache file is still valid based on its TTL"""
        if not cache_path.exists():
            return False
        
        file_time = datetime.fromtimestamp(cache_path.stat().st_mtime)
        current_time = datetime.now()
        max_age = timedelta(seconds=self.ttl[cache_type])
        
        return (current_time - file_time) < max_age
    
    # Schema Cache Methods
    def cache_schema(self, schema: str, database: str = "default") -> None:
        """Cache the database schema"""
        # Use a consistent filename for schema cache instead of multiple files
        cache_file = self.schema_cache_path / "schema_cache.json"
        
        # Load existing schemas if file exists
        schemas = {}
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    schemas = json.load(f)
            except json.JSONDecodeError:
                # If file is corrupted, start fresh
                schemas = {}
        
        # Update the schema for this database
        schemas[database] = {
            "schema": schema,
            "timestamp": time.time()
        }
        
        # Write back the updated schemas
        with open(cache_file, 'w') as f:
            json.dump(schemas, f, indent=2)
    
    def get_cached_schema(self, database: str = "default") -> Optional[str]:
        """Retrieve cached schema if it exists and is valid"""
        cache_file = self.schema_cache_path / "schema_cache.json"
        
        if not cache_file.exists():
            return None
            
        try:
            with open(cache_file, 'r') as f:
                schemas = json.load(f)
                
            if database not in schemas:
                return None
                
            db_cache = schemas[database]
            cache_time = db_cache.get("timestamp", 0)
            current_time = time.time()
            
            # Check if cache is still valid
            if (current_time - cache_time) < self.ttl['schema']:
                return db_cache.get("schema")
                
            return None
        except (json.JSONDecodeError, KeyError):
            return None
    
    # Query Cache Methods
    def cache_query_result(self, query: str, result: List[Dict]) -> None:
        """Cache Neo4j query results"""
        # Use a single consolidated file for query cache
        cache_file = self.query_cache_path / "query_cache.json"
        
        # Generate a key for this query
        key = self._generate_key(query)
        
        # Load existing query cache if file exists
        query_cache = {}
        if cache_file.exists():
            try:
                with open(cache_file, 'r') as f:
                    query_cache = json.load(f)
            except json.JSONDecodeError:
                # If file is corrupted, start fresh
                query_cache = {}
        
        # Update the cache for this query
        query_cache[key] = {
            "query": query,
            "result": result,
            "timestamp": time.time()
        }
        
        # Write back the updated cache
        with open(cache_file, 'w') as f:
            json.dump(query_cache, f)
    
    def get_cached_query_result(self, query: str) -> Optional[List[Dict]]:
        """Retrieve cached query results if valid"""
        cache_file = self.query_cache_path / "query_cache.json"
        
        if not cache_file.exists():
            return None
            
        # Generate key for this query
        key = self._generate_key(query)
        
        try:
            # Check if cache file's modification time is within TTL
            file_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
            current_time = datetime.now()
            max_age = timedelta(seconds=self.ttl['query'])
            
            if (current_time - file_time) > max_age:
                return None
                
            with open(cache_file, 'r') as f:
                query_cache = json.load(f)
                
            if key not in query_cache:
                return None
                
            cached_entry = query_cache[key]
            cache_time = cached_entry.get("timestamp", 0)
            
            # Check if this specific entry is still valid
            if (time.time() - cache_time) < self.ttl['query']:
                return cached_entry.get("result")
                
            return None
        except (json.JSONDecodeError, KeyError):
            return None
    
    # API Cache Methods
    def cache_api_response(self, endpoint: str, params: Dict, response: Dict) -> None:
        """Cache API response data"""
        cache_data = {
            "endpoint": endpoint,
            "params": params
        }
        key = self._generate_key(cache_data)
        cache_file = self.api_cache_path / f"{key}.json"
        
        with open(cache_file, 'w') as f:
            json.dump({
                "endpoint": endpoint,
                "params": params,
                "response": response,
                "timestamp": time.time()
            }, f)
    
    def get_cached_api_response(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """Retrieve cached API response if valid"""
        cache_data = {
            "endpoint": endpoint,
            "params": params
        }
        key = self._generate_key(cache_data)
        cache_file = self.api_cache_path / f"{key}.json"
        
        if self._is_cache_valid(cache_file, 'api'):
            with open(cache_file, 'r') as f:
                data = json.load(f)
                return data["response"]
        return None
    
    def clear_cache(self, cache_type: Optional[str] = None) -> None:
        """
        Clear specific or all caches
        
        Args:
            cache_type: 'schema', 'query', 'api', or None to clear all
        """
        if cache_type == 'schema' or cache_type is None:
            schema_cache = self.schema_cache_path / "schema_cache.json"
            if schema_cache.exists():
                schema_cache.unlink()
                
        if cache_type == 'query' or cache_type is None:
            query_cache = self.query_cache_path / "query_cache.json"
            if query_cache.exists():
                query_cache.unlink()
                
        if cache_type == 'api' or cache_type is None:
            for file in self.api_cache_path.glob('*.json'):
                file.unlink() 