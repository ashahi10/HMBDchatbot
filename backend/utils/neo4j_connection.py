from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError, ClientError
from typing import List, Dict, Optional, Any
import json

# Import our cache manager
from backend.utils.cache_manager import CacheManager

class Neo4jConnection:

    def __init__(self, uri: str, user: str, password: str, batch_size: int = 1000, use_cache: bool = True):
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
            
            # Initialize batch queue
            self._batch_size = batch_size
            self._queued_queries = []
            
            # Initialize cache manager if caching is enabled
            self._use_cache = use_cache
            if self._use_cache:
                self._cache_manager = CacheManager()
                
            # Test connection after cache manager is initialized
            self.test_connection()
            
        except AuthError:
            raise ValueError("Authentication failed. Check your username and password.")
        except ServiceUnavailable:
            raise ValueError("Unable to connect to the Neo4j database. Check that it's running and the URI is correct.")
        except Exception as e:
            raise ValueError(f"Unexpected error during Neo4j initialization: {str(e)}")

    def test_connection(self):
        records = self.run_query("RETURN 1 AS testVal", bypass_cache=True)
        if not records or records[0].get('testVal') != 1:
            raise ValueError("Connection test failed. The query did not return the expected result.")

    def close(self):
        self.flush_queries()
        if self._driver:
            self._driver.close()

    def add_query(self, cypher_query: str, parameters: dict = None):
        self._queued_queries.append((cypher_query, parameters or {}))
        if len(self._queued_queries) >= self._batch_size:
            self.flush_queries()

    def flush_queries(self):
        if not self._queued_queries:
            return

        with self._driver.session() as session:
            def run_tx(tx, queries):
                for query_text, params in queries:
                    tx.run(query_text, params)
            session.execute_write(run_tx, self._queued_queries)

        self._queued_queries.clear()

    def run_query(self, cypher_query: str, parameters: dict = None, limit: int = None, token_limit: int = 5000, bypass_cache: bool = False) -> list:
        # Check cache first if enabled and not bypassing
        if self._use_cache and not bypass_cache:
            # If using parameters, include them in the cache key by appending to the query string
            cache_key = cypher_query
            if parameters:
                cache_key = f"{cypher_query}__params__{json.dumps(parameters, sort_keys=True)}"
            
            cached_result = self._cache_manager.get_cached_query_result(cache_key)
            if cached_result is not None:
                print(f"Using cached result for query: {cypher_query[:50]}...")
                return cached_result

        try:
            if limit is not None and isinstance(limit, int) and limit > 0:
                cypher_query = cypher_query.rstrip(';')

                if " LIMIT " not in cypher_query.upper():
                    cypher_query = f"{cypher_query} LIMIT {limit}"

            with self._driver.session() as session:
                result = session.run(cypher_query, parameters or {}, timeout=30)
                data = result.data()
                import json
                result_json = json.dumps(data)
                token_count = len(result_json)

                if token_count > token_limit:
                    truncated_data = []
                    current_token_count = 0

                    for record in data:
                        record_json = json.dumps(record)
                        record_token_count = len(record_json)
                        if current_token_count + record_token_count > token_limit:
                            break
                        truncated_data.append(record)
                        current_token_count += record_token_count

                    # Cache the truncated result if caching is enabled
                    if self._use_cache:
                        cache_key = cypher_query
                        if parameters:
                            cache_key = f"{cypher_query}__params__{json.dumps(parameters, sort_keys=True)}"
                        self._cache_manager.cache_query_result(cache_key, truncated_data)
                    
                    return truncated_data
                
                # Cache the full result if caching is enabled
                if self._use_cache:
                    cache_key = cypher_query
                    if parameters:
                        cache_key = f"{cypher_query}__params__{json.dumps(parameters, sort_keys=True)}"
                    self._cache_manager.cache_query_result(cache_key, data)
                
                return data
        except ClientError as e:
            raise RuntimeError(f"Cypher error: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while running query: {str(e)}")
