from typing import List, Dict, Any, AsyncGenerator
from pipeline.config import PipelineConfig
from pipeline.stream_processor import StreamProcessor
from datetime import datetime

class QueryAttempt:
    def __init__(self, query: str, error: str = None, results: List[Dict[str, Any]] = None):
        self.query = query
        self.error = error
        self.results = results or []
        self.timestamp = datetime.now()

class QueryManager:
    def __init__(self, config: PipelineConfig, retry_chain: Any):
        self.config = config
        self.retry_chain = retry_chain
        self.current_results: List[Dict[str, Any]] = []
        self.current_query: str = ""
        self.max_retries = 5
        self.query_history: List[QueryAttempt] = []

    def _add_to_history(self, query: str, error: str = None, results: List[Dict[str, Any]] = None):
        attempt = QueryAttempt(query, error, results)
        self.query_history.append(attempt)

    async def execute_query(self, query_plan: Any, query_response: str, error: str = None) -> AsyncGenerator[str, None]:
        retry_count = 0
        while retry_count <= self.max_retries:
            try:
                self.current_results = self.config.neo4j_connection.run_query(query_response)
                self.current_query = query_response
                self._add_to_history(query_response, results=self.current_results)
                yield StreamProcessor.format_message("Query Results", f"{self.current_results}")
                break
            except Exception as e:
                error = str(e)
                retry_count += 1
                self._add_to_history(query_response, error=error)
                
                if retry_count > self.max_retries:
                    yield StreamProcessor.format_message("Error", f"Query failed after {self.max_retries} retries: {error}")
                    self.current_results = []
                    break
                
                yield StreamProcessor.format_message("Retry", f"Attempt {retry_count} of {self.max_retries}: {error}")
                retry_inputs = {
                    "query_plan": query_plan,
                    "schema": self.config.neo4j_schema_text,
                    "old_query": query_response,
                    "error": error,
                    "query_history": [{"query": h.query, "error": h.error} for h in self.query_history[-3:]]
                }
                retry_accumulator: List[str] = []
                async for message in StreamProcessor.process_stream(self.retry_chain, "Query execution", retry_inputs, retry_accumulator):
                    yield message
                query_response = "".join(retry_accumulator)

    async def handle_empty_results(self, query_plan: Any, query_response: str, neo4j_results: List[Dict[str, Any]]) -> AsyncGenerator[str, None]:
        retry_count = 0
        while retry_count <= self.max_retries:
            if len(neo4j_results) > 0:
                break
                
            retry_count += 1
            if retry_count > self.max_retries:
                yield StreamProcessor.format_message("Error", f"No results found after {self.max_retries} retries")
                break
                
            yield StreamProcessor.format_message("Retry", f"Attempt {retry_count} of {self.max_retries}: No results found, rerunning query...")
            retry_inputs = {
                "query_plan": query_plan,
                "schema": self.config.neo4j_schema_text,
                "old_query": query_response,
                "error": "This query returned no results. Please try again. Remember Metabolite is generally the central node, and the other entities are connected to it.",
                "query_history": [{"query": h.query, "error": h.error} for h in self.query_history[-3:]]
            }
            retry_accumulator: List[str] = []
            async for message in StreamProcessor.process_stream(self.retry_chain, "Query execution", retry_inputs, retry_accumulator):
                yield message
            query_response = "".join(retry_accumulator)
            
            self.current_results = self.config.neo4j_connection.run_query(query_response)
            self.current_query = query_response
            self._add_to_history(query_response, results=self.current_results)
            neo4j_results = self.current_results

    def get_current_results(self) -> List[Dict[str, Any]]:
        return self.current_results

    def get_current_query(self) -> str:
        return self.current_query