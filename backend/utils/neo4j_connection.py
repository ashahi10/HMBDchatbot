from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError, ClientError

class Neo4jConnection:

    def __init__(self, uri: str, user: str, password: str):
        try:
            self._driver = GraphDatabase.driver(uri, auth=(user, password))
            self.test_connection()
        except AuthError:
            raise ValueError("Authentication failed. Check your username and password.")
        except ServiceUnavailable:
            raise ValueError("Unable to connect to the Neo4j database. Check that it's running and the URI is correct.")
        except Exception as e:
            raise ValueError(f"Unexpected error during Neo4j initialization: {str(e)}")

    def test_connection(self):
        records = self.run_query("RETURN 1 AS testVal")
        if not records or records[0].get('testVal') != 1:
            raise ValueError("Connection test failed. The query did not return the expected result.")

    def close(self):
        if self._driver:
            self._driver.close()

    def run_query(self, cypher_query: str, parameters: dict = None, limit: int = None) -> list:
        try:
            if limit is not None and isinstance(limit, int) and limit > 0:
                cypher_query = cypher_query.rstrip(';')
                if " LIMIT " not in cypher_query.upper():
                    cypher_query = f"{cypher_query} LIMIT {limit}"

            with self._driver.session() as session:
                result = session.run(cypher_query, parameters or {})
                data = result.data()
                
                import json
                result_json = json.dumps(data)
                token_count = len(result_json)

                if token_count > 5000:  # Hardcoded token limit
                    truncated_data = []
                    current_token_count = 0

                    for record in data:
                        record_json = json.dumps(record)
                        record_token_count = len(record_json)
                        if current_token_count + record_token_count > 5000:
                            break
                        truncated_data.append(record)
                        current_token_count += record_token_count

                    return truncated_data
                
                return data
        except ClientError as e:
            raise RuntimeError(f"Cypher error: {str(e)}")
        except Exception as e:
            raise RuntimeError(f"Unexpected error while running query: {str(e)}")
