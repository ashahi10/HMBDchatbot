from dotenv import load_dotenv
import os
from neo4j import GraphDatabase

# Load environment variables from .env file
load_dotenv()

def clear_neo4j_database(neo4j_uri, batch_size=1000):
    """
    Clears the Neo4j database by:
      - Dropping all constraints and indexes.
      - Deleting relationships and nodes in batches.
      - Clearing query caches.
    
    Parameters:
        neo4j_uri (str): URI for the Neo4j instance.
        batch_size (int): Number of records to delete per batch.
    """
    # Retrieve credentials from environment variables
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")
    
    # Connect to the Neo4j instance
    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
    with driver.session() as session:
        # Drop all constraints
        constraints_result = session.run("SHOW CONSTRAINTS YIELD name")
        for record in constraints_result:
            constraint_name = record['name']
            session.run(f"DROP CONSTRAINT `{constraint_name}`")
        
        # Drop all indexes
        indexes_result = session.run("SHOW INDEXES YIELD name")
        for record in indexes_result:
            index_name = record['name']
            session.run(f"DROP INDEX `{index_name}`")
        
        # Delete relationships in batches
        while True:
            result = session.run(
                f"MATCH ()-[r]-() WITH r LIMIT {batch_size} DELETE r RETURN count(r) AS count"
            )
            deleted_rels = result.single()["count"]
            if deleted_rels == 0:
                break  # No more relationships to delete
        
        # Delete nodes in batches
        while True:
            result = session.run(
                f"MATCH (n) WITH n LIMIT {batch_size} DETACH DELETE n RETURN count(n) AS count"
            )
            deleted_nodes = result.single()["count"]
            if deleted_nodes == 0:
                break  # No more nodes to delete
        
        # Clear query caches (if applicable)
        session.run("CALL db.clearQueryCaches()")
        
        print("Database cleared: all nodes, relationships, indexes, constraints, and caches have been deleted.")
    
    # Close the driver connection
    driver.close()
