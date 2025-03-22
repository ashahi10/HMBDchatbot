from dotenv import load_dotenv
import os
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()

def clear_neo4j_database(batch_size=10000):
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))

    with driver.session() as session:
        # Drop all constraints
        constraints_result = session.run("SHOW CONSTRAINTS YIELD name")
        for record in constraints_result:
            session.run(f"DROP CONSTRAINT `{record['name']}`")
        
        # Drop all indexes
        indexes_result = session.run("SHOW INDEXES YIELD name")
        for record in indexes_result:
            session.run(f"DROP INDEX `{record['name']}`")
        
        # Delete relationships in batches
        while True:
            result = session.run(
                f"MATCH ()-[r]-() WITH r LIMIT {batch_size} DELETE r RETURN count(r)"
            )
            count = result.single()[0]
            if count == 0:
                break  # Stop when no more relationships exist
        
        # Delete nodes in batches
        while True:
            result = session.run(
                f"MATCH (n) WITH n LIMIT {batch_size} DETACH DELETE n RETURN count(n)"
            )
            count = result.single()[0]
            if count == 0:
                break  # Stop when no more nodes exist
        
        # Clear query caches
        session.run("CALL db.clearQueryCaches()")

        # Restart the database via APOC (if available)
        try:
            session.run("CALL apoc.systemdb.execute('RESTART DATABASE', {})")
            print("Database restart triggered to ensure full cleanup.")
        except Exception as e:
            print(f"Restart via APOC failed. Please restart manually: {e}")

        print("All nodes, relationships, indexes, constraints, and cached property keys have been deleted.")

    driver.close()

# Retrieve credentials from environment variables

if __name__ == "__main__":
    clear_neo4j_database()