# from dotenv import load_dotenv
# import os
# from neo4j import GraphDatabase

# # Load environment variables from .env file
# load_dotenv()

# def clear_neo4j_database(neo4j_uri, batch_size=1000):
#     """
#     Clears the Neo4j database by:
#       - Dropping all constraints and indexes.
#       - Deleting relationships and nodes in batches.
#       - Clearing query caches.
    
#     Parameters:
#         neo4j_uri (str): URI for the Neo4j instance.
#         batch_size (int): Number of records to delete per batch.
#     """
#     # Retrieve credentials from environment variables
#     neo4j_user = os.getenv("NEO4J_USERNAME")
#     neo4j_password = os.getenv("NEO4J_PASSWORD")
    
#     # Connect to the Neo4j instance
#     driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
    
#     with driver.session() as session:
#         # Drop all constraints
#         constraints_result = session.run("SHOW CONSTRAINTS YIELD name")
#         for record in constraints_result:
#             constraint_name = record['name']
#             session.run(f"DROP CONSTRAINT `{constraint_name}`")
        
#         # Drop all indexes
#         indexes_result = session.run("SHOW INDEXES YIELD name")
#         for record in indexes_result:
#             index_name = record['name']
#             session.run(f"DROP INDEX `{index_name}`")
        
#         # Delete relationships in batches
#         while True:
#             result = session.run(
#                 f"MATCH ()-[r]-() WITH r LIMIT {batch_size} DELETE r RETURN count(r) AS count"
#             )
#             deleted_rels = result.single()["count"]
#             if deleted_rels == 0:
#                 break  # No more relationships to delete
        
#         # Delete nodes in batches
#         while True:
#             result = session.run(
#                 f"MATCH (n) WITH n LIMIT {batch_size} DETACH DELETE n RETURN count(n) AS count"
#             )
#             deleted_nodes = result.single()["count"]
#             if deleted_nodes == 0:
#                 break  # No more nodes to delete
        
#         # Clear query caches (if applicable)
#         session.run("CALL db.clearQueryCaches()")
        
#         print("Database cleared: all nodes, relationships, indexes, constraints, and caches have been deleted.")
    
#     # Close the driver connection
#     driver.close()
from dotenv import load_dotenv
import os
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()

def clear_neo4j_database(neo4j_uri, batch_size=10000):
    """
    Clears Neo4j using APOC for fast deletion of all nodes, relationships, constraints, and indexes.
    """
    user = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(neo4j_uri, auth=(user, password))
    with driver.session() as session:
        print("🔧 Dropping constraints...")
        constraints = session.run("SHOW CONSTRAINTS YIELD name").data()
        for c in constraints:
            session.run(f"DROP CONSTRAINT `{c['name']}`")
        print(f"✅ Dropped {len(constraints)} constraints.")

        print("🔧 Dropping indexes...")
        indexes = session.run("SHOW INDEXES YIELD name").data()
        for i in indexes:
            session.run(f"DROP INDEX `{i['name']}`")
        print(f"✅ Dropped {len(indexes)} indexes.")

        print("🧹 Deleting all relationships (APOC)...")
        session.run("""
            CALL apoc.periodic.iterate(
              "MATCH (n) RETURN n",
              "DETACH DELETE n",
              {batchSize: $batch, parallel: true}
            )
        """, {"batch": batch_size})
        print("✅ Deleted all relationships.")

        print("🧹 Deleting all nodes (APOC)...")
        session.run("""
            CALL apoc.periodic.iterate(
                "MATCH (n) RETURN n",
                "DETACH DELETE n",
                {batchSize:10000, parallel:true}
            )
        """)
        print("✅ Deleted all nodes.")

        print("🧼 Clearing query cache...")
        session.run("CALL db.clearQueryCaches()")
        print("✅ Cache cleared.")

    driver.close()
    print("🎉 Database fully reset using APOC.")

