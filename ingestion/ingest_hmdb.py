import os
from dotenv import load_dotenv
import traceback
from reset import clear_neo4j_database

from neo4j_connection import Neo4jConnection
from population_logic import (
    create_indexes_and_constraints,
    build_knowledge_graph_from_hmdb,
    build_knowledge_graph_from_hmdb_proteins
)

uri, user, password = 'bolt://localhost:7687', os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")
neo4j_conn = Neo4jConnection(uri, user, password, batch_size=10000)

import time

# print("CLEARING DATABASE")
clear_start = time.time()
# clear_neo4j_database(uri)
# print(f"DONE CLEARING DATABASE in {time.time() - clear_start} seconds")


# print('STARTING INDEXES AND CONSTRAINTS')
# index_start = time.time()
# create_indexes_and_constraints(neo4j_conn)
# print(f'DONE INDEXES AND CONSTRAINTS in {time.time() - index_start} seconds')

# print('STARTING METABOLITES')
# met_start = time.time()
# metabolites = build_knowledge_graph_from_hmdb(neo4j_conn, 'ingestion/HMDB_DATA/hmdb_metabolites.xml')
# print(f"DONE METABOLITES in {time.time() - met_start} seconds")


print('STARTING PROTEINS')
prot_start = time.time()
proteins = build_knowledge_graph_from_hmdb_proteins(neo4j_conn, 'ingestion/HMDB_DATA/hmdb_proteins.xml')
print(f"DONE PROTEINS in {time.time() - prot_start} seconds")


print(f"TOTAL TIME: {time.time() - clear_start} seconds")


# All nodes, relationships, indexes, constraints, and cached property keys have been deleted.
# DONE CLEARING DATABASE in 1266.2901084423065 seconds
# STARTING INDEXES AND CONSTRAINTS
# DONE INDEXES AND CONSTRAINTS in 3.240144968032837 seconds
# STARTING METABOLITES
# DONE METABOLITES in 15110.91052889824 seconds
# STARTING PROTEINS