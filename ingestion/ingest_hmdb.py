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

# clear_neo4j_database()


# create_indexes_and_constraints(neo4j_conn)

def do_it(file):
    print('Doing ' + file)
    return build_knowledge_graph_from_hmdb(neo4j_conn, file)


# print("Building knowledge graph from HMDB data...")

# serum = do_it('ingestion/HMDB_DATA/serum_metabolites.xml')
# print('Done ')

# feces = do_it('ingestion/HMDB_DATA/feces_metabolites.xml')
# print('Done ')

# urine = do_it('ingestion/HMDB_DATA/urine_metabolites.xml')
# print('Done ')

# saliva = do_it('ingestion/HMDB_DATA/saliva_metabolites.xml')
# print('Done ')

# csf = do_it('ingestion/HMDB_DATA/csf_metabolites.xml')
# print('Done ')

# sweat = do_it('ingestion/HMDB_DATA/sweat_metabolites.xml')
# print('Done ')


# print("DONE METABOLITES")
proteins = build_knowledge_graph_from_hmdb_proteins(neo4j_conn, 'ingestion/HMDB_DATA/hmdb_proteins.xml')
# 