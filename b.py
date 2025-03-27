from backend.utils.neo4j_connection import Neo4jConnection
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

neo4j_connection = Neo4jConnection(
    uri=os.getenv("NEO4J_URI"),
    user=os.getenv("NEO4J_USER"),
    password=os.getenv("NEO4J_PASSWORD")
)

def match_protein(protein: str):
    protein_query = f"""
        CALL db.index.fulltext.queryNodes("protein_names", "{protein}~") YIELD node, score
        RETURN node.gene_name AS gene_name, node.proteinAcc AS proteinAcc, node.protein_name AS protein_name, node.uniprot_id AS uniprot_id, score
        LIMIT 3
    """
    protein_results = neo4j_connection.run_query(protein_query)

    if protein_results and len(protein_results) > 0:
        print(protein_results)
        return protein_results[0]
    
    return None

def match_disease(disease: str):
    disease_query = f"""
        CALL db.index.fulltext.queryNodes("disease_names", "{disease}~") YIELD node, score
        RETURN node.name AS name, score
        LIMIT 3
    """
    disease_results = neo4j_connection.run_query(disease_query)

    if disease_results and len(disease_results) > 0:
        print(disease_results)
        return disease_results[0]

    return None

# print(match_protein("Creatine kinase"))

print(match_disease("alzheimers"))