from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

from pipeline.config import PipelineConfig

class Entity(BaseModel):
    name: str = Field(..., description="The name of the entity")
    type: str = Field(..., description="The entity category")
    confidence: float = Field(..., description="Confidence score (0-1)")

class EntityList(BaseModel):
    entities: List[Entity] = Field(..., description="List of extracted entities")

class EntityManager:
    def __init__(self, config: PipelineConfig):
        self.config = config

    def match_metabolite(self, metabolite: str) -> Optional[str]:
        metabolite_query = f"""
            CALL db.index.fulltext.queryNodes("metabolite_names", "{metabolite}~0.5") YIELD node, score
            RETURN node.name AS name, score
            LIMIT {self.config.entities.max_results}
        """
        metabolite_results = self.config.neo4j_connection.run_query(metabolite_query)
        
        for result in metabolite_results:
            if result['score'] > self.config.entities.confidence_threshold:
                return result['name']
        
        synonyms_query_fuzzy = f"""
            CALL db.index.fulltext.queryNodes("synonymsFullText", "{metabolite}~{self.config.entities.fuzzy_threshold}") YIELD node, score
            RETURN node.synonymText AS synonymText, score
            LIMIT {self.config.entities.max_results}
        """
        synonyms_results = self.config.neo4j_connection.run_query(synonyms_query_fuzzy)
        
        if not synonyms_results or len(synonyms_results) == 0:
            synonyms_query_wildcard = f"""
                CALL db.index.fulltext.queryNodes("synonymsFullText", "{metabolite}*") YIELD node, score
                RETURN node.synonymText AS synonymText, score
                LIMIT {self.config.entities.max_results}
            """
            synonyms_results = self.config.neo4j_connection.run_query(synonyms_query_wildcard)
        
        for synonym_result in synonyms_results:
            if synonym_result['score'] > self.config.entities.synonym_threshold:
                associated_metabolite_query = f"""
                    MATCH (m:Metabolite)-[:HAS_SYNONYM]->(s:Synonym)
                    WHERE toLower(s.synonymText) = toLower("{synonym_result['synonymText']}")
                    RETURN m.name AS name
                    LIMIT 1
                """
                metabolite_match = self.config.neo4j_connection.run_query(associated_metabolite_query)
                if metabolite_match and len(metabolite_match) > 0:
                    return metabolite_match[0]['name']
        
        return None
    
    def match_protein(self, protein: str) -> Optional[str]:
        protein_query = f"""
            CALL db.index.fulltext.queryNodes("protein_names", "{protein}~0.5") YIELD node, score
            RETURN node.gene_name AS gene_name, node.proteinAcc AS proteinAcc, node.protein_name AS protein_name, node.uniprot_id AS uniprot_id, score
            LIMIT {self.config.entities.max_results}
        """
        protein_results = self.config.neo4j_connection.run_query(protein_query)
        for result in protein_results:
            if result['score'] > self.config.entities.confidence_threshold:
                return result['protein_name']
        
        return None
    
    def match_disease(self, disease: str) -> Optional[str]:
        disease_query = f"""
            CALL db.index.fulltext.queryNodes("disease_names", "{disease}~0.5") YIELD node, score
            RETURN node.diseaseName AS name, score
            LIMIT {self.config.entities.max_results}
        """
        disease_results = self.config.neo4j_connection.run_query(disease_query)
        for result in disease_results:
            if result['score'] > self.config.entities.confidence_threshold:
                return result['name']
        
        return None

    def get_metabolite_descriptions(self, metabolites: List[str]) -> List[Dict[str, Any]]:
        descriptions = []
        for metabolite in metabolites:
            results = self.config.neo4j_connection.run_query(f"""
                MATCH (m:Metabolite)
                WHERE toLower(m.name) = toLower('{metabolite}')
                OR EXISTS {{
                    MATCH (m)-[:HAS_SYNONYM]->(s:Synonym)
                    WHERE toLower(s.synonymText) = toLower('{metabolite}')
                }}
                RETURN m.description
            """)
            descriptions.extend(results)
        return descriptions 