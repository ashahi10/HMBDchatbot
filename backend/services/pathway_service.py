from typing import List, Dict, Optional
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PathwayService:
    """
    Service for retrieving and formatting pathway information related to metabolites.
    Provides functions for retrieving SMPDB and KEGG pathway information.
    """
    
    def __init__(self, neo4j_connection):
        """
        Initialize the pathway service with a Neo4j connection.
        
        Args:
            neo4j_connection: An initialized Neo4j connection object
        """
        self.neo4j_connection = neo4j_connection
    
    def get_pathways_for_metabolite(self, accession: str) -> List[Dict]:
        """
        Retrieve pathway information for a metabolite by its accession ID.
        Dynamically generates external URLs for each pathway.
        
        Args:
            accession: The metabolite accession identifier (e.g., HMDB0000001)
            
        Returns:
            List of dictionaries containing pathway name and URLs to SMPDB and KEGG
        """
        logger.info(f"Retrieving pathways for metabolite: {accession}")
        
        try:
            # Query Neo4j for pathways related to the metabolite
            cypher_query = """
            MATCH (m:Metabolite {accession: $accession})-[:INVOLVED_IN_PATHWAY]->(p:Pathway)
            RETURN p.pathway_name AS name, p.smpdb_id AS smpdb_id, p.kegg_map_id AS kegg_id
            """
            
            logger.debug(f"Executing Neo4j query for accession {accession}:\n{cypher_query}")
            
            # Execute the query with the accession parameter
            results = self.neo4j_connection.run_query(
                cypher_query, 
                parameters={"accession": accession}
            )
            
            logger.debug(f"Raw Neo4j results for {accession}:\n{json.dumps(results, indent=2)}")
        
            # Transform the results to include formatted URLs
            formatted_pathways = []
            for pathway in results:
                # Extract data from result
                name = pathway.get("name")
                smpdb_id = pathway.get("smpdb_id")
                kegg_id = pathway.get("kegg_id")
                
                logger.debug(f"Processing pathway:\nName: {name}\nSMPDB ID: {smpdb_id}\nKEGG ID: {kegg_id}")
                
                # Only add pathways with at least a name and one ID
                if name:
                    pathway_info = {"name": name}
                    
                    # Add SMPDB URL if ID exists
                    if smpdb_id:
                        pathway_info["smpdb_url"] = f"https://smpdb.ca/view/{smpdb_id}"
                        logger.debug(f"Added SMPDB URL for {name}: {pathway_info['smpdb_url']}")
                    
                    # Add KEGG URL if ID exists
                    if kegg_id:
                        pathway_info["kegg_url"] = f"https://www.kegg.jp/pathway/{kegg_id}"
                        logger.debug(f"Added KEGG URL for {name}: {pathway_info['kegg_url']}")
                    
                    formatted_pathways.append(pathway_info)
                    logger.debug(f"Added formatted pathway:\n{json.dumps(pathway_info, indent=2)}")
            
            logger.info(f"Found {len(formatted_pathways)} pathways for metabolite {accession}")
            logger.debug(f"Final formatted pathways:\n{json.dumps(formatted_pathways, indent=2)}")
            return formatted_pathways
            
        except Exception as e:
            logger.error(f"Error retrieving pathways for metabolite {accession}: {str(e)}")
            return []
    
    def get_pathways_by_metabolite_name(self, metabolite_name: str) -> List[Dict]:
        """
        Alternative method to retrieve pathway information using a metabolite name instead of accession.
        Useful when exact accession is not known but name is available.
        
        Args:
            metabolite_name: The name of the metabolite
            
        Returns:
            List of dictionaries containing pathway name and URLs to SMPDB and KEGG
        """
        logger.info(f"Retrieving pathways for metabolite by name: {metabolite_name}")
        
        try:
            # Direct query to get pathways by name without needing to find accession first
            direct_query = """
            MATCH (m:Metabolite)-[:INVOLVED_IN_PATHWAY]->(p:Pathway)
            WHERE toLower(m.name) = toLower($name)
            OR EXISTS { MATCH (m)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex) 
                       WHERE any(syn IN si.synonyms WHERE toLower(syn) = toLower($name)) }
            RETURN p.pathway_name AS name, p.smpdb_id AS smpdb_id, p.kegg_map_id AS kegg_id
            """
            
            # Execute direct query to get pathways
            results = self.neo4j_connection.run_query(
                direct_query,
                parameters={"name": metabolite_name}
            )
            
            # If we got results, format them and return
            if results and len(results) > 0:
                # Transform the results to include formatted URLs
                formatted_pathways = []
                for pathway in results:
                    # Extract data from result
                    name = pathway.get("name")
                    smpdb_id = pathway.get("smpdb_id")
                    kegg_id = pathway.get("kegg_id")
                    
                    # Only add pathways with at least a name
                    if name:
                        pathway_info = {"name": name}
                        
                        # Add SMPDB URL if ID exists
                        if smpdb_id:
                            pathway_info["smpdb_url"] = f"https://smpdb.ca/view/{smpdb_id}"
                        
                        # Add KEGG URL if ID exists
                        if kegg_id:
                            pathway_info["kegg_url"] = f"https://www.kegg.jp/pathway/{kegg_id}"
                            
                        
                        formatted_pathways.append(pathway_info)
                
                logger.info(f"Found {len(formatted_pathways)} pathways for metabolite {metabolite_name}")
                return formatted_pathways
            
            # If no results, try the original approach with accession lookup
            logger.info(f"No pathways found directly, trying accession lookup for: {metabolite_name}")
            
            # Get the accession ID for the metabolite
            accession_query = """
            MATCH (m:Metabolite)
            WHERE toLower(m.name) = toLower($name)
            OR EXISTS { MATCH (m)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex) 
                      WHERE any(syn IN si.synonyms WHERE toLower(syn) = toLower($name)) }
            RETURN m.accession AS accession LIMIT 1
            """
            
            # Execute query to find accession
            accession_results = self.neo4j_connection.run_query(
                accession_query,
                parameters={"name": metabolite_name}
            )
            
            # Check if we found an accession
            if not accession_results or len(accession_results) == 0:
                logger.warning(f"No accession found for metabolite name: {metabolite_name}")
                return []
                
            if "accession" not in accession_results[0]:
                logger.warning(f"No accession field in result for metabolite name: {metabolite_name}")
                return []
            
            # Get the accession ID and use it to find pathways
            accession = accession_results[0]["accession"]
            logger.info(f"Found accession {accession} for metabolite {metabolite_name}")
            return self.get_pathways_for_metabolite(accession)
            
        except Exception as e:
            logger.error(f"Error retrieving pathways by metabolite name {metabolite_name}: {str(e)}")
            return []


def get_pathways_for_metabolite(neo4j_connection, accession: str) -> List[Dict]:
    """
    Standalone utility function to retrieve pathway information for a metabolite.
    Creates a PathwayService instance on demand and delegates to it.
    
    Args:
        neo4j_connection: An initialized Neo4j connection
        accession: The metabolite accession identifier
        
    Returns:
        List of dictionaries containing pathway name and URLs to SMPDB and KEGG
    """
    service = PathwayService(neo4j_connection)
    return service.get_pathways_for_metabolite(accession)


def get_pathways_by_metabolite_name(neo4j_connection, metabolite_name: str) -> List[Dict]:
    """
    Standalone utility function to retrieve pathway information using a metabolite name.
    Creates a PathwayService instance on demand and delegates to it.
    
    Args:
        neo4j_connection: An initialized Neo4j connection
        metabolite_name: The name of the metabolite
        
    Returns:
        List of dictionaries containing pathway name and URLs to SMPDB and KEGG
    """
    service = PathwayService(neo4j_connection)
    return service.get_pathways_by_metabolite_name(metabolite_name)