from typing import Dict, Any, List, Optional
from backend.utils.neo4j_connection import Neo4jConnection
from backend.utils.cache_manager import CacheManager

def infer_property_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOLEAN"
    elif isinstance(value, int):
        return "INTEGER"
    elif isinstance(value, float):
        return "FLOAT"
    elif isinstance(value, str):
        return "STRING"
    elif isinstance(value, list):
        return "LIST"
    elif isinstance(value, dict):
        return "MAP"
    elif value is None:
        return "NULL"
    else:
        return type(value).__name__.upper()
    
    
def get_node_properties(neo4j_conn: Neo4jConnection, label: str) -> Dict[str, str]:
    query = f"""
    MATCH (n:`{label}`)
    RETURN keys(n)[..5] AS sample_keys
    LIMIT 1
    """
    result = neo4j_conn.run_query(query)
    if not result:
        return {}
    
    sample_keys = result[0].get("sample_keys", [])
    return {key: "UNKNOWN" for key in sample_keys}

# def get_node_properties(neo4j_conn: Neo4jConnection, label: str) -> Dict[str, str]:
#     query = f"""
#     MATCH (n:`{label}`)
#     UNWIND keys(n) AS key
#     WITH key, head(collect(n[key])) AS sample_value
#     RETURN DISTINCT key, sample_value
#     """
#     results = neo4j_conn.run_query(query)
#     return {r["key"]: infer_property_type(r["sample_value"]) for r in results}

def get_relationship_properties(neo4j_conn: Neo4jConnection, rel_type: str) -> Dict[str, str]:
    query = f"""
    MATCH ()-[r:`{rel_type}`]->()
    UNWIND keys(r) AS key
    WITH key, head(collect(r[key])) AS sample_value
    RETURN DISTINCT key, sample_value
    """
    results = neo4j_conn.run_query(query)
    return {r["key"]: infer_property_type(r["sample_value"]) for r in results}

def get_relationship_mappings(neo4j_conn: Neo4jConnection, rel_type: str) -> List[str]:
    query = f"""
    MATCH (start)-[r:`{rel_type}`]->(end)
    RETURN DISTINCT labels(start) AS start_labels, labels(end) AS end_labels
    """
    results = neo4j_conn.run_query(query)
    mappings = []
    for record in results:
        for start in record["start_labels"]:
            for end in record["end_labels"]:
                mappings.append(f"(:{start})-[:{rel_type}]->(:{end})")
    return mappings

def generate_text_schema(neo4j_conn: Neo4jConnection, use_cache: bool = True, database: str = "default") -> str:
    """
    Generate a text representation of the Neo4j database schema.
    
    Args:
        neo4j_conn: Neo4j connection object
        use_cache: Whether to use cached schema if available
        database: Database name for cache key (useful for multi-database setups)
        
    Returns:
        Text representation of the schema
    """
    # Check if we should use cache and if a cached schema exists
    if use_cache:
        cache_manager = CacheManager()
        cached_schema = cache_manager.get_cached_schema(database)
        if cached_schema:
            print("Using cached database schema")
            return cached_schema
    
    # If no cache or cache disabled, generate the schema
    print("Generating database schema...")
    
    node_labels = [
        r["label"]
        for r in neo4j_conn.run_query("CALL db.labels() YIELD label RETURN label ORDER BY label")
    ]
    
    relationship_types = [
        r["relationshipType"]
        for r in neo4j_conn.run_query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType ORDER BY relationshipType")
    ]
    
    lines = ["## Node Definitions"]
    
    for label in node_labels:
        props = get_node_properties(neo4j_conn, label)
        prop_list = [f"  {k}: {v}" for k, v in props.items()]
        props_block = ",\n".join(prop_list)
        lines.append(f"(:{label}) {{\n{props_block}\n}}")
        lines.append("")
    
    lines.append("## Relationship Definitions")
    for rel_type in relationship_types:
        rel_props = get_relationship_properties(neo4j_conn, rel_type)
        if rel_props:
            rp_list = [f"  {k}: {v}" for k, v in rel_props.items()]
            rp_block = ",\n".join(rp_list)
            lines.append(f"[:{rel_type}] {{\n{rp_block}\n}}")
        else:
            lines.append(f"[:{rel_type}] {{ -- No properties -- }}")
        lines.append("")
    
    lines.append("## Relationship Mappings")
    for rel_type in relationship_types:
        mappings = get_relationship_mappings(neo4j_conn, rel_type)
        for mapping in mappings:
            lines.append(mapping)
    
    schema_text = "\n".join(lines)
    
    # Cache the generated schema if caching is enabled
    if use_cache:
        cache_manager = CacheManager()
        cache_manager.cache_schema(schema_text, database)
    
    return schema_text

def get_or_cache_schema(neo4j_conn: Neo4jConnection, database: str = "default", force_reload: bool = False) -> str:
    """
    Get schema from cache if available, or generate and cache it.
    
    Args:
        neo4j_conn: Neo4j connection object
        database: Database name for cache key
        force_reload: If True, skip cache and regenerate schema
        
    Returns:
        Text representation of the schema
    """
    # Use existing schema generation but control caching explicitly
    return generate_text_schema(
        neo4j_conn=neo4j_conn, 
        use_cache=not force_reload, 
        database=database
    )

if __name__ == "__main__":
    pass