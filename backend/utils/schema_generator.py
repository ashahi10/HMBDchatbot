from typing import Dict, Any, List
from utils.neo4j_connection import Neo4jConnection

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
    UNWIND keys(n) AS key
    WITH key, head(collect(n[key])) AS sample_value
    RETURN DISTINCT key, sample_value
    """
    results = neo4j_conn.run_query(query)
    return {r["key"]: infer_property_type(r["sample_value"]) for r in results}

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

def generate_text_schema(neo4j_conn: Neo4jConnection) -> str:
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
    
    return "\n".join(lines)

if __name__ == "__main__":
    pass