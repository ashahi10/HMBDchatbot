import os
from dotenv import load_dotenv
from neo4j_connection import Neo4jConnection

# ---------------- CONFIG ----------------
load_dotenv()
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

def format_number(num):
    """Format number with thousands separator"""
    return f"{num:,}"

def run_verification_query(neo4j_connection: Neo4jConnection, title: str, query: str, expected_count: int = 0):
    """Run a verification query and format the results"""
    print(f"\n🔍 {title}")
    print("-" * 60)
    print(f"Query: {query}")
    print()
    
    try:
        result = neo4j_connection.run_query(query)
        
        if result:
            for record in result:
                for key, value in record.items():
                    if isinstance(value, int) and value > 1000:
                        print(f"{key}: {format_number(value)}")
                    else:
                        print(f"{key}: {value}")
            
            # Check if this is a count query and evaluate result
            if len(result) == 1 and 'count' in result[0]:
                count = result[0]['count']
                if count == expected_count:
                    print(f"✅ PASSED: Found {format_number(count)} (expected {expected_count})")
                else:
                    print(f"❌ FAILED: Found {format_number(count)} (expected {expected_count})")
            
        else:
            print("No results returned")
            
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")

def verify_description_migration(neo4j_connection: Neo4jConnection):
    """Comprehensive verification of the description migration"""
    
    print("🔍 COMPREHENSIVE DESCRIPTION MIGRATION VERIFICATION")
    print("=" * 80)
    
    # 1. Overall Statistics
    run_verification_query(
        neo4j_connection,
        "1. OVERALL STATISTICS",
        """
        MATCH (m:Metabolite)
        WITH count(m) as total_metabolites
        OPTIONAL MATCH (m2:Metabolite) WHERE m2.description IS NOT NULL
        WITH total_metabolites, count(m2) as with_descriptions
        OPTIONAL MATCH (d:Description)
        WITH total_metabolites, with_descriptions, count(d) as total_descriptions
        OPTIONAL MATCH (m3:Metabolite)-[:HAS_DESCRIPTION]->(d2:Description)
        RETURN 
            total_metabolites,
            with_descriptions,
            total_descriptions,
            count(DISTINCT m3) as metabolites_with_desc_nodes,
            count(*) as total_relationships
        """
    )
    
    # 2. Check for metabolites WITHOUT Description nodes (should be 0)
    run_verification_query(
        neo4j_connection,
        "2. METABOLITES MISSING DESCRIPTION NODES",
        """
        MATCH (m:Metabolite)
        WHERE m.description IS NOT NULL 
        AND NOT EXISTS((m)-[:HAS_DESCRIPTION]->(:Description))
        RETURN count(m) as count
        """,
        expected_count=0
    )
    
    # 3. Check for metabolites with MULTIPLE Description relationships (should be 0)
    run_verification_query(
        neo4j_connection,
        "3. METABOLITES WITH MULTIPLE DESCRIPTION RELATIONSHIPS",
        """
        MATCH (m:Metabolite)-[:HAS_DESCRIPTION]->(d:Description)
        WITH m, count(d) as desc_count
        WHERE desc_count > 1
        RETURN count(m) as count
        """,
        expected_count=0
    )
    
    # 4. Check for orphaned Description nodes (should be 0)
    run_verification_query(
        neo4j_connection,
        "4. ORPHANED DESCRIPTION NODES",
        """
        MATCH (d:Description)
        WHERE NOT EXISTS((:Metabolite)-[:HAS_DESCRIPTION]->(d))
        RETURN count(d) as count
        """,
        expected_count=0
    )
    
    # 5. Check for mismatched description text (should be 0)
    run_verification_query(
        neo4j_connection,
        "5. MISMATCHED DESCRIPTION TEXT",
        """
        MATCH (m:Metabolite)-[:HAS_DESCRIPTION]->(d:Description)
        WHERE m.description IS NOT NULL AND m.description <> d.text
        RETURN count(m) as count
        """,
        expected_count=0
    )
    
    # 6. Check for invalid Description IDs (should be 0)
    run_verification_query(
        neo4j_connection,
        "6. INVALID DESCRIPTION IDs",
        """
        MATCH (m:Metabolite)-[:HAS_DESCRIPTION]->(d:Description)
        WHERE d.descriptionId IS NULL OR d.descriptionId <> (m.accession + '_desc')
        RETURN count(d) as count
        """,
        expected_count=0
    )
    
    # 7. Check for Description nodes without required properties
    run_verification_query(
        neo4j_connection,
        "7. DESCRIPTION NODES WITH MISSING PROPERTIES",
        """
        MATCH (d:Description)
        WHERE d.descriptionId IS NULL 
           OR d.text IS NULL 
           OR d.metabolite_id IS NULL
        RETURN count(d) as count
        """,
        expected_count=0
    )
    
    # 8. Verify 1:1 relationship mapping
    run_verification_query(
        neo4j_connection,
        "8. VERIFY 1:1 METABOLITE-DESCRIPTION MAPPING",
        """
        MATCH (m:Metabolite)
        WHERE m.description IS NOT NULL
        WITH count(m) as metabolites_with_desc
        MATCH (m2:Metabolite)-[:HAS_DESCRIPTION]->(d:Description)
        WITH metabolites_with_desc, count(DISTINCT m2) as unique_metabolites, count(DISTINCT d) as unique_descriptions
        RETURN 
            metabolites_with_desc,
            unique_metabolites,
            unique_descriptions,
            CASE 
                WHEN metabolites_with_desc = unique_metabolites AND unique_metabolites = unique_descriptions 
                THEN 'PERFECT 1:1 MAPPING' 
                ELSE 'MAPPING ISSUES' 
            END as mapping_status
        """
    )
    
    # 9. Sample verification - show first 5 relationships
    run_verification_query(
        neo4j_connection,
        "9. SAMPLE RELATIONSHIPS (First 5)",
        """
        MATCH (m:Metabolite)-[:HAS_DESCRIPTION]->(d:Description)
        RETURN 
            m.accession as metabolite_accession,
            d.descriptionId as description_id,
            size(d.text) as description_length,
            d.metabolite_id as stored_metabolite_id,
            CASE WHEN m.description = d.text THEN 'MATCH' ELSE 'MISMATCH' END as text_match
        ORDER BY m.accession
        LIMIT 5
        """
    )
    
    # 10. Check for any duplicate Description IDs
    run_verification_query(
        neo4j_connection,
        "10. DUPLICATE DESCRIPTION IDs",
        """
        MATCH (d:Description)
        WITH d.descriptionId as desc_id, count(d) as count
        WHERE count > 1
        RETURN count(*) as count
        """,
        expected_count=0
    )
    
    # 11. Verify all metabolites with descriptions have exactly one relationship
    run_verification_query(
        neo4j_connection,
        "11. RELATIONSHIP COUNT VERIFICATION",
        """
        MATCH (m:Metabolite)
        WHERE m.description IS NOT NULL
        WITH m, size([(m)-[:HAS_DESCRIPTION]->() | 1]) as rel_count
        WITH 
            count(CASE WHEN rel_count = 0 THEN 1 END) as zero_relationships,
            count(CASE WHEN rel_count = 1 THEN 1 END) as one_relationship,
            count(CASE WHEN rel_count > 1 THEN 1 END) as multiple_relationships
        RETURN 
            zero_relationships,
            one_relationship,
            multiple_relationships
        """
    )
    
    # 12. Advanced integrity check - cross validation
    run_verification_query(
        neo4j_connection,
        "12. ADVANCED INTEGRITY CHECK",
        """
        MATCH (m:Metabolite)
        WHERE m.description IS NOT NULL
        OPTIONAL MATCH (m)-[:HAS_DESCRIPTION]->(d:Description)
        WITH 
            m,
            d,
            CASE 
                WHEN d IS NULL THEN 'NO_DESCRIPTION_NODE'
                WHEN m.description <> d.text THEN 'TEXT_MISMATCH'
                WHEN d.metabolite_id <> m.accession THEN 'ID_MISMATCH'
                WHEN d.descriptionId <> (m.accession + '_desc') THEN 'DESC_ID_MISMATCH'
                ELSE 'OK'
            END as status
        WITH status, count(*) as count
        RETURN status, count
        ORDER BY status
        """
    )
    
    # 13. Final Summary
    print(f"\n🏆 FINAL VERIFICATION SUMMARY")
    print("=" * 60)
    
    summary_query = """
    MATCH (m:Metabolite) WHERE m.description IS NOT NULL
    WITH count(m) as total_should_have_desc
    MATCH (m2:Metabolite)-[:HAS_DESCRIPTION]->(d:Description)
    WHERE m2.description IS NOT NULL 
    AND m2.description = d.text 
    AND d.metabolite_id = m2.accession
    AND d.descriptionId = (m2.accession + '_desc')
    RETURN 
        total_should_have_desc,
        count(DISTINCT m2) as perfectly_migrated,
        CASE 
            WHEN total_should_have_desc = count(DISTINCT m2) 
            THEN '🎉 PERFECT MIGRATION' 
            ELSE '⚠️ ISSUES FOUND' 
        END as final_status
    """
    
    try:
        result = neo4j_connection.run_query(summary_query)
        if result:
            record = result[0]
            total = record['total_should_have_desc']
            migrated = record['perfectly_migrated']
            status = record['final_status']
            
            print(f"Total metabolites with descriptions: {format_number(total)}")
            print(f"Perfectly migrated: {format_number(migrated)}")
            print(f"Success rate: {(migrated/total*100):.1f}%")
            print(f"Final status: {status}")
            
    except Exception as e:
        print(f"❌ Error in final summary: {str(e)}")

def main():
    try:
        print("🔌 Connecting to Neo4j...")
        conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)
        print("✅ Connected to Neo4j\n")
        
        verify_description_migration(conn)
        
        conn.close()
        print("\n✅ Verification completed")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 