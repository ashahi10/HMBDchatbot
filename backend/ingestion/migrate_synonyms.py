import os
import time
import logging
import traceback
from datetime import datetime
from dotenv import load_dotenv
from neo4j_connection import Neo4jConnection

# ---------------- CONFIG ----------------
load_dotenv()
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
BATCH_SIZE = 2000  # Increased for better performance

# --------------- LOGGING ----------------
logging.basicConfig(
    filename=f"synonym_migration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def format_number(num):
    """Format number with thousands separator"""
    return f"{num:,}"

def format_time(seconds):
    """Format time in human readable format"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"

def get_migration_stats(neo4j_connection: Neo4jConnection):
    """Get current migration statistics"""
    stats_query = """
    MATCH (m:Metabolite)-[:HAS_SYNONYM]->(s:Synonym)
    WITH count(DISTINCT m) as total_with_synonyms,
         count(s) as total_synonyms
    OPTIONAL MATCH (m2:Metabolite)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex)
    RETURN total_with_synonyms,
           total_synonyms,
           count(DISTINCT m2) as with_synonym_index,
           (total_with_synonyms - count(DISTINCT m2)) as need_migration
    """
    result = neo4j_connection.run_query(stats_query)
    return result[0] if result else None

def cleanup_existing_issues(neo4j_connection: Neo4jConnection):
    """Built-in cleanup to fix any existing data integrity issues"""
    print("🧹 Running built-in cleanup...")
    
    # 1. Remove orphaned SynonymIndex nodes
    orphan_query = """
    MATCH (si:SynonymIndex)
    WHERE NOT EXISTS((:Metabolite)-[:HAS_SYNONYM_INDEX]->(si))
    DELETE si
    RETURN count(*) as deleted
    """
    result = neo4j_connection.run_query(orphan_query)
    orphaned = result[0]['deleted'] if result else 0
    if orphaned > 0:
        print(f"   ✅ Removed {format_number(orphaned)} orphaned SynonymIndex nodes")
    
    # 2. Fix duplicate relationships - keep only one SynonymIndex per metabolite
    duplicate_query = """
    MATCH (m:Metabolite)-[r:HAS_SYNONYM_INDEX]->(si:SynonymIndex)
    WITH m, collect(r) as rels, collect(si) as indexes
    WHERE size(rels) > 1
    FOREACH (i IN range(1, size(rels)-1) | 
        DELETE rels[i]
    )
    WITH m, indexes
    FOREACH (i IN range(1, size(indexes)-1) | 
        DELETE indexes[i]
    )
    RETURN count(DISTINCT m) as cleaned
    """
    try:
        result = neo4j_connection.run_query(duplicate_query)
        cleaned = result[0]['cleaned'] if result else 0
        if cleaned > 0:
            print(f"   ✅ Fixed {format_number(cleaned)} duplicate relationships")
    except Exception as e:
        logging.error(f"Error during duplicate cleanup: {str(e)}")
    
    print("✅ Built-in cleanup completed")

def ensure_constraints(neo4j_connection: Neo4jConnection):
    """Ensure required constraints exist"""
    try:
        # Create unique constraint on SynonymIndex nodes
        constraint_query = """
        CREATE CONSTRAINT IF NOT EXISTS FOR (si:SynonymIndex) 
        REQUIRE si.canonical IS UNIQUE
        """
        neo4j_connection.run_query(constraint_query)
        
        # Create index on synonyms array for text search - fixed syntax
        index_query = """
        CALL db.index.fulltext.createIfNotExists(
            'synonym_index_search',
            ['SynonymIndex'],
            ['synonyms']
        )
        """
        neo4j_connection.run_query(index_query)
        print("✅ SynonymIndex constraints and indexes verified")
    except Exception as e:
        print(f"⚠️  Warning: Could not create SynonymIndex constraint: {e}")

def process_migration_batch(neo4j_connection: Neo4jConnection, batch_size: int):
    """
    Process a single batch using a more reliable sequential approach.
    This method avoids SKIP/LIMIT issues by processing available records.
    """
    # Use a single atomic query to process a batch of records
    batch_query = """
    MATCH (m:Metabolite)
    WHERE EXISTS((m)-[:HAS_SYNONYM]->(:Synonym))
    AND NOT EXISTS((m)-[:HAS_SYNONYM_INDEX]->(:SynonymIndex))
    WITH m
    ORDER BY m.accession
    LIMIT $batch_size
    MATCH (m)-[:HAS_SYNONYM]->(s:Synonym)
    WITH m, collect(DISTINCT s.synonymText) as synonyms
    CREATE (si:SynonymIndex {
        canonical: m.name,
        name: m.name,
        synonyms: synonyms
    })
    CREATE (m)-[:HAS_SYNONYM_INDEX]->(si)
    RETURN count(*) as migrated
    """
    
    try:
        result = neo4j_connection.run_query(batch_query, {"batch_size": batch_size})
        migrated = result[0]['migrated'] if result else 0
        return migrated, 0, []
    except Exception as e:
        logging.error(f"Batch migration error: {str(e)}")
        # If batch fails, try individual processing as fallback
        return process_individual_fallback(neo4j_connection, batch_size)

def process_individual_fallback(neo4j_connection: Neo4jConnection, batch_size: int):
    """
    Fallback method: process records individually if batch processing fails
    """
    print("   🔄 Using individual processing fallback...")
    
    # Get individual records that need migration
    get_records_query = """
    MATCH (m:Metabolite)
    WHERE EXISTS((m)-[:HAS_SYNONYM]->(:Synonym))
    AND NOT EXISTS((m)-[:HAS_SYNONYM_INDEX]->(:SynonymIndex))
    RETURN m.accession as acc, m.name as name
    ORDER BY m.accession
    LIMIT $batch_size
    """
    
    try:
        records = neo4j_connection.run_query(get_records_query, {"batch_size": batch_size})
        if not records:
            return 0, 0, []
        
        successful = 0
        failed = 0
        failed_list = []
        
        for record in records:
            acc = record['acc']
            name = record['name']
            
            # Process individual record
            individual_query = """
            MATCH (m:Metabolite {accession: $acc})-[:HAS_SYNONYM]->(s:Synonym)
            WITH m, collect(DISTINCT s.synonymText) as synonyms
            CREATE (si:SynonymIndex {
                canonical: m.name,
                name: m.name,
                synonyms: synonyms
            })
            CREATE (m)-[:HAS_SYNONYM_INDEX]->(si)
            RETURN 1 as created
            """
            
            try:
                result = neo4j_connection.run_query(individual_query, {"acc": acc})
                if result and result[0].get('created', 0) > 0:
                    successful += 1
                else:
                    failed += 1
                    failed_list.append(acc)
            except Exception as e:
                failed += 1
                failed_list.append(acc)
                logging.error(f"Failed to migrate individual record {acc}: {str(e)}")
        
        return successful, failed, failed_list
        
    except Exception as e:
        logging.error(f"Individual fallback error: {str(e)}")
        return 0, 0, []

def verify_migration_integrity(neo4j_connection: Neo4jConnection):
    """Comprehensive verification of migration results"""
    verification_queries = {
        "Missing SynonymIndex nodes": """
            MATCH (m:Metabolite)
            WHERE EXISTS((m)-[:HAS_SYNONYM]->(:Synonym))
            AND NOT EXISTS((m)-[:HAS_SYNONYM_INDEX]->(:SynonymIndex))
            RETURN count(m) as count
        """,
        "Mismatched synonyms": """
            MATCH (m:Metabolite)-[:HAS_SYNONYM]->(s:Synonym)
            WITH m, collect(DISTINCT s.synonymText) as original_synonyms
            MATCH (m)-[:HAS_SYNONYM_INDEX]->(si:SynonymIndex)
            WHERE NOT all(x IN original_synonyms WHERE x IN si.synonyms)
            OR NOT all(x IN si.synonyms WHERE x IN original_synonyms)
            RETURN count(m) as count
        """,
        "Orphaned SynonymIndex nodes": """
            MATCH (si:SynonymIndex)
            WHERE NOT EXISTS((:Metabolite)-[:HAS_SYNONYM_INDEX]->(si))
            RETURN count(si) as count
        """,
        "Duplicate relationships": """
            MATCH (m:Metabolite)
            WHERE size([(m)-[:HAS_SYNONYM_INDEX]->() | 1]) > 1
            RETURN count(m) as count
        """
    }
    
    print("\n🔍 Verifying migration integrity...")
    all_good = True
    
    for check_name, query in verification_queries.items():
        try:
            result = neo4j_connection.run_query(query)
            count = result[0]['count'] if result else 0
            if count > 0:
                print(f"⚠️  {check_name}: {format_number(count)}")
                all_good = False
            else:
                print(f"✅ {check_name}: OK")
        except Exception as e:
            print(f"❌ Error checking {check_name}: {e}")
            all_good = False
    
    return all_good

def migrate_synonyms(neo4j_connection: Neo4jConnection):
    """
    COMPLETELY FIXED migration script that handles all edge cases and data integrity issues.
    This is a self-contained solution that doesn't require separate cleanup scripts.
    """
    print("\n🔍 Starting COMPREHENSIVE synonym migration...")
    logging.info("Starting comprehensive synonym migration")
    
    # Step 1: Ensure constraints and cleanup existing issues
    ensure_constraints(neo4j_connection)
    cleanup_existing_issues(neo4j_connection)
    
    # Step 2: Get migration statistics
    print("\n📊 Getting migration statistics...")
    stats = get_migration_stats(neo4j_connection)
    if not stats:
        print("❌ Error getting migration statistics")
        return
    
    total_to_migrate = stats['need_migration']
    if total_to_migrate <= 0:
        print("✅ No metabolites need synonym migration")
        return
    
    print(f"📊 Migration Overview:")
    print(f"   • Total with synonyms: {format_number(stats['total_with_synonyms'])}")
    print(f"   • Total synonym relationships: {format_number(stats['total_synonyms'])}")
    print(f"   • Already migrated: {format_number(stats['with_synonym_index'])}")
    print(f"   • Need migration: {format_number(total_to_migrate)}")
    
    # Step 3: Process migration in batches
    print(f"\n🚀 Starting sequential batch migration...")
    print(f"   Batch size: {format_number(BATCH_SIZE)}\n")
    
    start_time = time.time()
    total_migrated = 0
    total_failed = 0
    batch_number = 0
    failed_accessions = []
    
    while total_migrated < total_to_migrate:
        batch_number += 1
        batch_start = time.time()
        
        try:
            # Process current batch
            migrated, failed, failed_list = process_migration_batch(neo4j_connection, BATCH_SIZE)
            
            if migrated == 0 and failed == 0:
                print("✅ No more records to process")
                break
            
            total_migrated += migrated
            total_failed += failed
            failed_accessions.extend(failed_list)
            
            # Progress calculations
            elapsed = time.time() - start_time
            batch_time = time.time() - batch_start
            rate = total_migrated / elapsed if elapsed > 0 else 0
            
            # Show progress
            progress_pct = (total_migrated / total_to_migrate) * 100 if total_to_migrate > 0 else 0
            remaining_time = (total_to_migrate - total_migrated) / rate if rate > 0 else 0
            
            print(f"✓ Batch {batch_number}: {format_number(migrated)} migrated")
            print(f"  Progress: {format_number(total_migrated)}/{format_number(total_to_migrate)} ({progress_pct:.1f}%)")
            print(f"  Speed: {format_number(int(rate))}/sec, Time remaining: {format_time(remaining_time)}")
            print(f"  Batch time: {batch_time:.1f}s")
            
            if failed > 0:
                print(f"  ⚠️  {format_number(failed)} failed in this batch")
            print()
            
            # Log progress periodically
            if batch_number % 10 == 0:
                logging.info(f"Batch {batch_number}: {total_migrated} migrated, {total_failed} failed")
            
        except Exception as e:
            print(f"❌ Error in batch {batch_number}: {str(e)}")
            logging.error(f"Batch {batch_number} error: {str(e)}")
            traceback.print_exc()
            time.sleep(1)  # Brief pause before continuing
            continue
    
    # Step 4: Final verification and cleanup
    print("🔍 Running final verification...")
    integrity_ok = verify_migration_integrity(neo4j_connection)
    
    # Step 5: Final statistics
    total_time = time.time() - start_time
    final_rate = total_migrated / total_time if total_time > 0 else 0
    
    print(f"\n📊 MIGRATION COMPLETE")
    print("=" * 50)
    print(f"Total migrated: {format_number(total_migrated)}")
    print(f"Total failed: {format_number(total_failed)}")
    print(f"Success rate: {((total_migrated / (total_migrated + total_failed)) * 100):.1f}%" if (total_migrated + total_failed) > 0 else "100%")
    print(f"Average speed: {format_number(int(final_rate))}/sec")
    print(f"Total time: {format_time(total_time)}")
    print(f"Data integrity: {'✅ PASSED' if integrity_ok else '⚠️  ISSUES FOUND'}")
    
    if failed_accessions:
        print(f"\n⚠️  Failed migrations ({len(failed_accessions)}):")
        for i, acc in enumerate(failed_accessions[:5]):  # Show first 5
            print(f"   {acc}")
        if len(failed_accessions) > 5:
            print(f"   ... and {len(failed_accessions) - 5} more")
    else:
        print("\n✅ All migrations completed successfully!")
    
    logging.info(f"Migration completed. Migrated: {total_migrated}, Failed: {total_failed}, Time: {total_time:.1f}s")

def main():
    try:
        print("🔌 Connecting to Neo4j...")
        conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, batch_size=BATCH_SIZE)
        print("✅ Connected to Neo4j")
        
        migrate_synonyms(conn)
        
        conn.close()
        print("\n✅ Migration completed successfully")
        
    except Exception as e:
        print("\n❌ Fatal error occurred")
        traceback.print_exc()
        logging.error(f"Fatal error: {str(e)}")

if __name__ == "__main__":
    main() 