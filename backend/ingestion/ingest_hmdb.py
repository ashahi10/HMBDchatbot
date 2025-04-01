import os
import time
import traceback
import logging
from dotenv import load_dotenv
from neo4j_connection import Neo4jConnection
from population_logic import (
    create_indexes_and_constraints,
    build_knowledge_graph_from_hmdb,
    build_knowledge_graph_from_hmdb_proteins,
)

# ---------------- CONFIG ----------------
load_dotenv()
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
BATCH_SIZE = 10000
SKIP_CLEAR = True  # Set True to skip clearing Neo4j

# Explicit XML file paths to avoid directory issues
XML_FILES = [
    # "HMDB_DATA/csf_metabolites.xml",
    # "HMDB_DATA/saliva_metabolites.xml",
    # "HMDB_DATA/serum_metabolites.xml",
    # "HMDB_DATA/sweat_metabolites.xml",
    # "HMDB_DATA/urine_metabolites.xml",
    # "HMDB_DATA/feces_metabolites.xml",
    "HMDB_DATA/hmdb_proteins.xml",
    "HMDB_DATA/hmdb_metabolites.xml",
]

# --------------- LOGGING ----------------
logging.basicConfig(
    filename="ingestion_run.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# --------------- INGESTION --------------
def main():
    try:
        start_total = time.time()
        print("🔌 Connecting to Neo4j...")
        conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, batch_size=BATCH_SIZE)
        print("✅ Connected to Neo4j.")

        if not SKIP_CLEAR:
            from reset import clear_neo4j_database
            print("🧹 Clearing database...")
            clear_neo4j_database(NEO4J_URI)
            logging.info("Database cleared.")
            print("✅ Database cleared.")

        print("📐 Creating indexes and constraints...")
        create_indexes_and_constraints(conn)
        print("✅ Constraints created.")


        num_processed = 0  # Initialize counter

        for file_path in XML_FILES:
            if not os.path.exists(file_path):
                print(f"❌ Skipping (not found): {file_path}")
                continue

            print(f"\n📦 Ingesting {file_path} ...")
            logging.info(f"==== Ingesting {file_path} ====")
            file_start = time.time()
            try:
                if "protein" in file_path.lower():
                    build_knowledge_graph_from_hmdb_proteins(conn, file_path)
                else:
                    build_knowledge_graph_from_hmdb(conn, file_path)

                num_processed += 1 # Increment counter    

                elapsed = time.time() - file_start
                print(f"✅ Finished {file_path} in {elapsed:.2f}s")
                logging.info(f"Finished {file_path} in {elapsed:.2f}s")
            except Exception as e:
                print(f"❌ Error processing {file_path}: {e}")
                traceback.print_exc()
                logging.error(f"Error processing {file_path}: {e}")

        print(f"\n✅ Total files processed: {num_processed}")  # Print total processed files

        conn.close()
        total_time = time.time() - start_total
        print(f"\n✅ All HMDB data ingested successfully in {total_time:.2f}s")
        logging.info(f"Total ingestion time: {total_time:.2f}s")

    except Exception as e:
        print("❌ Fatal error occurred.")
        traceback.print_exc()
        logging.error(f"Fatal error: {e}")

if __name__ == "__main__":
    main()
