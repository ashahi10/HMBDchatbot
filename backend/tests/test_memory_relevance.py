import os
import sys
import uuid
import json
import time
from pathlib import Path

# Add the parent directory to sys.path so Python can find the modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.services.memory_service import MemoryService

def print_separator():
    print("\n" + "-" * 80 + "\n")

def main():
    # Create a temporary test directory for memory files
    test_dir = Path(__file__).parent / "test_memory"
    if not test_dir.exists():
        test_dir.mkdir(parents=True)
    
    # Initialize memory service with test directory
    memory_service = MemoryService(base_path=str(test_dir))
    
    # Create a session for testing
    session_id = str(uuid.uuid4())
    
    # Test memory storage and entity extraction
    print("Testing memory storage with automated entity extraction...\n")
    
    # Store some test conversations
    queries = [
        {
            "query": "What is the chemical formula for citric acid?",
            "answer": "The chemical formula for citric acid is C6H8O7. Citric acid is a weak organic acid that occurs naturally in citrus fruits.",
            "source": "neo4j"
        },
        {
            "query": "What is the InChIKey for citric acid?",
            "answer": "The InChIKey for citric acid is VQOGYQOJQEEOKU-UHFFFAOYSA-N. This is a unique identifier used in chemical databases.",
            "source": "neo4j"
        },
        {
            "query": "Tell me about HMDB0000094",
            "answer": "HMDB0000094 is the Human Metabolome Database identifier for citric acid, which is a key metabolite in the TCA cycle.",
            "source": "api"
        },
        {
            "query": "Give me information about D-Psicose",
            "answer": "D-Psicose (also called D-allulose) is a rare sugar with the molecular formula C6H12O6. It's an epimer of fructose found in small quantities in wheat, figs, and raisins.",
            "source": "api"
        },
        {
            "query": "What is the structure of D-Psicose?",
            "answer": "D-Psicose has a ketohexose structure similar to fructose but with a different orientation of the hydroxyl group at C-3.",
            "source": "neo4j"
        },
        {
            "query": "What's the InChIKey for D-Psicose?",
            "answer": "The InChIKey for D-Psicose is LKDRXBCSQODPBY-UHFFFAOYSA-N.",
            "source": "neo4j"
        }
    ]
    
    # Store all conversations
    print("Storing test conversations...")
    for i, item in enumerate(queries):
        memory_service.store(
            session_id=session_id,
            user_query=item["query"],
            answer=item["answer"],
            source=item["source"]
        )
        print(f"  Stored #{i+1}: {item['query']}")
    
    print_separator()
    
    # Test 1: Query about citric acid InChIKey
    test_query = "what was the inchikey for citric acid?"
    relevant_turns = memory_service.find_relevant(session_id, test_query)
    
    print(f"TEST 1: '{test_query}'")
    print("\nRelevant memories found (ranked by relevance):")
    for i, turn in enumerate(relevant_turns):
        print(f"\n[#{i+1}] Score: {turn.get('relevance_score', 0):.2f}")
        print(f"  Query: {turn.get('user_query')}")
        if 'score_components' in turn:
            print(f"  Score components: {json.dumps(turn.get('score_components'), indent=2)}")
        print(f"  Entity: {turn.get('entity', 'N/A')}")
    
    print_separator()
    
    # Test 2: Query about D-Psicose with entity mismatch
    test_query = "What is the molecular weight of D-Psicose?"
    relevant_turns = memory_service.find_relevant(session_id, test_query)
    
    print(f"TEST 2: '{test_query}'")
    print("\nRelevant memories found (ranked by relevance):")
    for i, turn in enumerate(relevant_turns):
        print(f"\n[#{i+1}] Score: {turn.get('relevance_score', 0):.2f}")
        print(f"  Query: {turn.get('user_query')}")
        if 'score_components' in turn:
            print(f"  Score components: {json.dumps(turn.get('score_components'), indent=2)}")
        print(f"  Entity: {turn.get('entity', 'N/A')}")
    
    print_separator()
    
    # Test 3: Ambiguous follow-up query (explicit entity)
    test_query = "What about its InChIKey?"
    relevant_turns = memory_service.find_relevant(session_id, test_query)
    
    print(f"TEST 3 (Ambiguous follow-up): '{test_query}'")
    print("\nRelevant memories found (ranked by relevance):")
    for i, turn in enumerate(relevant_turns):
        print(f"\n[#{i+1}] Score: {turn.get('relevance_score', 0):.2f}")
        print(f"  Query: {turn.get('user_query')}")
        if 'score_components' in turn:
            print(f"  Score components: {json.dumps(turn.get('score_components'), indent=2)}")
        print(f"  Entity: {turn.get('entity', 'N/A')}")
    
    print_separator()
    
    # Clean up
    memory_service.clear(session_id)
    
    # Try to remove the test directory
    try:
        for file in test_dir.glob("*.json"):
            file.unlink()
        test_dir.rmdir()
        print("Test directory cleaned up.")
    except:
        print("Could not completely clean up test directory.")

if __name__ == "__main__":
    main() 