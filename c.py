import xml.etree.ElementTree as ET
from collections import defaultdict
import os

def parse_xml_entities_relationships_iterative(xml_file_path: str) -> dict:
    entity_relationships = defaultdict(set)
    element_stack = []  # Stack to track parent elements

    # Using ET.iterparse for memory-efficient incremental parsing
    for event, element in ET.iterparse(xml_file_path, events=("start", "end")):
        if event == "start":
            # Remove namespace if present (format: {namespace}tag -> tag)
            if '}' in element.tag:
                element.tag = element.tag.split('}', 1)[1]
            # If there is a parent element, record the relationship (parent -> child)
            if element_stack:
                parent_element = element_stack[-1]
                entity_relationships[parent_element.tag].add(element.tag)
            element_stack.append(element)
        elif event == "end":
            # Finished processing this element; remove it from the stack and clear it to free memory
            element_stack.pop()
            element.clear()
    return entity_relationships

if __name__ == "__main__":
    # Directory containing the XML files
    xml_directory = "ingestion/HMDB_DATA"
    
    # List XML files in the directory, skipping those in the exclusion set
    xml_files = [f for f in os.listdir(xml_directory) if f.endswith(".xml") and f]
    
    for xml_filename in xml_files:
        xml_file_path = os.path.join(xml_directory, xml_filename)
        print(f"Analyzing entities and relationships in {xml_file_path}:")
        relationships = parse_xml_entities_relationships_iterative(xml_file_path)
        
        for parent_tag, child_tags in relationships.items():
            child_tags_str = ', '.join(child_tags)
            print(f"{parent_tag} can connect to: {child_tags_str}")
        
        print("\n")
