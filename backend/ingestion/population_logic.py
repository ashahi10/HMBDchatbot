import re
import lxml.etree as ET
from neo4j_connection import Neo4jConnection

# Precompile the namespace removal pattern (if needed later)
NAMESPACE_PATTERN = re.compile(r'\sxmlns(:\w+)?="[^"]+"')

###########################################################################
# Utility Functions
###########################################################################

def stream_parse_hmdb(xml_file_path: str, target_tag: str):
    context = ET.iterparse(xml_file_path, events=("start", "end"))
    _, root = next(context)  # Get the root element for cleanup
    for event, elem in context:
        if event == "start":
            # Remove namespace from tag if present (e.g., "{namespace}metabolite" -> "metabolite")
            if '}' in elem.tag:
                elem.tag = elem.tag.split('}', 1)[1]
        if event == "end" and elem.tag == target_tag:
            yield elem
            root.clear()

def get_text(parent_element: ET.Element, tag_name: str) -> str:
    """
    Retrieves stripped text from a child element (tag_name) of parent_element.
    """
    if parent_element is None:
        return None
    child = parent_element.find(tag_name)
    if child is not None and child.text:
        return child.text.strip()
    return None

def protein_already_processed(neo4j_connection: Neo4jConnection, protein_accession: str) -> bool:
    query = "MATCH (p:Protein { proteinAcc: $acc }) RETURN p LIMIT 1"
    result = neo4j_connection.run_query(query, {"acc": protein_accession})
    # Depending on your Neo4jConnection implementation, result evaluation might vary.
    return bool(result)


def metabolite_already_processed(neo4j_connection: Neo4jConnection, accession: str) -> bool:
    query = "MATCH (m:Metabolite { accession: $acc }) RETURN m LIMIT 1"
    result = neo4j_connection.run_query(query, {"acc": accession})
    return bool(result)



###########################################################################
# Neo4j Creation Functions
###########################################################################
def create_indexes_and_constraints(neo4j_connection: Neo4jConnection):
    """
    Creates uniqueness constraints (indexes) for primary key properties of each node label.
    """
    constraint_commands = [
        # Metabolite and related nodes
        "CREATE CONSTRAINT IF NOT EXISTS FOR (m:Metabolite) REQUIRE m.accession IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sa:SecondaryAccession) REQUIRE sa.secAccValue IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Synonym) REQUIRE s.synonymText IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (si:SynonymIndex) REQUIRE si.canonical IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (o:OntologyTerm) REQUIRE o.termName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Taxonomy) REQUIRE t.taxonomyName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (ep:ExperimentalProperty) REQUIRE ep.expPropId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pp:PredictedProperty) REQUIRE pp.predPropId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sp:Spectrum) REQUIRE sp.spectrumNodeId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (c:CellularLocation) REQUIRE c.cellName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (bf:Biofluid) REQUIRE bf.biofluidName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (ts:Tissue) REQUIRE ts.tissueName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pw:Pathway) REQUIRE pw.pathwayNodeId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (nc:NormalConcentration) REQUIRE nc.concId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (ac:AbnormalConcentration) REQUIRE ac.abConcId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (cr:ConcentrationReference) REQUIRE cr.refId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Disease) REQUIRE d.diseaseName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (dr:DiseaseReference) REQUIRE dr.diseaseRefId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Protein) REQUIRE p.proteinAcc IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (gr:GeneralReference) REQUIRE gr.generalRefId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (crf:CrossReference) REQUIRE crf.crossRefId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sr:SynthesisReference) REQUIRE sr.synthesisRefId IS UNIQUE",
        # New constraints for protein-specific nodes
        "CREATE CONSTRAINT IF NOT EXISTS FOR (gp:GeneProperty) REQUIRE gp.genePropertyId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pf:Pfam) REQUIRE pf.pfamId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (tr:TransmembraneRegion) REQUIRE tr.regionId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sig:SignalRegion) REQUIRE sig.regionId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (mr:MetaboliteReference) REQUIRE mr.metaboliteRefId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (go:GOClass) REQUIRE go.goId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sc:SubcellularLocation) REQUIRE sc.locationName IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pdb:PdbID) REQUIRE pdb.pdbId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pprop:ProteinProperty) REQUIRE pprop.propertyId IS UNIQUE",
        # Functional Ontology constraints
        "CREATE CONSTRAINT IF NOT EXISTS FOR (fot:FunctionalOntologyTerm) REQUIRE fot.termId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (he:HealthEffect) REQUIRE he.effectId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (bl:BiologicalLocation) REQUIRE bl.locationId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (ds:DispositionSource) REQUIRE ds.sourceId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (ots:OntologyTermSynonym) REQUIRE ots.synonymId IS UNIQUE",
        # Enhanced Enzyme constraints
        "CREATE CONSTRAINT IF NOT EXISTS FOR (ep:EnhancedProtein) REQUIRE ep.hmdbp_id IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pf:ProteinFunction) REQUIRE pf.functionId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pd:ProteinDetail) REQUIRE pd.detailId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (gl:GeneLocation) REQUIRE gl.locationId IS UNIQUE",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pr:ProteinReaction) REQUIRE pr.reactionId IS UNIQUE",
        # Create indexes for the new alias relationships to improve query performance
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:IS_ALIAS_OF]-() ON (r)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_ALIAS]-() ON (r)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_SYNONYM_INDEX]-() ON (r)",
        # Create index for the is_secondary property to make filtering efficient
        "CREATE INDEX IF NOT EXISTS FOR (m:Metabolite) ON (m.is_secondary)",
        # Functional Ontology relationship indexes
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_HEALTH_EFFECT]-() ON (r)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_BIOLOGICAL_LOCATION]-() ON (r)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:HAS_DISPOSITION_SOURCE]-() ON (r)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:IS_CHILD_OF]-() ON (r)",
        "CREATE INDEX IF NOT EXISTS FOR ()-[r:IS_SYNONYM_OF]-() ON (r)",
        # Additional indexes for fast lookups on node properties
        "CREATE INDEX IF NOT EXISTS FOR (he:HealthEffect) ON (he.name)",
        "CREATE INDEX IF NOT EXISTS FOR (bl:BiologicalLocation) ON (bl.name)",
        "CREATE INDEX IF NOT EXISTS FOR (ds:DispositionSource) ON (ds.name)",
        "CREATE INDEX IF NOT EXISTS FOR (fot:FunctionalOntologyTerm) ON (fot.term)",
        "CREATE INDEX IF NOT EXISTS FOR (fot:FunctionalOntologyTerm) ON (fot.category)",
        # Enhanced Enzyme property indexes for fast lookups
        "CREATE INDEX IF NOT EXISTS FOR (ep:EnhancedProtein) ON (ep.gene_name)",
        "CREATE INDEX IF NOT EXISTS FOR (ep:EnhancedProtein) ON (ep.protein_type)",
        "CREATE INDEX IF NOT EXISTS FOR (ep:EnhancedProtein) ON (ep.uniprot_id)",
        "CREATE INDEX IF NOT EXISTS FOR (ep:EnhancedProtein) ON (ep.locus)",
        "CREATE INDEX IF NOT EXISTS FOR (pf:ProteinFunction) ON (pf.general_function)",
        "CREATE INDEX IF NOT EXISTS FOR (gl:GeneLocation) ON (gl.locus)"
    ]

    for command in constraint_commands:
        try:
            neo4j_connection.run_query(command)
        except Exception as e:
            print(f"Warning: Could not create constraint with query: {command}. Error: {str(e)}")

    # Create fulltext index for SynonymIndex search
    try:
        fulltext_index_query = """
        CALL db.index.fulltext.createIfNotExists(
            'synonym_comprehensive_search',
            ['SynonymIndex'],
            ['canonical', 'name', 'synonyms']
        )
        """
        neo4j_connection.run_query(fulltext_index_query)
    except Exception as e:
        print(f"Warning: Could not create fulltext index for SynonymIndex: {str(e)}")

def create_or_merge_node(
    neo4j_connection: Neo4jConnection,
    label: str,
    primary_key: str,
    properties: dict
):
    """
    Queues a MERGE operation to create or merge a node with the given label and properties.
    Uses 'primary_key' as the unique identifier property.
    """
    if primary_key not in properties:
        raise ValueError(f"Primary key '{primary_key}' is not in the provided properties.")
    pk_value = properties[primary_key]
    cypher_query = f"""
    MERGE (n:{label} {{ {primary_key}: $pk_value }})
    SET n += $props
    """
    params = {"pk_value": pk_value, "props": properties}
    neo4j_connection.add_query(cypher_query, params)

def create_or_merge_relationship(
    neo4j_connection: Neo4jConnection,
    subject_node_id: str,
    relationship_type: str,
    object_node_id: str,
    subject_label: str,
    object_label: str,
    subject_key: str,
    object_key: str,
    rel_properties: dict = None
):
    """
    Queues a MERGE operation for a relationship of type `relationship_type` between two nodes
    identified by (subject_label/subject_key) and (object_label/object_key).
    Optionally sets relationship properties.
    """
    if rel_properties is None:
        rel_properties = {}
    cypher_query = f"""
    MATCH (s:{subject_label} {{ {subject_key}: $subject_id }})
    MATCH (o:{object_label} {{ {object_key}: $object_id }})
    MERGE (s)-[r:{relationship_type}]->(o)
    SET r += $relProps
    """
    params = {"subject_id": subject_node_id, "object_id": object_node_id, "relProps": rel_properties}
    neo4j_connection.add_query(cypher_query, params)

###########################################################################
# HMDB Metabolite Parsing Functions
###########################################################################
def parse_cross_references(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses cross-reference fields from a metabolite element, creating CrossReference nodes
    and linking them to the Metabolite.
    """
    cross_ref_fields = {
        "foodb_id": "FoodB",
        "kegg_id": "KEGG",
        "chemspider_id": "ChemSpider",
        "chebi_id": "ChEBI",
        "pubchem_compound_id": "PubChem",
        "pdb_id": "PDB",
        "biocyc_id": "BioCyc",
        "drugbank_id": "DrugBank",
        "phenol_explorer_compound_id": "PhenolExplorer",
        "wikipedia_id": "Wikipedia",
        "knapsack_id": "Knapsack",
        "bigg_id": "BiGG",
        "metlin_id": "Metlin",
        "vmh_id": "VMH"
    }
    for field, source in cross_ref_fields.items():
        value = get_text(metabolite_element, field)
        if value:
            cross_ref_node_id = f"{accession_id}_{source}_{value}"
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="CrossReference",
                primary_key="crossRefId",
                properties={
                    "crossRefId": cross_ref_node_id,
                    "source": source,
                    "identifier": value
                }
            )
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=accession_id,
                relationship_type="HAS_CROSS_REFERENCE",
                object_node_id=cross_ref_node_id,
                subject_label="Metabolite",
                object_label="CrossReference",
                subject_key="accession",
                object_key="crossRefId"
            )

def parse_synthesis_reference(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses the synthesis_reference field from a metabolite element,
    creating a SynthesisReference node and linking it to the Metabolite.
    """
    synthesis_ref = get_text(metabolite_element, "synthesis_reference")
    if synthesis_ref:
        synthesis_ref_id = f"{accession_id}_synthesis"
        create_or_merge_node(
            neo4j_connection=neo4j_connection,
            label="SynthesisReference",
            primary_key="synthesisRefId",
            properties={
                "synthesisRefId": synthesis_ref_id,
                "reference_text": synthesis_ref
            }
        )
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=accession_id,
            relationship_type="HAS_SYNTHESIS_REFERENCE",
            object_node_id=synthesis_ref_id,
            subject_label="Metabolite",
            object_label="SynthesisReference",
            subject_key="accession",
            object_key="synthesisRefId"
        )

###########################################################################
# FUNCTIONAL ONTOLOGY PARSING FUNCTIONS
###########################################################################

def parse_functional_ontology_term_synonyms(neo4j_connection: Neo4jConnection, synonyms_element: ET.Element, term_id: str):
    """
    Parses synonyms for an ontology term and creates OntologyTermSynonym nodes.
    """
    if synonyms_element is not None:
        for syn_el in synonyms_element.findall("synonym"):
            synonym_text = syn_el.text.strip() if syn_el.text else None
            if synonym_text:
                synonym_id = f"{term_id}_syn_{hash(synonym_text) % 100000}"
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="OntologyTermSynonym",
                    primary_key="synonymId",
                    properties={
                        "synonymId": synonym_id,
                        "synonym_text": synonym_text,
                        "term_id": term_id
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=term_id,
                    relationship_type="IS_SYNONYM_OF",
                    object_node_id=synonym_id,
                    subject_label="FunctionalOntologyTerm",
                    object_label="OntologyTermSynonym",
                    subject_key="termId",
                    object_key="synonymId"
                )

def parse_functional_ontology_descendants(neo4j_connection: Neo4jConnection, descendants_element: ET.Element, metabolite_id: str, parent_term_id: str, category: str):
    """
    Recursively parses descendant terms in functional ontology.
    """
    if descendants_element is not None:
        for descendant_el in descendants_element.findall("descendant"):
            parse_functional_ontology_term(neo4j_connection, descendant_el, metabolite_id, parent_term_id, category)

def parse_functional_ontology_term(neo4j_connection: Neo4jConnection, term_element: ET.Element, metabolite_id: str, parent_term_id: str = None, category: str = ""):
    """
    Parses a single functional ontology term and creates appropriate nodes based on category.
    """
    term_name = get_text(term_element, "term")
    definition = get_text(term_element, "definition")
    level_text = get_text(term_element, "level")
    term_type = get_text(term_element, "type")
    parent_id = get_text(term_element, "parent_id")
    
    if not term_name:
        return
    
    # Create unique term ID
    term_id = f"{metabolite_id}_{category}_{hash(term_name) % 100000}"
    
    # Create FunctionalOntologyTerm node
    create_or_merge_node(
        neo4j_connection=neo4j_connection,
        label="FunctionalOntologyTerm",
        primary_key="termId",
        properties={
            "termId": term_id,
            "term": term_name,
            "definition": definition,
            "level": level_text,
            "type": term_type,
            "parent_id": parent_id,
            "category": category
        }
    )
    
    # Create specific typed nodes based on category and term type
    if category == "health_effect" and term_type == "child":
        create_health_effect_node(neo4j_connection, term_id, term_name, definition, term_element)
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=metabolite_id,
            relationship_type="HAS_HEALTH_EFFECT",
            object_node_id=f"{term_id}_health",
            subject_label="Metabolite",
            object_label="HealthEffect",
            subject_key="accession",
            object_key="effectId"
        )
    
    elif category == "biological_location" and term_type == "child":
        create_biological_location_node(neo4j_connection, term_id, term_name, definition, term_element)
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=metabolite_id,
            relationship_type="HAS_BIOLOGICAL_LOCATION",
            object_node_id=f"{term_id}_location",
            subject_label="Metabolite",
            object_label="BiologicalLocation",
            subject_key="accession",
            object_key="locationId"
        )
    
    elif category == "source" and term_type == "child":
        create_disposition_source_node(neo4j_connection, term_id, term_name, definition, term_element)
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=metabolite_id,
            relationship_type="HAS_DISPOSITION_SOURCE",
            object_node_id=f"{term_id}_source",
            subject_label="Metabolite",
            object_label="DispositionSource",
            subject_key="accession",
            object_key="sourceId"
        )
    
    # Connect to parent term if exists
    if parent_term_id:
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=term_id,
            relationship_type="IS_CHILD_OF",
            object_node_id=parent_term_id,
            subject_label="FunctionalOntologyTerm",
            object_label="FunctionalOntologyTerm",
            subject_key="termId",
            object_key="termId"
        )
    
    # Parse synonyms
    synonyms_element = term_element.find("synonyms")
    parse_functional_ontology_term_synonyms(neo4j_connection, synonyms_element, term_id)
    
    # Parse descendants recursively
    descendants_element = term_element.find("descendants")
    parse_functional_ontology_descendants(neo4j_connection, descendants_element, metabolite_id, term_id, category)

def create_health_effect_node(neo4j_connection: Neo4jConnection, term_id: str, term_name: str, definition: str, term_element: ET.Element):
    """
    Creates a specific HealthEffect node with enhanced properties.
    """
    effect_id = f"{term_id}_health"
    
    # Build category path for hierarchical context
    level = get_text(term_element, "level")
    category_path = build_category_path(term_element, term_name)
    
    # Collect synonyms
    synonyms_list = []
    synonyms_element = term_element.find("synonyms")
    if synonyms_element is not None:
        for syn_el in synonyms_element.findall("synonym"):
            synonym_text = syn_el.text.strip() if syn_el.text else None
            if synonym_text:
                synonyms_list.append(synonym_text)
    
    create_or_merge_node(
        neo4j_connection=neo4j_connection,
        label="HealthEffect",
        primary_key="effectId",
        properties={
            "effectId": effect_id,
            "name": term_name,
            "definition": definition,
            "category": category_path,
            "level": level,
            "synonyms": synonyms_list,
            "source_term_id": term_id
        }
    )

def create_biological_location_node(neo4j_connection: Neo4jConnection, term_id: str, term_name: str, definition: str, term_element: ET.Element):
    """
    Creates a specific BiologicalLocation node with enhanced properties.
    """
    location_id = f"{term_id}_location"
    
    # Build category path for hierarchical context
    level = get_text(term_element, "level")
    category_path = build_category_path(term_element, term_name)
    
    # Collect synonyms
    synonyms_list = []
    synonyms_element = term_element.find("synonyms")
    if synonyms_element is not None:
        for syn_el in synonyms_element.findall("synonym"):
            synonym_text = syn_el.text.strip() if syn_el.text else None
            if synonym_text:
                synonyms_list.append(synonym_text)
    
    create_or_merge_node(
        neo4j_connection=neo4j_connection,
        label="BiologicalLocation",
        primary_key="locationId",
        properties={
            "locationId": location_id,
            "name": term_name,
            "definition": definition,
            "category": category_path,
            "level": level,
            "synonyms": synonyms_list,
            "source_term_id": term_id
        }
    )

def create_disposition_source_node(neo4j_connection: Neo4jConnection, term_id: str, term_name: str, definition: str, term_element: ET.Element):
    """
    Creates a specific DispositionSource node with enhanced properties.
    """
    source_id = f"{term_id}_source"
    
    # Build category path for hierarchical context
    level = get_text(term_element, "level")
    category_path = build_category_path(term_element, term_name)
    
    # Collect synonyms
    synonyms_list = []
    synonyms_element = term_element.find("synonyms")
    if synonyms_element is not None:
        for syn_el in synonyms_element.findall("synonym"):
            synonym_text = syn_el.text.strip() if syn_el.text else None
            if synonym_text:
                synonyms_list.append(synonym_text)
    
    create_or_merge_node(
        neo4j_connection=neo4j_connection,
        label="DispositionSource",
        primary_key="sourceId",
        properties={
            "sourceId": source_id,
            "name": term_name,
            "definition": definition,
            "category": category_path,
            "level": level,
            "synonyms": synonyms_list,
            "source_term_id": term_id
        }
    )

def build_category_path(term_element: ET.Element, current_term: str):
    """
    Builds a hierarchical category path for better context.
    """
    # This is a simplified version - in a full implementation, 
    # you might want to traverse up the hierarchy to build full paths
    level = get_text(term_element, "level")
    parent_id = get_text(term_element, "parent_id")
    
    if level and int(level) > 1:
        return f"Level {level} > {current_term}"
    else:
        return current_term

def parse_functional_ontology(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Main function to parse functional ontology from metabolite XML.
    Processes both health effects and disposition information.
    """
    ontology_root = metabolite_element.find("ontology")
    if ontology_root is None:
        return
    
    for root_term in ontology_root.findall("root"):
        root_term_name = get_text(root_term, "term")
        
        if not root_term_name:
            continue
            
        # Determine category based on root term name
        if "physiological" in root_term_name.lower() or "health" in root_term_name.lower():
            category = "health_effect"
        elif "disposition" in root_term_name.lower():
            # Process disposition descendants to determine subcategory
            parse_disposition_subtree(neo4j_connection, root_term, accession_id)
            continue
        else:
            category = "general"
        
        # Parse the term tree
        parse_functional_ontology_term(neo4j_connection, root_term, accession_id, None, category)

def parse_disposition_subtree(neo4j_connection: Neo4jConnection, disposition_root: ET.Element, metabolite_id: str):
    """
    Specialized parser for disposition subtree which has Source and Biological location branches.
    """
    descendants_element = disposition_root.find("descendants")
    if descendants_element is not None:
        for descendant_el in descendants_element.findall("descendant"):
            descendant_term = get_text(descendant_el, "term")
            
            if "source" in descendant_term.lower():
                parse_functional_ontology_term(neo4j_connection, descendant_el, metabolite_id, None, "source")
            elif "biological location" in descendant_term.lower():
                parse_functional_ontology_term(neo4j_connection, descendant_el, metabolite_id, None, "biological_location")
            else:
                # Generic disposition parsing
                parse_functional_ontology_term(neo4j_connection, descendant_el, metabolite_id, None, "disposition")

###########################################################################
# EXISTING ONTOLOGY FUNCTIONS (UPDATED)
###########################################################################

def parse_ontology_subtree(neo4j_connection: Neo4jConnection, ontology_element: ET.Element, metabolite_id: str, parent_term_name: str = None):
    """
    Recursively parses an <ontology> element and creates OntologyTerm nodes.
    Links child terms to their parent and connects each term to the Metabolite.
    """
    term_name = get_text(ontology_element, "term")
    definition_text = get_text(ontology_element, "definition")
    level_text = get_text(ontology_element, "level")
    term_type = get_text(ontology_element, "type")

    if term_name:
        create_or_merge_node(
            neo4j_connection=neo4j_connection,
            label="OntologyTerm",
            primary_key="termName",
            properties={
                "termName": term_name,
                "definition": definition_text,
                "level": level_text,
                "term_type": term_type
            }
        )
        if parent_term_name is not None:
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=parent_term_name,
                relationship_type="HAS_CHILD_TERM",
                object_node_id=term_name,
                subject_label="OntologyTerm",
                object_label="OntologyTerm",
                subject_key="termName",
                object_key="termName"
            )
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=metabolite_id,
            relationship_type="HAS_ONTOLOGY_TERM",
            object_node_id=term_name,
            subject_label="Metabolite",
            object_label="OntologyTerm",
            subject_key="accession",
            object_key="termName"
        )

        descendants_element = ontology_element.find("descendants")
        if descendants_element is not None:
            for descendant_el in descendants_element.findall("descendant"):
                parse_ontology_subtree(
                    neo4j_connection=neo4j_connection,
                    ontology_element=descendant_el,
                    metabolite_id=metabolite_id,
                    parent_term_name=term_name
                )

def parse_ontology(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses the <ontology> element of a metabolite, processing each root term.
    """
    ontology_root = metabolite_element.find("ontology")
    if ontology_root is not None:
        for root_term in ontology_root.findall("root"):
            parse_ontology_subtree(neo4j_connection, root_term, metabolite_id=accession_id)

def parse_secondary_accessions(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <secondary_accessions> for a metabolite and creates SecondaryAccession nodes and relationships.
    """
    secondary_accessions_el = metabolite_element.find("secondary_accessions")
    if secondary_accessions_el is not None:
        for sec_acc_el in secondary_accessions_el.findall("accession"):
            secondary_value = sec_acc_el.text.strip() if sec_acc_el.text else None
            if secondary_value:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="SecondaryAccession",
                    primary_key="secAccValue",
                    properties={"secAccValue": secondary_value}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="HAS_SECONDARY_ACCESSION",
                    object_node_id=secondary_value,
                    subject_label="Metabolite",
                    object_label="SecondaryAccession",
                    subject_key="accession",
                    object_key="secAccValue"
                )

def parse_secondary_accessions_as_metabolite_nodes(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <secondary_accessions> for a metabolite and creates Metabolite nodes for each secondary accession.
    Links them to the primary Metabolite node using IS_ALIAS_OF and HAS_ALIAS relationships.
    Each secondary accession node has an is_secondary=true property to distinguish it from primary nodes.
    """
    secondary_accessions_el = metabolite_element.find("secondary_accessions")
    if secondary_accessions_el is not None:
        for sec_acc_el in secondary_accessions_el.findall("accession"):
            secondary_value = sec_acc_el.text.strip() if sec_acc_el.text else None
            if secondary_value and secondary_value != accession_id:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Metabolite",
                    primary_key="accession",
                    properties={
                        "accession": secondary_value,
                        "is_secondary": True,  # Flag to distinguish secondary accessions from primary
                        "primary_accession": accession_id  # Reference to the primary accession
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=secondary_value,
                    relationship_type="IS_ALIAS_OF",
                    object_node_id=accession_id,
                    subject_label="Metabolite",
                    object_label="Metabolite",
                    subject_key="accession",
                    object_key="accession"
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="HAS_ALIAS",
                    object_node_id=secondary_value,
                    subject_label="Metabolite",
                    object_label="Metabolite",
                    subject_key="accession",
                    object_key="accession"
                )

def parse_synonyms(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <synonyms> for a metabolite and creates a SynonymIndex node with array storage.
    This replaces the legacy approach of creating individual Synonym nodes and HAS_SYNONYM relationships.
    """
    synonyms_root = metabolite_element.find("synonyms")
    if synonyms_root is not None:
        # Collect all synonyms into a list with source attribution when available
        synonym_list = []
        synonym_sources = []
        
        for syn_el in synonyms_root.findall("synonym"):
            synonym_text = syn_el.text.strip() if syn_el.text is not None and syn_el.text and syn_el.text.strip() else None
            if synonym_text:
                synonym_list.append(synonym_text)
                # Try to get source from attributes (this might not exist in all XML versions)
                source = syn_el.get("source", "HMDB")  # Default to HMDB if no source
                synonym_sources.append(source)
        
        # Only create SynonymIndex if we have synonyms
        if synonym_list:
            # Get metabolite name for canonical reference
            metabolite_name_query = """
            MATCH (m:Metabolite {accession: $acc})
            RETURN m.name as name
            """
            name_result = neo4j_connection.run_query(metabolite_name_query, {"acc": accession_id})
            metabolite_name = name_result[0]['name'] if name_result else accession_id
            
            # Create SynonymIndex node with synonym array and sources
            synonym_index_id = f"{accession_id}_synonyms"
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="SynonymIndex",
                primary_key="canonical",
                properties={
                    "canonical": metabolite_name,
                    "name": metabolite_name,
                    "synonyms": synonym_list,
                    "synonym_sources": synonym_sources  # NEW: Source attribution
                }
            )
            
            # Create HAS_SYNONYM_INDEX relationship
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=accession_id,
                relationship_type="HAS_SYNONYM_INDEX",
                object_node_id=metabolite_name,
                subject_label="Metabolite",
                object_label="SynonymIndex",
                subject_key="accession",
                object_key="canonical"
            )

def parse_taxonomy(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses the <taxonomy> element of a metabolite, creates Taxonomy nodes for major fields,
    and links them to the Metabolite.
    """
    taxonomy_el = metabolite_element.find("taxonomy")
    if taxonomy_el is None:
        return

    taxonomy_description = get_text(taxonomy_el, "description")
    if taxonomy_description:
        update_query = """
        MATCH (m:Metabolite {accession:$acc})
        SET m.taxonomy_description = $tdesc
        """
        neo4j_connection.add_query(update_query, {"acc": accession_id, "tdesc": taxonomy_description})

    def create_taxonomy_node_and_link(field_value: str, level_name: str):
        if field_value:
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="Taxonomy",
                primary_key="taxonomyName",
                properties={"taxonomyName": field_value, "level": level_name}
            )
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=accession_id,
                relationship_type="HAS_TAXONOMY",
                object_node_id=field_value,
                subject_label="Metabolite",
                object_label="Taxonomy",
                subject_key="accession",
                object_key="taxonomyName"
            )

    create_taxonomy_node_and_link(get_text(taxonomy_el, "direct_parent"), "direct_parent")
    create_taxonomy_node_and_link(get_text(taxonomy_el, "kingdom"), "kingdom")
    create_taxonomy_node_and_link(get_text(taxonomy_el, "super_class"), "super_class")
    create_taxonomy_node_and_link(get_text(taxonomy_el, "class"), "class")
    create_taxonomy_node_and_link(get_text(taxonomy_el, "sub_class"), "sub_class")
    create_taxonomy_node_and_link(get_text(taxonomy_el, "molecular_framework"), "molecular_framework")

    alt_parents_el = taxonomy_el.find("alternative_parents")
    if alt_parents_el is not None:
        for alt_el in alt_parents_el.findall("alternative_parent"):
            alt_val = alt_el.text.strip() if alt_el.text else None
            create_taxonomy_node_and_link(alt_val, "alternative_parent")

    substituents_el = taxonomy_el.find("substituents")
    if substituents_el is not None:
        for subs_el in substituents_el.findall("substituent"):
            subs_val = subs_el.text.strip() if subs_el.text else None
            create_taxonomy_node_and_link(subs_val, "substituent")

    ext_descr_el = taxonomy_el.find("external_descriptors")
    if ext_descr_el is not None:
        for desc_el in ext_descr_el.findall("external_descriptor"):
            ext_val = desc_el.text.strip() if desc_el.text else None
            create_taxonomy_node_and_link(ext_val, "external_descriptor")

def parse_experimental_properties(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <experimental_properties> and creates ExperimentalProperty nodes.
    """
    exp_props_el = metabolite_element.find("experimental_properties")
    if exp_props_el is not None:
        for prop_el in exp_props_el.findall("property"):
            prop_kind = get_text(prop_el, "kind")
            prop_value = get_text(prop_el, "value")
            prop_source = get_text(prop_el, "source")
            if prop_kind or prop_value:
                unique_exp_id = f"{accession_id}_{prop_kind}_{prop_value}"
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="ExperimentalProperty",
                    primary_key="expPropId",
                    properties={
                        "expPropId": unique_exp_id,
                        "kind": prop_kind,
                        "value": prop_value,
                        "source": prop_source
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="HAS_EXPERIMENTAL_PROPERTY",
                    object_node_id=unique_exp_id,
                    subject_label="Metabolite",
                    object_label="ExperimentalProperty",
                    subject_key="accession",
                    object_key="expPropId"
                )

def parse_predicted_properties(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <predicted_properties> and creates PredictedProperty nodes.
    """
    pred_props_el = metabolite_element.find("predicted_properties")
    if pred_props_el is not None:
        for prop_el in pred_props_el.findall("property"):
            prop_kind = get_text(prop_el, "kind")
            prop_value = get_text(prop_el, "value")
            prop_source = get_text(prop_el, "source")
            if prop_kind or prop_value:
                unique_pred_id = f"{accession_id}_{prop_kind}_{prop_value}"
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="PredictedProperty",
                    primary_key="predPropId",
                    properties={
                        "predPropId": unique_pred_id,
                        "kind": prop_kind,
                        "value": prop_value,
                        "source": prop_source
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="HAS_PREDICTED_PROPERTY",
                    object_node_id=unique_pred_id,
                    subject_label="Metabolite",
                    object_label="PredictedProperty",
                    subject_key="accession",
                    object_key="predPropId"
                )

def parse_spectra(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <spectra> for a metabolite and creates Spectrum nodes.
    """
    spectra_el = metabolite_element.find("spectra")
    if spectra_el is not None:
        for spec_el in spectra_el.findall("spectrum"):
            spectrum_type = get_text(spec_el, "type")
            spectrum_id = get_text(spec_el, "spectrum_id")
            if spectrum_type or spectrum_id:
                unique_spec_id = f"{accession_id}_{spectrum_type}_{spectrum_id}"
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Spectrum",
                    primary_key="spectrumNodeId",
                    properties={
                        "spectrumNodeId": unique_spec_id,
                        "spectrum_type": spectrum_type,
                        "spectrum_id": spectrum_id
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="HAS_SPECTRUM",
                    object_node_id=unique_spec_id,
                    subject_label="Metabolite",
                    object_label="Spectrum",
                    subject_key="accession",
                    object_key="spectrumNodeId"
                )

def parse_biological_properties(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <biological_properties> of a metabolite: cellular_locations, biospecimen_locations,
    tissue_locations, and pathways.
    """
    bio_props_el = metabolite_element.find("biological_properties")
    if bio_props_el is None:
        return

    # Cellular locations
    cell_locs = bio_props_el.find("cellular_locations")
    if cell_locs is not None:
        for cell_el in cell_locs.findall("cellular"):
            cell_val = cell_el.text.strip() if cell_el.text else None
            if cell_val:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="CellularLocation",
                    primary_key="cellName",
                    properties={"cellName": cell_val}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="DETECTED_IN_CELLULAR_LOCATION",
                    object_node_id=cell_val,
                    subject_label="Metabolite",
                    object_label="CellularLocation",
                    subject_key="accession",
                    object_key="cellName"
                )

    # Biospecimen locations
    biospec_locs = bio_props_el.find("biospecimen_locations")
    if biospec_locs is not None:
        for bio_el in biospec_locs.findall("biospecimen"):
            bio_val = bio_el.text.strip() if bio_el.text else None
            if bio_val:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Biofluid",
                    primary_key="biofluidName",
                    properties={"biofluidName": bio_val}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="DETECTED_IN_BIOFLUID",
                    object_node_id=bio_val,
                    subject_label="Metabolite",
                    object_label="Biofluid",
                    subject_key="accession",
                    object_key="biofluidName"
                )

    # Tissue locations
    tissue_locs = bio_props_el.find("tissue_locations")
    if tissue_locs is not None:
        for tissue_el in tissue_locs.findall("tissue"):
            tissue_val = tissue_el.text.strip() if tissue_el.text else None
            if tissue_val:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Tissue",
                    primary_key="tissueName",
                    properties={"tissueName": tissue_val}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="DETECTED_IN_TISSUE",
                    object_node_id=tissue_val,
                    subject_label="Metabolite",
                    object_label="Tissue",
                    subject_key="accession",
                    object_key="tissueName"
                )

    # Pathways
    pathways_el = bio_props_el.find("pathways")
    if pathways_el is not None:
        for pw_el in pathways_el.findall("pathway"):
            pw_name = get_text(pw_el, "name")
            pw_smpdb = get_text(pw_el, "smpdb_id")
            pw_kegg = get_text(pw_el, "kegg_map_id")
            combined_pw_id = f"{pw_smpdb or ''}_{pw_kegg or ''}_{pw_name or ''}"
            if pw_name or pw_smpdb or pw_kegg:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Pathway",
                    primary_key="pathwayNodeId",
                    properties={
                        "pathwayNodeId": combined_pw_id,
                        "pathway_name": pw_name,
                        "smpdb_id": pw_smpdb,
                        "kegg_map_id": pw_kegg
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="INVOLVED_IN_PATHWAY",
                    object_node_id=combined_pw_id,
                    subject_label="Metabolite",
                    object_label="Pathway",
                    subject_key="accession",
                    object_key="pathwayNodeId"
                )

def parse_normal_concentrations(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <normal_concentrations> and creates NormalConcentration nodes linked to the Metabolite.
    """
    normal_concs_el = metabolite_element.find("normal_concentrations")
    if normal_concs_el is None:
        return
    for conc_el in normal_concs_el.findall("concentration"):
        biospec_val = get_text(conc_el, "biospecimen")
        concentration_value = get_text(conc_el, "concentration_value")
        concentration_units = get_text(conc_el, "concentration_units")
        subject_age = get_text(conc_el, "subject_age")
        subject_sex = get_text(conc_el, "subject_sex")
        subject_condition = get_text(conc_el, "subject_condition")
        comment_text = get_text(conc_el, "comment")
        unique_conc_id = f"{accession_id}_normal_{biospec_val}_{concentration_value}_{subject_age}_{subject_sex}_{subject_condition}"
        create_or_merge_node(
            neo4j_connection=neo4j_connection,
            label="NormalConcentration",
            primary_key="concId",
            properties={
                "concId": unique_conc_id,
                "biospecimen": biospec_val,
                "concentration_value": concentration_value,
                "concentration_units": concentration_units,
                "subject_age": subject_age,
                "subject_sex": subject_sex,
                "subject_condition": subject_condition,
                "comment": comment_text
            }
        )
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=accession_id,
            relationship_type="HAS_NORMAL_CONCENTRATION",
            object_node_id=unique_conc_id,
            subject_label="Metabolite",
            object_label="NormalConcentration",
            subject_key="accession",
            object_key="concId"
        )
        refs_block = conc_el.find("references")
        if refs_block is not None:
            for ref_el in refs_block.findall("reference"):
                ref_text_val = get_text(ref_el, "reference_text")
                ref_pubmed_val = get_text(ref_el, "pubmed_id")
                ref_node_id = f"normalRef_{accession_id}_{(ref_pubmed_val or '')}_{len(ref_text_val or '')}"
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="ConcentrationReference",
                    primary_key="refId",
                    properties={
                        "refId": ref_node_id,
                        "reference_text": ref_text_val,
                        "pubmed_id": ref_pubmed_val
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=unique_conc_id,
                    relationship_type="HAS_CONCENTRATION_REF",
                    object_node_id=ref_node_id,
                    subject_label="NormalConcentration",
                    object_label="ConcentrationReference",
                    subject_key="concId",
                    object_key="refId"
                )

def parse_abnormal_concentrations(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <abnormal_concentrations> and creates AbnormalConcentration nodes linked to the Metabolite.
    """
    abnormal_concs_el = metabolite_element.find("abnormal_concentrations")
    if abnormal_concs_el is None:
        return
    for conc_el in abnormal_concs_el.findall("concentration"):
        biospec_val = get_text(conc_el, "biospecimen")
        concentration_value = get_text(conc_el, "concentration_value")
        concentration_units = get_text(conc_el, "concentration_units")
        patient_age = get_text(conc_el, "patient_age")
        patient_sex = get_text(conc_el, "patient_sex")
        patient_info = get_text(conc_el, "patient_information")
        comment_text = get_text(conc_el, "comment")
        unique_abconc_id = f"{accession_id}_abnormal_{biospec_val}_{concentration_value}_{patient_age}_{patient_sex}"
        create_or_merge_node(
            neo4j_connection=neo4j_connection,
            label="AbnormalConcentration",
            primary_key="abConcId",
            properties={
                "abConcId": unique_abconc_id,
                "biospecimen": biospec_val,
                "concentration_value": concentration_value,
                "concentration_units": concentration_units,
                "patient_age": patient_age,
                "patient_sex": patient_sex,
                "patient_information": patient_info,
                "comment": comment_text
            }
        )
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=accession_id,
            relationship_type="HAS_ABNORMAL_CONCENTRATION",
            object_node_id=unique_abconc_id,
            subject_label="Metabolite",
            object_label="AbnormalConcentration",
            subject_key="accession",
            object_key="abConcId"
        )
        refs_block = conc_el.find("references")
        if refs_block is not None:
            for ref_el in refs_block.findall("reference"):
                ref_text_val = get_text(ref_el, "reference_text")
                ref_pubmed_val = get_text(ref_el, "pubmed_id")
                ref_node_id = f"abRef_{accession_id}_{(ref_pubmed_val or '')}_{len(ref_text_val or '')}"
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="ConcentrationReference",
                    primary_key="refId",
                    properties={
                        "refId": ref_node_id,
                        "reference_text": ref_text_val,
                        "pubmed_id": ref_pubmed_val
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=unique_abconc_id,
                    relationship_type="HAS_CONCENTRATION_REF",
                    object_node_id=ref_node_id,
                    subject_label="AbnormalConcentration",
                    object_label="ConcentrationReference",
                    subject_key="abConcId",
                    object_key="refId"
                )

def parse_diseases(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <diseases> for a metabolite, creating Disease nodes and linking them.
    """
    diseases_el = metabolite_element.find("diseases")
    if diseases_el is None:
        return
    for dis_el in diseases_el.findall("disease"):
        disease_name = get_text(dis_el, "name")
        omim_val = get_text(dis_el, "omim_id")
        if disease_name:
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="Disease",
                primary_key="diseaseName",
                properties={"diseaseName": disease_name, "omim_id": omim_val}
            )
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=accession_id,
                relationship_type="ASSOCIATED_WITH_DISEASE",
                object_node_id=disease_name,
                subject_label="Metabolite",
                object_label="Disease",
                subject_key="accession",
                object_key="diseaseName"
            )
            disease_refs = dis_el.find("references")
            if disease_refs is not None:
                for ref_el in disease_refs.findall("reference"):
                    ref_text_val = get_text(ref_el, "reference_text")
                    ref_pubmed_val = get_text(ref_el, "pubmed_id")
                    disease_ref_id = f"diseaseRef_{disease_name}_{(ref_pubmed_val or '')}_{len(ref_text_val or '')}"
                    create_or_merge_node(
                        neo4j_connection=neo4j_connection,
                        label="DiseaseReference",
                        primary_key="diseaseRefId",
                        properties={
                            "diseaseRefId": disease_ref_id,
                            "reference_text": ref_text_val,
                            "pubmed_id": ref_pubmed_val
                        }
                    )
                    create_or_merge_relationship(
                        neo4j_connection=neo4j_connection,
                        subject_node_id=disease_name,
                        relationship_type="HAS_DISEASE_REFERENCE",
                        object_node_id=disease_ref_id,
                        subject_label="Disease",
                        object_label="DiseaseReference",
                        subject_key="diseaseName",
                        object_key="diseaseRefId"
                    )

def parse_protein_associations(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <protein_associations> for a metabolite, creating Protein nodes and linking them.
    """
    protein_el = metabolite_element.find("protein_associations")
    if protein_el is None:
        return
    for prot_el in protein_el.findall("protein"):
        protein_accession = get_text(prot_el, "protein_accession")
        protein_name = get_text(prot_el, "name")
        uniprot_id = get_text(prot_el, "uniprot_id")
        gene_name = get_text(prot_el, "gene_name")
        protein_type = get_text(prot_el, "protein_type")
        if protein_accession:
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="Protein",
                primary_key="proteinAcc",
                properties={
                    "proteinAcc": protein_accession,
                    "protein_name": protein_name,
                    "uniprot_id": uniprot_id,
                    "gene_name": gene_name,
                    "protein_type": protein_type
                }
            )
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=accession_id,
                relationship_type="HAS_PROTEIN_ASSOCIATION",
                object_node_id=protein_accession,
                subject_label="Metabolite",
                object_label="Protein",
                subject_key="accession",
                object_key="proteinAcc"
            )

###########################################################################
# ENHANCED ENZYME PROCESSING FUNCTIONS
###########################################################################

def parse_enhanced_metabolite_protein_associations(metabolite_element: ET.Element, metabolite_id: str, neo4j_connection: Neo4jConnection):
    """Parse protein associations from metabolite XML with enhanced details."""
    protein_el = metabolite_element.find("protein_associations")
    if protein_el is None:
        return
        
    for prot_el in protein_el.findall("protein"):
        protein_accession = get_text(prot_el, "protein_accession")  # This is HMDBP ID
        protein_name = get_text(prot_el, "name")
        uniprot_id = get_text(prot_el, "uniprot_id")
        gene_name = get_text(prot_el, "gene_name")
        protein_type = get_text(prot_el, "protein_type")
        
        if protein_accession:
            # Create basic enhanced protein node (will be enriched by proteins XML processing)
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="EnhancedProtein",
                primary_key="hmdbp_id",
                properties={
                    "hmdbp_id": protein_accession,
                    "protein_accession": protein_accession,
                    "name": protein_name,
                    "uniprot_id": uniprot_id,
                    "gene_name": gene_name,
                    "protein_type": protein_type
                }
            )
            
            # Create enhanced association with relationship properties
            rel_properties = {
                "association_type": "metabolite_enzyme",
                "protein_name": protein_name,
                "gene_name": gene_name,
                "protein_type": protein_type
            }
            
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=metabolite_id,
                relationship_type="HAS_ENHANCED_ASSOCIATION",
                object_node_id=protein_accession,
                subject_label="Metabolite",
                object_label="EnhancedProtein",
                subject_key="accession",
                object_key="hmdbp_id",
                rel_properties=rel_properties
            )

def create_enhanced_protein_node(protein_element: ET.Element, neo4j_connection: Neo4jConnection):
    """Create an EnhancedProtein node with all available properties."""
    # Get HMDB protein ID (primary identifier)
    hmdbp_id = get_text(protein_element, "accession")  # This is the HMDBP ID
    if not hmdbp_id:
        return None
        
    # Basic protein information
    name = get_text(protein_element, "name")
    protein_type = get_text(protein_element, "protein_type")
    gene_name = get_text(protein_element, "gene_name")
    uniprot_id = get_text(protein_element, "uniprot_id")
    uniprot_name = get_text(protein_element, "uniprot_name")
    genbank_protein_id = get_text(protein_element, "genbank_protein_id")
    genbank_gene_id = get_text(protein_element, "genbank_gene_id")
    genecard_id = get_text(protein_element, "genecard_id")
    
    # Get locus from gene_properties
    locus = None
    gene_props_el = protein_element.find("gene_properties")
    if gene_props_el is not None:
        locus = get_text(gene_props_el, "locus")
    
    # Get protein properties
    molecular_weight = None
    theoretical_pi = None
    num_residues = None
    properties_el = protein_element.find("protein_properties")
    if properties_el is not None:
        molecular_weight = get_text(properties_el, "molecular_weight")
        theoretical_pi = get_text(properties_el, "theoretical_pi")
        num_residues = get_text(properties_el, "residue_number")
    
    # Get PDB IDs
    pdb_ids = []
    pdb_ids_el = protein_element.find("pdb_ids")
    if pdb_ids_el is not None:
        for pdb_el in pdb_ids_el.findall("pdb_id"):
            if pdb_el.text:
                pdb_ids.append(pdb_el.text.strip())
    
    # Create the EnhancedProtein node
    create_or_merge_node(
        neo4j_connection=neo4j_connection,
        label="EnhancedProtein",
        primary_key="hmdbp_id",
        properties={
            "hmdbp_id": hmdbp_id,
            "protein_accession": hmdbp_id,  # Same as hmdbp_id
            "name": name,
            "protein_type": protein_type,
            "gene_name": gene_name,
            "uniprot_id": uniprot_id,
            "uniprot_name": uniprot_name,
            "genbank_protein_id": genbank_protein_id,
            "genbank_gene_id": genbank_gene_id,
            "genecard_id": genecard_id,
            "locus": locus,
            "molecular_weight": molecular_weight,
            "theoretical_pi": theoretical_pi,
            "num_residues": num_residues,
            "pdb_ids": pdb_ids
        }
    )
    
    return hmdbp_id

def parse_protein_function(protein_element: ET.Element, hmdbp_id: str, neo4j_connection: Neo4jConnection):
    """Parse protein function information into ProteinFunction node."""
    general_function = get_text(protein_element, "general_function")
    specific_function = get_text(protein_element, "specific_function")
    protein_type = get_text(protein_element, "protein_type")
    
    if general_function or specific_function:
        function_id = f"{hmdbp_id}_function"
        create_or_merge_node(
            neo4j_connection=neo4j_connection,
            label="ProteinFunction",
            primary_key="functionId",
            properties={
                "functionId": function_id,
                "general_function": general_function,
                "specific_function": specific_function,
                "protein_type": protein_type,
                "hmdbp_id": hmdbp_id
            }
        )
        
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=hmdbp_id,
            relationship_type="HAS_PROTEIN_FUNCTION",
            object_node_id=function_id,
            subject_label="EnhancedProtein",
            object_label="ProteinFunction",
            subject_key="hmdbp_id",
            object_key="functionId"
        )

def parse_protein_details(protein_element: ET.Element, hmdbp_id: str, neo4j_connection: Neo4jConnection):
    """Parse detailed protein properties into ProteinDetail node."""
    properties_el = protein_element.find("protein_properties")
    if properties_el is not None:
        molecular_weight = get_text(properties_el, "molecular_weight")
        theoretical_pi = get_text(properties_el, "theoretical_pi")
        residue_number = get_text(properties_el, "residue_number")
        polypeptide_sequence = get_text(properties_el, "polypeptide_sequence")
        
        detail_id = f"{hmdbp_id}_details"
        create_or_merge_node(
            neo4j_connection=neo4j_connection,
            label="ProteinDetail",
            primary_key="detailId",
            properties={
                "detailId": detail_id,
                "molecular_weight": molecular_weight,
                "theoretical_pi": theoretical_pi,
                "residue_number": residue_number,
                "polypeptide_sequence": polypeptide_sequence,
                "hmdbp_id": hmdbp_id
            }
        )
        
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=hmdbp_id,
            relationship_type="HAS_PROTEIN_DETAIL",
            object_node_id=detail_id,
            subject_label="EnhancedProtein",
            object_label="ProteinDetail",
            subject_key="hmdbp_id",
            object_key="detailId"
        )

def parse_gene_location(protein_element: ET.Element, hmdbp_id: str, neo4j_connection: Neo4jConnection):
    """Parse gene location information into GeneLocation node."""
    gene_props_el = protein_element.find("gene_properties")
    if gene_props_el is not None:
        locus = get_text(gene_props_el, "locus")
        chromosome_location = get_text(gene_props_el, "chromosome_location")
        gene_sequence = get_text(gene_props_el, "gene_sequence")
        
        if locus:
            location_id = f"{hmdbp_id}_location"
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="GeneLocation",
                primary_key="locationId",
                properties={
                    "locationId": location_id,
                    "locus": locus,
                    "chromosome_location": chromosome_location,
                    "gene_sequence": gene_sequence,
                    "hmdbp_id": hmdbp_id
                }
            )
            
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=hmdbp_id,
                relationship_type="LOCATED_AT_LOCUS",
                object_node_id=location_id,
                subject_label="EnhancedProtein",
                object_label="GeneLocation",
                subject_key="hmdbp_id",
                object_key="locationId"
            )

def parse_protein_reactions(protein_element: ET.Element, hmdbp_id: str, neo4j_connection: Neo4jConnection):
    """Parse protein reactions (if available in XML)."""
    protein_type = get_text(protein_element, "protein_type")
    name = get_text(protein_element, "name")
    
    if protein_type and protein_type.lower() == "enzyme":
        reaction_id = f"{hmdbp_id}_reaction"
        # Create a basic reaction node based on available information
        create_or_merge_node(
            neo4j_connection=neo4j_connection,
            label="ProteinReaction",
            primary_key="reactionId",
            properties={
                "reactionId": reaction_id,
                "reaction_type": "enzymatic",
                "enzyme_name": name,
                "hmdbp_id": hmdbp_id
            }
        )
        
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=hmdbp_id,
            relationship_type="CATALYZES_REACTION",
            object_node_id=reaction_id,
            subject_label="EnhancedProtein",
            object_label="ProteinReaction",
            subject_key="hmdbp_id",
            object_key="reactionId"
        )

def process_protein_for_enhancement(protein_element: ET.Element, neo4j_connection: Neo4jConnection):
    """Process a single protein element for enhancement."""
    hmdbp_id = create_enhanced_protein_node(protein_element, neo4j_connection)
    if not hmdbp_id:
        return
        
    # Parse detailed protein information
    parse_protein_function(protein_element, hmdbp_id, neo4j_connection)
    parse_protein_details(protein_element, hmdbp_id, neo4j_connection)
    parse_gene_location(protein_element, hmdbp_id, neo4j_connection)
    parse_protein_reactions(protein_element, hmdbp_id, neo4j_connection)

def parse_general_references(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <general_references> for a metabolite, creating GeneralReference nodes.
    """
    general_refs_el = metabolite_element.find("general_references")
    if general_refs_el is not None:
        for ref_el in general_refs_el.findall("reference"):
            ref_text_val = get_text(ref_el, "reference_text")
            ref_pubmed_val = get_text(ref_el, "pubmed_id")
            gen_ref_id = f"genRef_{accession_id}_{(ref_pubmed_val or '')}_{len(ref_text_val or '')}"
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="GeneralReference",
                primary_key="generalRefId",
                properties={
                    "generalRefId": gen_ref_id,
                    "reference_text": ref_text_val,
                    "pubmed_id": ref_pubmed_val
                }
            )
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=accession_id,
                relationship_type="HAS_GENERAL_REFERENCE",
                object_node_id=gen_ref_id,
                subject_label="Metabolite",
                object_label="GeneralReference",
                subject_key="accession",
                object_key="generalRefId"
            )

###########################################################################
# MAIN METABOLITE PARSER
###########################################################################
def parse_metabolite_description(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Creates a Description node for a metabolite and links it with HAS_DESCRIPTION relationship.
    The Description node contains the full description text and maintains both accession and element ID linkage.
    Uses improved logic to prevent duplicates and ensure data integrity.
    """
    description = get_text(metabolite_element, "description")
    if description:
        # Create unique ID for the Description node using accession
        description_node_id = f"{accession_id}_desc"
        
        # Use a single atomic operation to create both node and relationship
        atomic_query = """
        MATCH (m:Metabolite {accession: $acc})
        WHERE m.description IS NOT NULL
        AND NOT EXISTS((m)-[:HAS_DESCRIPTION]->(:Description))
        CREATE (d:Description {
            descriptionId: $desc_id,
            text: $desc,
            metabolite_id: $acc,
            metabolite_element_id: elementId(m)
        })
        CREATE (m)-[:HAS_DESCRIPTION]->(d)
        RETURN 1 as created
        """
        neo4j_connection.add_query(atomic_query, {
            "acc": accession_id,
            "desc_id": description_node_id,
            "desc": description
        })

def parse_full_metabolite(metabolite_element: ET.Element, neo4j_connection: Neo4jConnection):
    """
    Parses a single <metabolite> element and merges its data into the Neo4j knowledge graph.
    """
    accession_id = get_text(metabolite_element, "accession")
    if not accession_id:
        return
    if metabolite_already_processed(neo4j_connection, accession_id):
        print(f"⏩ Skipping metabolite {accession_id} (already processed).")
        return    

    # Basic fields for Metabolite
    version = get_text(metabolite_element, "version")
    creation_date = get_text(metabolite_element, "creation_date")
    update_date = get_text(metabolite_element, "update_date")
    status = get_text(metabolite_element, "status")
    name = get_text(metabolite_element, "name")
    description = get_text(metabolite_element, "description")
    chemical_formula = get_text(metabolite_element, "chemical_formula")
    average_molecular_weight = get_text(metabolite_element, "average_molecular_weight")
    monoisotopic_molecular_weight = get_text(metabolite_element, "monisotopic_molecular_weight")
    iupac_name = get_text(metabolite_element, "iupac_name")
    traditional_iupac = get_text(metabolite_element, "traditional_iupac")
    cas_number = get_text(metabolite_element, "cas_registry_number")
    smiles = get_text(metabolite_element, "smiles")
    inchi = get_text(metabolite_element, "inchi")
    inchikey = get_text(metabolite_element, "inchikey")
    state_val = get_text(metabolite_element, "state")

    # Create or merge Metabolite node
    create_or_merge_node(
        neo4j_connection=neo4j_connection,
        label="Metabolite",
        primary_key="accession",
        properties={
            "accession": accession_id,
            "version": version,
            "creation_date": creation_date,
            "update_date": update_date,
            "status": status,
            "name": name,
            "description": description,
            "chemical_formula": chemical_formula,
            "average_molecular_weight": average_molecular_weight,
            "monoisotopic_molecular_weight": monoisotopic_molecular_weight,
            "iupac_name": iupac_name,
            "traditional_iupac": traditional_iupac,
            "cas_registry_number": cas_number,
            "smiles": smiles,
            "inchi": inchi,
            "inchikey": inchikey,
            "state": state_val,
            "is_secondary": False  # Explicitly mark this as a primary node
        }
    )

    # Create Description node and relationship
    parse_metabolite_description(metabolite_element, accession_id, neo4j_connection)

    # Parse metabolite sub-sections
    parse_secondary_accessions(metabolite_element, accession_id, neo4j_connection)
    parse_secondary_accessions_as_metabolite_nodes(metabolite_element, accession_id, neo4j_connection)
    
    parse_synonyms(metabolite_element, accession_id, neo4j_connection)
    parse_taxonomy(metabolite_element, accession_id, neo4j_connection)
    parse_ontology(metabolite_element, accession_id, neo4j_connection)
    parse_functional_ontology(metabolite_element, accession_id, neo4j_connection)  # NEW: Functional ontology parsing
    parse_experimental_properties(metabolite_element, accession_id, neo4j_connection)
    parse_predicted_properties(metabolite_element, accession_id, neo4j_connection)
    parse_spectra(metabolite_element, accession_id, neo4j_connection)
    parse_biological_properties(metabolite_element, accession_id, neo4j_connection)
    parse_normal_concentrations(metabolite_element, accession_id, neo4j_connection)
    parse_abnormal_concentrations(metabolite_element, accession_id, neo4j_connection)
    parse_diseases(metabolite_element, accession_id, neo4j_connection)
    parse_protein_associations(metabolite_element, accession_id, neo4j_connection)
    parse_enhanced_metabolite_protein_associations(metabolite_element, accession_id, neo4j_connection)  # NEW: Enhanced enzyme associations
    parse_general_references(metabolite_element, accession_id, neo4j_connection)
    parse_cross_references(metabolite_element, accession_id, neo4j_connection)
    parse_synthesis_reference(metabolite_element, accession_id, neo4j_connection)

# For streaming, we alias the above function.
def parse_full_metabolite_stream(metabolite_element: ET.Element, neo4j_connection: Neo4jConnection):
    parse_full_metabolite(metabolite_element, neo4j_connection)

def build_knowledge_graph_from_hmdb(neo4j_connection: Neo4jConnection, hmdb_xml_file: str) -> str:
    """
    Streams through the HMDB XML file (metabolite data) and builds the knowledge graph in Neo4j.
    """
    count = 0
    for metabolite_el in stream_parse_hmdb(hmdb_xml_file, target_tag="metabolite"):
        parse_full_metabolite_stream(metabolite_el, neo4j_connection)
        count += 1
        if count % 1000 == 0:
            print(f"🔄 Flushing batch at {count} metabolites...")
            neo4j_connection.flush_queries()
            

    neo4j_connection.flush_queries()
    return "Knowledge Graph Build Complete for Metabolites"


###########################################################################
# HMDB PROTEINS FILE PARSING FUNCTIONS
###########################################################################
def parse_secondary_accessions_protein(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <secondary_accessions> from a protein element and creates SecondaryAccession nodes.
    """
    secondary_accessions_el = protein_element.find("secondary_accessions")
    if secondary_accessions_el is not None:
        for sec_acc_el in secondary_accessions_el.findall("accession"):
            secondary_value = sec_acc_el.text.strip() if sec_acc_el.text else None
            if secondary_value:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="SecondaryAccession",
                    primary_key="secAccValue",
                    properties={"secAccValue": secondary_value}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="HAS_SECONDARY_ACCESSION",
                    object_node_id=secondary_value,
                    subject_label="Protein",
                    object_label="SecondaryAccession",
                    subject_key="proteinAcc",
                    object_key="secAccValue"
                )

def parse_metabolite_associations(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <metabolite_associations> from a protein element.
    Each associated metabolite (with minimal info such as name and accession) is created as a MetaboliteAssociation node.
    """
    associations_el = protein_element.find("metabolite_associations")
    if associations_el is not None:
        for assoc_el in associations_el.findall("metabolite"):
            assoc_accession = get_text(assoc_el, "accession")
            assoc_name = get_text(assoc_el, "name")
            if assoc_accession:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="MetaboliteAssociation",
                    primary_key="accession",
                    properties={"accession": assoc_accession, "name": assoc_name}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="ASSOCIATED_WITH_METABOLITE",
                    object_node_id=assoc_accession,
                    subject_label="Protein",
                    object_label="MetaboliteAssociation",
                    subject_key="proteinAcc",
                    object_key="accession"
                )

def parse_gene_properties(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <gene_properties> from a protein element and creates a GeneProperty node.
    """
    gene_props_el = protein_element.find("gene_properties")
    if gene_props_el is not None:
        locus = get_text(gene_props_el, "locus")
        chromosome_location = get_text(gene_props_el, "chromosome_location")
        gene_sequence = get_text(gene_props_el, "gene_sequence")
        gene_property_id = f"{protein_accession}_geneProp"
        create_or_merge_node(
            neo4j_connection=neo4j_connection,
            label="GeneProperty",
            primary_key="genePropertyId",
            properties={
                "genePropertyId": gene_property_id,
                "locus": locus,
                "chromosome_location": chromosome_location,
                "gene_sequence": gene_sequence
            }
        )
        create_or_merge_relationship(
            neo4j_connection=neo4j_connection,
            subject_node_id=protein_accession,
            relationship_type="HAS_GENE_PROPERTY",
            object_node_id=gene_property_id,
            subject_label="Protein",
            object_label="GeneProperty",
            subject_key="proteinAcc",
            object_key="genePropertyId"
        )

def parse_general_references_protein(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <general_references> from a protein element, creating GeneralReference nodes.
    """
    general_refs_el = protein_element.find("general_references")
    if general_refs_el is not None:
        for ref_el in general_refs_el.findall("reference"):
            ref_text = get_text(ref_el, "reference_text")
            ref_pubmed = get_text(ref_el, "pubmed_id")
            gen_ref_id = f"genProtRef_{protein_accession}_{(ref_pubmed or '')}_{len(ref_text or '')}"
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="GeneralReference",
                primary_key="generalRefId",
                properties={
                    "generalRefId": gen_ref_id,
                    "reference_text": ref_text,
                    "pubmed_id": ref_pubmed
                }
            )
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=protein_accession,
                relationship_type="HAS_GENERAL_REFERENCE",
                object_node_id=gen_ref_id,
                subject_label="Protein",
                object_label="GeneralReference",
                subject_key="proteinAcc",
                object_key="generalRefId"
            )

def parse_synonyms_protein(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <synonyms> from a protein element and creates a SynonymIndex node with array storage.
    This replaces the legacy approach of creating individual Synonym nodes and HAS_SYNONYM relationships.
    """
    synonyms_el = protein_element.find("synonyms")
    if synonyms_el is not None:
        # Collect all synonyms into a list
        synonym_list = []
        for syn_el in synonyms_el.findall("synonym"):
            synonym_text = syn_el.text.strip() if syn_el.text else None
            if synonym_text:
                synonym_list.append(synonym_text)
        
        # Only create SynonymIndex if we have synonyms
        if synonym_list:
            # Get protein name for canonical reference
            protein_name_query = """
            MATCH (p:Protein {proteinAcc: $acc})
            RETURN p.name as name
            """
            name_result = neo4j_connection.run_query(protein_name_query, {"acc": protein_accession})
            protein_name = name_result[0]['name'] if name_result else protein_accession
            
            # Create SynonymIndex node with synonym array
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="SynonymIndex",
                primary_key="canonical",
                properties={
                    "canonical": protein_name,
                    "name": protein_name,
                    "synonyms": synonym_list
                }
            )
            
            # Create HAS_SYNONYM_INDEX relationship
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=protein_accession,
                relationship_type="HAS_SYNONYM_INDEX",
                object_node_id=protein_name,
                subject_label="Protein",
                object_label="SynonymIndex",
                subject_key="proteinAcc",
                object_key="canonical"
            )

def parse_pathways_protein(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <pathways> from a protein element and creates Pathway nodes.
    """
    pathways_el = protein_element.find("pathways")
    if pathways_el is not None:
        for pw_el in pathways_el.findall("pathway"):
            pw_name = get_text(pw_el, "name")
            pw_smpdb = get_text(pw_el, "smpdb_id")
            pw_kegg = get_text(pw_el, "kegg_map_id")
            pathway_id = f"{pw_smpdb or ''}_{pw_kegg or ''}_{pw_name or ''}"
            if pw_name or pw_smpdb or pw_kegg:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Pathway",
                    primary_key="pathwayNodeId",
                    properties={
                        "pathwayNodeId": pathway_id,
                        "pathway_name": pw_name,
                        "smpdb_id": pw_smpdb,
                        "kegg_map_id": pw_kegg
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="INVOLVED_IN_PATHWAY",
                    object_node_id=pathway_id,
                    subject_label="Protein",
                    object_label="Pathway",
                    subject_key="proteinAcc",
                    object_key="pathwayNodeId"
                )

def parse_protein_properties(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <protein_properties> from a protein element, including molecular_weight,
    pfams, transmembrane_regions, polypeptide_sequence, theoretical_pi, residue_number,
    and signal_regions.
    """
    properties_el = protein_element.find("protein_properties")
    if properties_el is None:
        return

    molecular_weight = get_text(properties_el, "molecular_weight")
    polypeptide_sequence = get_text(properties_el, "polypeptide_sequence")
    theoretical_pi = get_text(properties_el, "theoretical_pi")
    residue_number = get_text(properties_el, "residue_number")
    protein_prop_id = f"{protein_accession}_prop"
    create_or_merge_node(
        neo4j_connection=neo4j_connection,
        label="ProteinProperty",
        primary_key="propertyId",
        properties={
            "propertyId": protein_prop_id,
            "molecular_weight": molecular_weight,
            "polypeptide_sequence": polypeptide_sequence,
            "theoretical_pi": theoretical_pi,
            "residue_number": residue_number
        }
    )
    create_or_merge_relationship(
        neo4j_connection=neo4j_connection,
        subject_node_id=protein_accession,
        relationship_type="HAS_PROTEIN_PROPERTY",
        object_node_id=protein_prop_id,
        subject_label="Protein",
        object_label="ProteinProperty",
        subject_key="proteinAcc",
        object_key="propertyId"
    )

    # Parse pfams
    pfams_el = properties_el.find("pfams")
    if pfams_el is not None:
        for pfam_el in pfams_el.findall("pfam"):
            pfam_name = get_text(pfam_el, "name")
            pfam_id = get_text(pfam_el, "pfam_id")
            if pfam_id:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Pfam",
                    primary_key="pfamId",
                    properties={"pfamId": pfam_id, "name": pfam_name}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="HAS_PFAM",
                    object_node_id=pfam_id,
                    subject_label="Protein",
                    object_label="Pfam",
                    subject_key="proteinAcc",
                    object_key="pfamId"
                )

    # Parse transmembrane_regions
    tm_regions_el = properties_el.find("transmembrane_regions")
    if tm_regions_el is not None:
        for idx, region_el in enumerate(tm_regions_el.findall("region"), start=1):
            region_text = region_el.text.strip() if region_el.text else None
            region_id = f"{protein_accession}_tm_{idx}"
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="TransmembraneRegion",
                primary_key="regionId",
                properties={"regionId": region_id, "description": region_text}
            )
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=protein_accession,
                relationship_type="HAS_TRANSMEMBRANE_REGION",
                object_node_id=region_id,
                subject_label="Protein",
                object_label="TransmembraneRegion",
                subject_key="proteinAcc",
                object_key="regionId"
            )

    # Parse signal_regions
    sig_regions_el = properties_el.find("signal_regions")
    if sig_regions_el is not None:
        for idx, region_el in enumerate(sig_regions_el.findall("region"), start=1):
            region_text = region_el.text.strip() if region_el.text else None
            region_id = f"{protein_accession}_sig_{idx}"
            create_or_merge_node(
                neo4j_connection=neo4j_connection,
                label="SignalRegion",
                primary_key="regionId",
                properties={"regionId": region_id, "description": region_text}
            )
            create_or_merge_relationship(
                neo4j_connection=neo4j_connection,
                subject_node_id=protein_accession,
                relationship_type="HAS_SIGNAL_REGION",
                object_node_id=region_id,
                subject_label="Protein",
                object_label="SignalRegion",
                subject_key="proteinAcc",
                object_key="regionId"
            )

def parse_metabolite_references(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <metabolite_references> from a protein element and creates MetaboliteReference nodes.
    """
    met_ref_el = protein_element.find("metabolite_references")
    if met_ref_el is not None:
        for ref_el in met_ref_el.findall("metabolite_reference"):
            assoc_metabolite = ref_el.find("metabolite")
            ref_info = ref_el.find("reference")
            if assoc_metabolite is not None and ref_info is not None:
                assoc_accession = get_text(assoc_metabolite, "accession")
                assoc_name = get_text(assoc_metabolite, "name")
                ref_pubmed = get_text(ref_info, "pubmed_id")
                ref_text = get_text(ref_info, "reference_text")
                met_ref_id = f"{protein_accession}_{assoc_accession}_{(ref_pubmed or '')}"
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="MetaboliteReference",
                    primary_key="metaboliteRefId",
                    properties={
                        "metaboliteRefId": met_ref_id,
                        "metabolite_accession": assoc_accession,
                        "metabolite_name": assoc_name,
                        "pubmed_id": ref_pubmed,
                        "reference_text": ref_text
                    }
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="HAS_METABOLITE_REFERENCE",
                    object_node_id=met_ref_id,
                    subject_label="Protein",
                    object_label="MetaboliteReference",
                    subject_key="proteinAcc",
                    object_key="metaboliteRefId"
                )

def parse_go_classifications(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <go_classifications> from a protein element and creates GOClass nodes.
    """
    go_class_el = protein_element.find("go_classifications")
    if go_class_el is not None:
        for go_el in go_class_el.findall("go_class"):
            category = get_text(go_el, "category")
            go_id = get_text(go_el, "go_id")
            description = get_text(go_el, "description")
            if go_id:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="GOClass",
                    primary_key="goId",
                    properties={"goId": go_id, "category": category, "description": description}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="HAS_GO_CLASSIFICATION",
                    object_node_id=go_id,
                    subject_label="Protein",
                    object_label="GOClass",
                    subject_key="proteinAcc",
                    object_key="goId"
                )

def parse_subcellular_locations(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <subcellular_locations> from a protein element and creates SubcellularLocation nodes.
    """
    subcell_el = protein_element.find("subcellular_locations")
    if subcell_el is not None:
        for loc_el in subcell_el.findall("subcellular_location"):
            location_name = loc_el.text.strip() if loc_el.text else None
            if location_name:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="SubcellularLocation",
                    primary_key="locationName",
                    properties={"locationName": location_name}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="LOCATED_IN_SUBCELLULAR_LOCATION",
                    object_node_id=location_name,
                    subject_label="Protein",
                    object_label="SubcellularLocation",
                    subject_key="proteinAcc",
                    object_key="locationName"
                )

def parse_pdb_ids(protein_element: ET.Element, protein_accession: str, neo4j_connection: Neo4jConnection):
    """
    Parses <pdb_ids> from a protein element and creates PdbID nodes.
    """
    pdb_ids_el = protein_element.find("pdb_ids")
    if pdb_ids_el is not None:
        for pdb_el in pdb_ids_el.findall("pdb_id"):
            pdb_id_value = pdb_el.text.strip() if pdb_el.text else None
            if pdb_id_value:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="PdbID",
                    primary_key="pdbId",
                    properties={"pdbId": pdb_id_value}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="HAS_PDB_ID",
                    object_node_id=pdb_id_value,
                    subject_label="Protein",
                    object_label="PdbID",
                    subject_key="proteinAcc",
                    object_key="pdbId"
                )

def parse_full_protein(protein_element: ET.Element, neo4j_connection: Neo4jConnection):
    """
    Parses a single <protein> element from the HMDB Proteins file and merges its data into the Neo4j knowledge graph.
    """
    protein_accession = get_text(protein_element, "accession")
    if not protein_accession:
        return
    
    if protein_already_processed(neo4j_connection, protein_accession):
        print(f"Skipping protein {protein_accession} as it is already processed.")
        return

    # Basic protein fields
    specific_function = get_text(protein_element, "specific_function")
    uniprot_name = get_text(protein_element, "uniprot_name")
    creation_date = get_text(protein_element, "creation_date")
    update_date = get_text(protein_element, "update_date")
    uniprot_id = get_text(protein_element, "uniprot_id")
    protein_type = get_text(protein_element, "protein_type")
    genbank_protein_id = get_text(protein_element, "genbank_protein_id")
    general_function = get_text(protein_element, "general_function")
    geneatlas_id = get_text(protein_element, "geneatlas_id")
    protein_name = get_text(protein_element, "name")
    version = get_text(protein_element, "version")
    genecard_id = get_text(protein_element, "genecard_id")
    hgnc_id = get_text(protein_element, "hgnc_id")
    genbank_gene_id = get_text(protein_element, "genbank_gene_id")
    gene_name = get_text(protein_element, "gene_name")

    # Create or merge the Protein node
    create_or_merge_node(
        neo4j_connection=neo4j_connection,
        label="Protein",
        primary_key="proteinAcc",
        properties={
            "proteinAcc": protein_accession,
            "specific_function": specific_function,
            "uniprot_name": uniprot_name,
            "creation_date": creation_date,
            "update_date": update_date,
            "uniprot_id": uniprot_id,
            "protein_type": protein_type,
            "genbank_protein_id": genbank_protein_id,
            "general_function": general_function,
            "geneatlas_id": geneatlas_id,
            "name": protein_name,
            "version": version,
            "genecard_id": genecard_id,
            "hgnc_id": hgnc_id,
            "genbank_gene_id": genbank_gene_id,
            "gene_name": gene_name
        }
    )

    # Parse protein sub-sections
    parse_secondary_accessions_protein(protein_element, protein_accession, neo4j_connection)
    parse_metabolite_associations(protein_element, protein_accession, neo4j_connection)
    parse_gene_properties(protein_element, protein_accession, neo4j_connection)
    parse_general_references_protein(protein_element, protein_accession, neo4j_connection)
    parse_synonyms_protein(protein_element, protein_accession, neo4j_connection)
    parse_pathways_protein(protein_element, protein_accession, neo4j_connection)
    parse_protein_properties(protein_element, protein_accession, neo4j_connection)
    parse_metabolite_references(protein_element, protein_accession, neo4j_connection)
    parse_go_classifications(protein_element, protein_accession, neo4j_connection)
    parse_subcellular_locations(protein_element, protein_accession, neo4j_connection)
    parse_pdb_ids(protein_element, protein_accession, neo4j_connection)
    
    # NEW: Enhanced protein processing
    process_protein_for_enhancement(protein_element, neo4j_connection)

def build_knowledge_graph_from_hmdb_proteins(neo4j_connection: Neo4jConnection, proteins_xml_file: str) -> str:
    """
    Streams through the HMDB Proteins XML file and builds/extends the protein knowledge graph in Neo4j.
    """
    for protein_el in stream_parse_hmdb(proteins_xml_file, target_tag="protein"):
        parse_full_protein(protein_el, neo4j_connection)
    neo4j_connection.flush_queries()
    return "Protein Knowledge Graph Build Complete"
