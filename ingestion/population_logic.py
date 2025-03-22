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
        "CREATE CONSTRAINT IF NOT EXISTS FOR (pprop:ProteinProperty) REQUIRE pprop.propertyId IS UNIQUE"
    ]

    for command in constraint_commands:
        try:
            neo4j_connection.run_query(command)
        except Exception as e:
            print(f"Warning: Could not create constraint with query: {command}. Error: {str(e)}")

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

def parse_synonyms(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <synonyms> for a metabolite, creates Synonym nodes, and links them to the Metabolite.
    """
    synonyms_root = metabolite_element.find("synonyms")
    if synonyms_root is not None:
        for syn_el in synonyms_root.findall("synonym"):
            synonym_text = syn_el.text.strip() if syn_el.text else None
            if synonym_text:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Synonym",
                    primary_key="synonymText",
                    properties={"synonymText": synonym_text}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=accession_id,
                    relationship_type="HAS_SYNONYM",
                    object_node_id=synonym_text,
                    subject_label="Metabolite",
                    object_label="Synonym",
                    subject_key="accession",
                    object_key="synonymText"
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

def parse_general_references(metabolite_element: ET.Element, accession_id: str, neo4j_connection: Neo4jConnection):
    """
    Parses <general_references> for a metabolite, creating GeneralReference nodes.
    """
    general_refs_el = metabolite_element.find("general_references")
    if general_refs_el is None:
        return
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
def parse_full_metabolite(metabolite_element: ET.Element, neo4j_connection: Neo4jConnection):
    """
    Parses a single <metabolite> element and merges its data into the Neo4j knowledge graph.
    """
    accession_id = get_text(metabolite_element, "accession")
    if not accession_id:
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
            "state": state_val
        }
    )

    # Parse metabolite sub-sections
    parse_secondary_accessions(metabolite_element, accession_id, neo4j_connection)
    parse_synonyms(metabolite_element, accession_id, neo4j_connection)
    parse_taxonomy(metabolite_element, accession_id, neo4j_connection)
    parse_ontology(metabolite_element, accession_id, neo4j_connection)
    parse_experimental_properties(metabolite_element, accession_id, neo4j_connection)
    parse_predicted_properties(metabolite_element, accession_id, neo4j_connection)
    parse_spectra(metabolite_element, accession_id, neo4j_connection)
    parse_biological_properties(metabolite_element, accession_id, neo4j_connection)
    parse_normal_concentrations(metabolite_element, accession_id, neo4j_connection)
    parse_abnormal_concentrations(metabolite_element, accession_id, neo4j_connection)
    parse_diseases(metabolite_element, accession_id, neo4j_connection)
    parse_protein_associations(metabolite_element, accession_id, neo4j_connection)
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
    for metabolite_el in stream_parse_hmdb(hmdb_xml_file, target_tag="metabolite"):
        parse_full_metabolite_stream(metabolite_el, neo4j_connection)
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
    Parses <synonyms> from a protein element, creating Synonym nodes.
    """
    synonyms_el = protein_element.find("synonyms")
    if synonyms_el is not None:
        for syn_el in synonyms_el.findall("synonym"):
            synonym_text = syn_el.text.strip() if syn_el.text else None
            if synonym_text:
                create_or_merge_node(
                    neo4j_connection=neo4j_connection,
                    label="Synonym",
                    primary_key="synonymText",
                    properties={"synonymText": synonym_text}
                )
                create_or_merge_relationship(
                    neo4j_connection=neo4j_connection,
                    subject_node_id=protein_accession,
                    relationship_type="HAS_SYNONYM",
                    object_node_id=synonym_text,
                    subject_label="Protein",
                    object_label="Synonym",
                    subject_key="proteinAcc",
                    object_key="synonymText"
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
    print(f"Processing Protein: {protein_accession}")

    prot_already_exists = neo4j_connection.run_query(f'MATCH (p:Protein) WHERE p.proteinAcc ="{protein_accession}" RETURN p;')
    print(f"Protein Exists: {prot_already_exists}")
    if not protein_accession:
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

def build_knowledge_graph_from_hmdb_proteins(neo4j_connection: Neo4jConnection, proteins_xml_file: str) -> str:
    """
    Streams through the HMDB Proteins XML file and builds/extends the protein knowledge graph in Neo4j.
    """
    for protein_el in stream_parse_hmdb(proteins_xml_file, target_tag="protein"):
        parse_full_protein(protein_el, neo4j_connection)
    neo4j_connection.flush_queries()
    return "Protein Knowledge Graph Build Complete"
