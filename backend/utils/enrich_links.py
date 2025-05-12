from typing import List, Dict
import re

# Map any raw alias to our canonical ID field
RAW_TO_CANONICAL = {
    # cr.source / cr.identifier handling
    "cr.source": "cr_source",
    "cr.identifier": "crossref_identifier",
    # direct query aliases
    "p.pubchem_id": "pubchem_id",
    "m.pubchem_id": "pubchem_id",
    "pubchem_id": "pubchem_id",
    "p.drugbank_id": "drugbank_id",
    "drugbank_id": "drugbank_id",
    "d.omim_id": "omim_id",
    "omim_id": "omim_id",
    "chebi_id": "chebi_id",
    "p.chebi_id": "chebi_id",
    "kegg_id": "kegg_id",
    "p.kegg_id": "kegg_id",
    "m.kegg_id": "kegg_id",
    "d.kegg_id": "kegg_id",
    "gr.kegg_id": "kegg_id",
    "smpdb_id": "smpdb_id",
    "p.smpdb_id": "smpdb_id",
    "m.smpdb_id": "smpdb_id",
    "d.smpdb_id": "smpdb_id",
    "kegg_map_id": "kegg_map_id",
    "p.kegg_map_id": "kegg_map_id",
    "m.kegg_map_id": "kegg_map_id",
    "pubmed_id": "pubmed_id", 
    "p.pubmed_id": "pubmed_id",
    "m.pubmed_id": "pubmed_id",
    "d.pubmed_id": "pubmed_id",
    "gr.pubmed_id": "pubmed_id",
    "hmdb_id": "hmdb_id",
    "p.hmdb_id": "hmdb_id",
    "m.hmdb_id": "hmdb_id",
    "inchi_key": "inchi_key",
    "p.inchi_key": "inchi_key",
    "m.inchi_key": "inchi_key",
    "chemspider_id": "chemspider_id",
    "p.chemspider_id": "chemspider_id",
    "m.chemspider_id": "chemspider_id",
    "wikipedia_id": "wikipedia_id",
    "p.wikipedia_id": "wikipedia_id",
    "m.wikipedia_id": "wikipedia_id",
    "vmh_id": "vmh_id",
    "p.vmh_id": "vmh_id",
    "m.vmh_id": "vmh_id",
    "biocyc_id": "biocyc_id",
    "p.biocyc_id": "biocyc_id",
    "m.biocyc_id": "biocyc_id",
    "foodb_id": "foodb_id",
    "p.foodb_id": "foodb_id",
    "m.foodb_id": "foodb_id",
    "bigg_id": "bigg_id",
    "p.bigg_id": "bigg_id",
    "m.bigg_id": "bigg_id"
}

# Precompiled regex patterns for better performance
_ID_PATTERNS = {
    "pubmed_id": r"(?:PMID\s*[:#]?\s*)(\d{5,8})",
    "omim_id": r"(?:OMIM\s*[:#]?\s*)(\d{5,6})",
    "drugbank_id": r"(DB\d{5})",
    "chebi_id": r"(CHEBI:\d+)",
    "pubchem_id": r"(?:PubChem\s*[:#]?\s*)(\d+)",
    "hmdb_id": r"(HMDB\d{5,9})",
    "kegg_map_id": r"(map\d{5})",
    "kegg_id": r"(?:KEGG\s*[:#]?\s*)(C\d{5}|G\d{5}|D\d{5})",
    "smpdb_id": r"(SMP\d{5,7})",
    "inchi_key": r"([A-Z]{14}-[A-Z]{10}-[A-Z])",
    "chemspider_id": r"(?:ChemSpider\s*[:#]?\s*)(\d+)",
    "wikipedia_id": r"(?:Wikipedia\s*[:#]?\s*)([A-Za-z0-9_]+)",
    "vmh_id": r"(?:VMH\s*[:#]?\s*)([A-Za-z0-9_]+)",
    "biocyc_id": r"(?:BioCyc\s*[:#]?\s*)([A-Za-z0-9_:]+(?:-[A-Za-z0-9_]+)*)",
    "foodb_id": r"(?:FoodB\s*[:#]?\s*)([A-Za-z0-9_]+)",
    "bigg_id": r"(?:BiGG\s*[:#]?\s*)([A-Za-z0-9_]+)",
}

# Compile all patterns once at module load for better performance
_COMPILED_PATTERNS = {k: re.compile(pat, re.IGNORECASE) for k, pat in _ID_PATTERNS.items()}

# Define display templates for link text
LINK_TEXT_TEMPLATES = {
    "pubchem_id": "PubChem: {val}",
    "drugbank_id": "DrugBank: {val}",
    "omim_id": "OMIM: {val}",
    "chebi_id": "ChEBI: {val}",
    "kegg_id": "KEGG: {val}",
    "hmdb_id": "HMDB: {val}",
    "inchi_key": "InChIKey: {val}",
    "chemspider_id": "ChemSpider: {val}",
    "wikipedia_id": "Wikipedia: {val}",
    "vmh_id": "VMH: {val}",
    "biocyc_id": "BioCyc: {val}",
    "foodb_id": "FooDB: {val}",
    "bigg_id": "BiGG: {val}",
    "smpdb_id": "SMPDB: {val}",
    "pubmed_id": "PMID: {val}",
    "kegg_map_id": "KEGG Map: {val}",
}

# Centralized, scalable enrichment logic
def enrich_record_with_links(record: Dict) -> Dict:
    """
    Given a raw Cypher result row, generate external URLs for all known biomedical identifiers
    (e.g., smpdb_id, pubmed_id, omim_id) and attach them as *_url fields.

    Args:
        record: A dictionary representing one result row from Neo4j query.

    Returns:
        A new enriched dictionary with added *_url fields.
    """
    # 1) Shallow copy & normalize raw aliases
    normalized = {}
    for key, val in record.items():
        canonical = RAW_TO_CANONICAL.get(key, key)
        normalized[canonical] = val
    
    enriched = dict(normalized)  # Use normalized dictionary
    
    # Track which fields were enriched by which source (direct, crossref, or text)
    # This helps resolve conflicts and maintain precedence
    enrichment_sources = {}

    # Define mapping: ID field -> URL generation function
    ID_TO_URL = {
        "smpdb_id": lambda val: f"https://smpdb.ca/view/{val}",
        "kegg_map_id": lambda val: f"https://www.genome.jp/dbget-bin/show_pathway?{val}",
        "pubmed_id": lambda val: f"https://pubmed.ncbi.nlm.nih.gov/{val}",
        "omim_id": lambda val: f"https://www.omim.org/entry/{val}",
        "drugbank_id": lambda val: f"https://go.drugbank.com/drugs/{val}",
        "chebi_id": lambda val: f"https://www.ebi.ac.uk/chebi/searchId.do?chebiId={val}",
        "pubchem_id": lambda val: f"https://pubchem.ncbi.nlm.nih.gov/compound/{val}",
        "hmdb_id": lambda val: f"https://hmdb.ca/metabolites/{val}",
        "inchi_key": lambda val: f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/inchikey/{val}/JSON",
        # New URL templates
        "chemspider_id": lambda val: f"https://www.chemspider.com/Chemical-Structure.{val}.html",
        "wikipedia_id": lambda val: f"https://en.wikipedia.org/wiki/{val}",
        "vmh_id": lambda val: f"https://www.vmh.life/#metabolite/{val}",
        "biocyc_id": lambda val: f"https://biocyc.org/compound?orgid=META&id={val}",
        "foodb_id": lambda val: f"https://foodb.ca/compounds/{val}",
        "bigg_id": lambda val: f"http://bigg.ucsd.edu/universal/metabolites/{val}",
        # Regular KEGG ID (not just map)
        "kegg_id": lambda val: f"https://www.genome.jp/dbget-bin/www_bget?{val}",
    }

    # Source mapping for cross-references
    SOURCE_TO_ID_TYPE = {
        "PubChem": "pubchem_id",
        "DrugBank": "drugbank_id",
        "ChEBI": "chebi_id",
        "KEGG": "kegg_id",
        "OMIM": "omim_id",
        "ChemSpider": "chemspider_id",
        "Wikipedia": "wikipedia_id",
        "VMH": "vmh_id",
        "BioCyc": "biocyc_id",
        "FoodB": "foodb_id",
        "BiGG": "bigg_id",
        "HMDB": "hmdb_id",
    }

    # 1) Try all direct keys from the normalized dictionary
    for key, fn in ID_TO_URL.items():
        value = enriched.get(key)
        if value:
            enriched[f"{key}_url"] = fn(value)
            # Mark as directly extracted (highest priority)
            enrichment_sources[key] = "direct"
            enrichment_sources[f"{key}_url"] = "direct"

    # 2) Handle cross-references from cr_source and crossref_identifier (using normalized names)
    cr_source = enriched.get("cr_source")
    cr_identifier = enriched.get("crossref_identifier")
    
    if cr_source and cr_identifier and cr_source in SOURCE_TO_ID_TYPE:
        id_type = SOURCE_TO_ID_TYPE[cr_source]
        
        # Only add the ID if it doesn't already exist
        if id_type not in enriched:
            enriched[id_type] = cr_identifier
            enrichment_sources[id_type] = "crossref"
            
        # Add URL if field doesn't exist or if we just added the ID
        if f"{id_type}_url" not in enriched or enrichment_sources.get(id_type) == "crossref":
            enriched[f"{id_type}_url"] = ID_TO_URL[id_type](cr_identifier)
            enrichment_sources[f"{id_type}_url"] = "crossref"
    
    # Special handling for ChEBI IDs that need the "CHEBI:" prefix
    if "chebi_id" in enriched and not enriched["chebi_id"].startswith("CHEBI:"):
        # Check if it's a numeric ID that needs the prefix
        if enriched["chebi_id"].isdigit():
            enriched["chebi_id"] = f"CHEBI:{enriched['chebi_id']}"
            if "chebi_id_url" in enriched:
                enriched["chebi_id_url"] = ID_TO_URL["chebi_id"](enriched["chebi_id"])

    # 3) Extract IDs from description-like text fields (lowest priority)
    text_fields = ["description", "reference_text", "ontology_term", "text", "properties", "comment", "notes", "additional_info"]
    for text_field in text_fields:
        text = enriched.get(text_field)
        if text:
            extracted_ids = extract_ids_from_text(text)
            for id_type, id_values in extracted_ids.items():
                if id_type in ID_TO_URL and id_values:
                    # Only add the ID and URL if neither exists yet
                    # This prevents text extraction from overwriting higher-priority sources
                    if id_type not in enriched and f"{id_type}_url" not in enriched:
                        enriched[id_type] = id_values[0]
                        enriched[f"{id_type}_url"] = ID_TO_URL[id_type](id_values[0])
                        enrichment_sources[id_type] = "text"
                        enrichment_sources[f"{id_type}_url"] = "text"
                    
                    # Add all additional IDs as indexed fields only if they don't exist
                    # This preserves any existing indexed fields from higher-priority sources
                    for i, id_value in enumerate(id_values[1:], 1):
                        field_name = f"{id_type}_{i}"
                        url_field_name = f"{id_type}_{i}_url"
                        
                        if field_name not in enriched and url_field_name not in enriched:
                            enriched[field_name] = id_value
                            enriched[url_field_name] = ID_TO_URL[id_type](id_value)
                            enrichment_sources[field_name] = "text"
                            enrichment_sources[url_field_name] = "text"

    # Store the source information in the enriched record
    # Add a metadata field to track enrichment sources if needed
    enriched["_enrichment_sources"] = enrichment_sources

    return enriched


def extract_ids_from_text(text: str) -> Dict[str, List[str]]:
    """
    Extract known reference IDs from unstructured text (PubMed, OMIM, DrugBank, etc.)
    
    Args:
        text: A string that might contain biomedical identifiers
        
    Returns:
        Dictionary mapping ID types to lists of their values
    """
    id_matches = {}
    if not isinstance(text, str):
        return id_matches

    # Use pre-compiled patterns for better performance
    for field, compiled_pattern in _COMPILED_PATTERNS.items():
        matches = compiled_pattern.findall(text)
        if matches:
            # Store all matches as a list
            id_matches[field] = [match.strip() for match in matches]

    return id_matches


def enrich_records_with_links(records: List[Dict]) -> List[Dict]:
    """
    Batch enrichment of a list of Cypher result records.

    Args:
        records: List of Neo4j result dictionaries.

    Returns:
        List of enriched result dictionaries.
    """
    return [enrich_record_with_links(row) for row in records]

def get_link_text(id_field: str, id_val: str, record: Dict = None) -> str:
    """
    Get the display text for a link based on the ID field type and value.
    Optionally uses record data for more context-aware text.

    Args:
        id_field: The type of ID (e.g., "pubchem_id", "hmdb_id")
        id_val: The actual ID value
        record: Optional full record for context-aware text

    Returns:
        Formatted display text for the link
    """
    # First try to get a name from the record if available
    if record is not None:
        # Check for common name fields based on ID type
        name_fields = {
            "pubchem_id": ["compound_name", "name", "metabolite_name"],
            "hmdb_id": ["metabolite_name", "name"],
            "chebi_id": ["compound_name", "name"],
            "drugbank_id": ["drug_name", "name"],
            "kegg_id": ["compound_name", "name"],
        }
        
        if id_field in name_fields:
            for name_field in name_fields[id_field]:
                if name_field in record and record[name_field]:
                    # Use the name but append the ID in parentheses
                    return f"{record[name_field]} ({LINK_TEXT_TEMPLATES.get(id_field, '{val}').format(val=id_val)})"
    
    # Fallback to standard template if no name found or no record provided
    return LINK_TEXT_TEMPLATES.get(id_field, "{val}").format(val=id_val)

def inject_hyperlinks(answer: str, records: List[Dict]) -> str:
    """
    Replace each occurrence of a canonical ID in the answer text
    with a Markdown link using its corresponding *_url field.
    Uses customizable templates for link text to improve readability.
    """
    # List all (id_field, url_field) pairs based on your ID_TO_URL keys
    field_pairs = [
        ("pubchem_id",   "pubchem_id_url"),
        ("drugbank_id",  "drugbank_id_url"),
        ("omim_id",      "omim_id_url"),
        ("chebi_id",     "chebi_id_url"),
        ("kegg_id",      "kegg_id_url"),
        ("hmdb_id",      "hmdb_id_url"),
        ("inchi_key",    "inchi_key_url"),
        ("chemspider_id","chemspider_id_url"),
        ("wikipedia_id", "wikipedia_id_url"),
        ("vmh_id",       "vmh_id_url"),
        ("biocyc_id",    "biocyc_id_url"),
        ("foodb_id",     "foodb_id_url"),
        ("bigg_id",      "bigg_id_url"),
        ("smpdb_id",     "smpdb_id_url"),
        ("pubmed_id",    "pubmed_id_url"),
        ("kegg_map_id",  "kegg_map_id_url"),
    ]

    # First, handle all the primary ID fields
    for id_field, url_field in field_pairs:
        for rec in records:
            id_val = rec.get(id_field)
            url = rec.get(url_field)
            if id_val and url:
                # Only replace plain occurrences, not already linked text
                pattern = re.escape(str(id_val))
                # Get customized link text
                link_text = get_link_text(id_field, id_val, rec)
                answer = re.sub(
                    rf"(?<!\]\()({pattern})(?!\))",
                    rf"[{link_text}]({url})",
                    answer
                )
    
    # Now handle any indexed ID fields (for multiple occurrences)
    for id_field, _ in field_pairs:
        for rec in records:
            i = 1
            while f"{id_field}_{i}" in rec and f"{id_field}_{i}_url" in rec:
                id_val = rec.get(f"{id_field}_{i}")
                url = rec.get(f"{id_field}_{i}_url")
                if id_val and url:
                    pattern = re.escape(str(id_val))
                    # Get customized link text for indexed fields
                    link_text = get_link_text(id_field, id_val, rec)
                    answer = re.sub(
                        rf"(?<!\]\()({pattern})(?!\))",
                        rf"[{link_text}]({url})",
                        answer
                    )
                i += 1
                
    return answer
