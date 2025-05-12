import unittest
from utils.enrich_links import extract_ids_from_text, enrich_record_with_links

class TestEnrichLinks(unittest.TestCase):
    
    def test_extract_ids_from_text(self):
        # Test with single ID
        text1 = "This is a reference to PMID: 12345678 in text."
        self.assertEqual(extract_ids_from_text(text1), {"pubmed_id": "12345678"})
        
        # Test with multiple IDs
        text2 = "This paper (PMID: 87654321) discusses OMIM: 123456 and DrugBank DB12345."
        extracted = extract_ids_from_text(text2)
        self.assertEqual(extracted["pubmed_id"], "87654321")
        self.assertEqual(extracted["omim_id"], "123456")
        self.assertEqual(extracted["drugbank_id"], "DB12345")
        
        # Test with HMDB and CHEBI
        text3 = "Metabolite HMDB0001234 is related to CHEBI:12345."
        extracted = extract_ids_from_text(text3)
        self.assertEqual(extracted["hmdb_id"], "HMDB0001234")
        self.assertEqual(extracted["chebi_id"], "CHEBI:12345")
        
        # Test new ID types
        text4 = "Compound with ChemSpider: 12345, KEGG: C12345, and Wikipedia: Glucose."
        extracted = extract_ids_from_text(text4)
        self.assertEqual(extracted["chemspider_id"], "12345")
        self.assertEqual(extracted["kegg_id"], "C12345")
        self.assertEqual(extracted["wikipedia_id"], "Glucose")
        
        # Test more new ID types
        text5 = "Metabolite with BioCyc: META:COMPOUND-123, FoodB: FDB012345, VMH: VMH12345, and BiGG: atp_c."
        extracted = extract_ids_from_text(text5)
        self.assertEqual(extracted["biocyc_id"], "META:COMPOUND-123")
        self.assertEqual(extracted["foodb_id"], "FDB012345")
        self.assertEqual(extracted["vmh_id"], "VMH12345")
        self.assertEqual(extracted["bigg_id"], "atp_c")
        
        # Test no matches
        text6 = "This text has no IDs."
        self.assertEqual(extract_ids_from_text(text6), {})
        
        # Test with non-string input
        self.assertEqual(extract_ids_from_text(None), {})
        self.assertEqual(extract_ids_from_text(123), {})
    
    def test_enrich_record_with_links(self):
        # Test with explicit IDs in record
        record1 = {"pubmed_id": "12345678", "name": "Test"}
        enriched1 = enrich_record_with_links(record1)
        self.assertEqual(enriched1["pubmed_id_url"], "https://pubmed.ncbi.nlm.nih.gov/12345678")
        
        # Test with nested IDs in record
        record2 = {"p.smpdb_id": "SMP00001", "name": "Test Pathway"}
        enriched2 = enrich_record_with_links(record2)
        self.assertEqual(enriched2["smpdb_id_url"], "https://smpdb.ca/view/SMP00001")
        
        # Test with IDs in description fields
        record3 = {"description": "This study (PMID: 23456789) examines OMIM: 234567.", "name": "Test"}
        enriched3 = enrich_record_with_links(record3)
        self.assertEqual(enriched3["pubmed_id"], "23456789")
        self.assertEqual(enriched3["pubmed_id_url"], "https://pubmed.ncbi.nlm.nih.gov/23456789")
        self.assertEqual(enriched3["omim_id"], "234567")
        self.assertEqual(enriched3["omim_id_url"], "https://www.omim.org/entry/234567")
        
        # Test with IDs in both explicit fields and description
        record4 = {
            "pubmed_id": "12345678", 
            "reference_text": "Related to OMIM: 345678 and DrugBank DB54321."
        }
        enriched4 = enrich_record_with_links(record4)
        self.assertEqual(enriched4["pubmed_id_url"], "https://pubmed.ncbi.nlm.nih.gov/12345678")
        self.assertEqual(enriched4["omim_id"], "345678")
        self.assertEqual(enriched4["omim_id_url"], "https://www.omim.org/entry/345678")
        self.assertEqual(enriched4["drugbank_id"], "DB54321")
        self.assertEqual(enriched4["drugbank_id_url"], "https://go.drugbank.com/drugs/DB54321")
        
        # Test with new database cross-references
        record5 = {
            "cr.source": "PubChem",
            "cr.identifier": "123456",
            "name": "Test Compound"
        }
        enriched5 = enrich_record_with_links(record5)
        self.assertEqual(enriched5["pubchem_id"], "123456")
        self.assertEqual(enriched5["pubchem_id_url"], "https://pubchem.ncbi.nlm.nih.gov/compound/123456")
        
        # Test with ChEBI ID that needs prefix
        record6 = {
            "chebi_id": "12345",  # Missing "CHEBI:" prefix
            "name": "Test Compound"
        }
        enriched6 = enrich_record_with_links(record6)
        self.assertEqual(enriched6["chebi_id"], "CHEBI:12345")
        self.assertEqual(enriched6["chebi_id_url"], "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:12345")
        
        # Test with multiple cross-reference sources
        record7 = {
            "cr.source": "ChemSpider",
            "cr.identifier": "12345",
            "description": "This compound is also known in KEGG: C12345."
        }
        enriched7 = enrich_record_with_links(record7)
        self.assertEqual(enriched7["chemspider_id"], "12345")
        self.assertEqual(enriched7["chemspider_id_url"], "https://www.chemspider.com/Chemical-Structure.12345.html")
        self.assertEqual(enriched7["kegg_id"], "C12345")
        self.assertEqual(enriched7["kegg_id_url"], "https://www.genome.jp/dbget-bin/www_bget?C12345")

if __name__ == "__main__":
    unittest.main() 