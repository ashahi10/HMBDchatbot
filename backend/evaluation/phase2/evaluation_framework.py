"""
Phase 2: Evaluation Framework
Comprehensive test set construction for automated chatbot evaluation

This module defines evaluation categories, complexity levels, and core data structures
for building a diverse, representative benchmark question-answer set.


"""

from enum import Enum
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
import json
from datetime import datetime

class EvaluationCategory(Enum):
    """Question categories for comprehensive evaluation coverage"""
    ENTITY_FACTOID = "entity_factoid"              # Simple property queries
    ENTITY_DESCRIPTION = "entity_description"      # Descriptive information
    PROPERTY_EXTRACTION = "property_extraction"    # List-based properties
    ONE_HOP_RELATIONSHIP = "one_hop_relationship"  # Direct relationships
    TWO_HOP_RELATIONSHIP = "two_hop_relationship"  # Nested relationships
    EXTERNAL_REFERENCE = "external_reference"      # Cross-reference queries
    SPECIMEN_SPECIFIC = "specimen_specific"        # Concentration/specimen data
    CROSS_LINKAGE = "cross_linkage"               # Multi-database linkage
    AGGREGATION = "aggregation"                    # Count/statistical queries
    COMPARISON = "comparison"                      # Multi-entity comparisons

class ComplexityLevel(Enum):
    """Complexity levels for graded evaluation"""
    LEVEL_1 = 1  # Simple fact/property (1 field, no relations)
    LEVEL_2 = 2  # Single-hop relationship (entity → linked entity)
    LEVEL_3 = 3  # Multi-hop/nested relationship (entity → linked → property)
    LEVEL_4 = 4  # Aggregation/comparison (multiple entities/relationships)
    LEVEL_5 = 5  # Open-ended/natural language (LLM evaluation)

class EntityType(Enum):
    """Primary entity types in the knowledge base"""
    METABOLITE = "metabolite"
    PROTEIN = "protein"
    RELATIONSHIP = "relationship"

class AnswerType(Enum):
    """Expected answer format types"""
    STRING = "string"           # Single text value
    NUMBER = "number"           # Numeric value
    LIST = "list"              # List of values
    BOOLEAN = "boolean"         # True/False
    DICT = "dict"              # Structured object
    CONCENTRATION = "concentration"  # Specimen concentration data

@dataclass
class QuestionTemplate:
    """Template for generating questions"""
    template: str
    category: EvaluationCategory
    complexity: ComplexityLevel
    answer_type: AnswerType
    required_fields: List[str]
    optional_fields: List[str] = field(default_factory=list)
    min_field_count: int = 1
    entity_type: EntityType = EntityType.METABOLITE

@dataclass
class GeneratedQuestion:
    """Complete question-answer pair with metadata"""
    question: str
    answer: Any
    entity_id: str
    entity_type: EntityType
    category: EvaluationCategory
    complexity: ComplexityLevel
    answer_type: AnswerType
    source_type: str  # "direct", "scripted", "llm_generated"
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for export"""
        return {
            "question": self.question,
            "answer": self.answer,
            "entity_id": self.entity_id,
            "entity_type": self.entity_type.value,
            "category": self.category.value,
            "complexity": self.complexity.value,
            "answer_type": self.answer_type.value,
            "source_type": self.source_type,
            "confidence": self.confidence,
            "metadata": self.metadata
        }

class QuestionTemplateLibrary:
    """Comprehensive library of question templates"""
    
    def __init__(self):
        self.templates = self._build_template_library()
    
    def _build_template_library(self) -> List[QuestionTemplate]:
        """Build comprehensive template library with diverse question types"""
        templates = []
        
        # Level 1-2: Basic Metabolite Questions with IMPROVED DIVERSITY
        templates.extend([
            # Entity Factoid - More diverse templates
            QuestionTemplate(
                template="What is the chemical name of {name}?",
                category=EvaluationCategory.ENTITY_FACTOID,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Identify the molecular weight of {name}.",
                category=EvaluationCategory.ENTITY_FACTOID,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.NUMBER,
                required_fields=["average_molecular_weight", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="What molecular formula does {name} have?",
                category=EvaluationCategory.ENTITY_FACTOID,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["chemical_formula", "name"],
                entity_type=EntityType.METABOLITE
            ),
            
            # Property Extraction - Enhanced variety
            QuestionTemplate(
                template="What chemical class does {name} belong to?",
                category=EvaluationCategory.PROPERTY_EXTRACTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["taxonomy_class", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Which chemical family is {name} classified under?",
                category=EvaluationCategory.PROPERTY_EXTRACTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["taxonomy_class", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="What is the SMILES notation for {name}?",
                category=EvaluationCategory.PROPERTY_EXTRACTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["smiles", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Provide the structural representation (SMILES) of {name}.",
                category=EvaluationCategory.PROPERTY_EXTRACTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["smiles", "name"],
                entity_type=EntityType.METABOLITE
            ),
        
            # Entity Description - More natural variations
            QuestionTemplate(
                template="Describe the chemical properties and classification of {name}.",
                category=EvaluationCategory.ENTITY_DESCRIPTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["taxonomy_class", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Provide a biochemical description of {name}.",
                category=EvaluationCategory.ENTITY_DESCRIPTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["taxonomy_class", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="What are the key molecular characteristics of {name}?",
                category=EvaluationCategory.ENTITY_DESCRIPTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["taxonomy_class", "name"],
                entity_type=EntityType.METABOLITE
            ),
            
            # Aggregation - Multiple question styles including zero-count questions
            QuestionTemplate(
                template="How many protein associations are documented for {name}?",
                category=EvaluationCategory.AGGREGATION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.NUMBER,
                required_fields=["protein_associations_count", "name"],
                min_field_count=0,  # Accept entities with 0 associations
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Count the number of protein interactions known for {name}.",
                category=EvaluationCategory.AGGREGATION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.NUMBER,
                required_fields=["protein_associations_count", "name"],
                min_field_count=0,  # Accept entities with 0 associations
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Does {name} have any documented protein interactions?",
                category=EvaluationCategory.AGGREGATION,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["protein_associations_count", "name"],
                min_field_count=0,  # Accept any count including 0
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Are there any protein associations recorded for {name}?",
                category=EvaluationCategory.AGGREGATION,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["protein_associations_count", "name"],
                min_field_count=0,  # Accept any count including 0
                entity_type=EntityType.METABOLITE
            ),
            
            # One-hop relationships - Diverse phrasing
            QuestionTemplate(
                template="List all proteins associated with {name}.",
                category=EvaluationCategory.ONE_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.LIST,
                required_fields=["protein_associations_count", "name"],
                min_field_count=1,  # Must have at least 1 protein
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Which proteins interact with {name}?",
                category=EvaluationCategory.ONE_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.LIST,
                required_fields=["protein_associations_count", "name"],
                min_field_count=1,  # Must have at least 1 protein
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Name the proteins that have associations with {name}.",
                category=EvaluationCategory.ONE_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.LIST,
                required_fields=["protein_associations_count", "name"],
                min_field_count=1,  # Must have at least 1 protein
                entity_type=EntityType.METABOLITE
            ),
            
            # External Reference - Enhanced variety
            QuestionTemplate(
                template="What is the PubChem identifier for {name}?",
                category=EvaluationCategory.EXTERNAL_REFERENCE,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["pubchem_compound_id", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Provide the ChEBI ID for {name}.",
                category=EvaluationCategory.EXTERNAL_REFERENCE,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["chebi_id", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="What external database identifiers are available for {name}?",
                category=EvaluationCategory.EXTERNAL_REFERENCE,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["pubchem_compound_id", "name"],
                entity_type=EntityType.METABOLITE
            ),
            
            # Cross-linkage - Multi-database linkage questions
            QuestionTemplate(
                template="How is {name} cross-referenced between PubChem, ChEBI, and KEGG databases?",
                category=EvaluationCategory.CROSS_LINKAGE,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["pubchem_compound_id", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="List all external database cross-references for {name}.",
                category=EvaluationCategory.CROSS_LINKAGE,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.DICT,
                required_fields=["pubchem_compound_id", "name"],
                entity_type=EntityType.METABOLITE
            ),
            
            # Specimen-specific - More biological context
            QuestionTemplate(
                template="In which biological specimens can {name} be measured?",
                category=EvaluationCategory.SPECIMEN_SPECIFIC,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["normal_concentrations_count", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="What concentration data exists for {name} in biological samples?",
                category=EvaluationCategory.SPECIMEN_SPECIFIC,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["normal_concentrations_count", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="How many concentration measurements are documented for {name}?",
                category=EvaluationCategory.SPECIMEN_SPECIFIC,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["normal_concentrations_count", "name"],
                entity_type=EntityType.METABOLITE
            ),
            
            # Comparison - Enhanced diversity
            QuestionTemplate(
                template="How does {name} compare to other metabolites in its chemical class?",
                category=EvaluationCategory.COMPARISON,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["taxonomy_class", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="What distinguishes {name} from similar metabolites?",
                category=EvaluationCategory.COMPARISON,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["taxonomy_class", "name"],
                entity_type=EntityType.METABOLITE
            ),
            QuestionTemplate(
                template="Compare the protein associations of {name} with other metabolites.",
                category=EvaluationCategory.COMPARISON,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["protein_associations_count", "name"],
                entity_type=EntityType.METABOLITE
            )
        ])
        
        # PROTEIN TEMPLATES - ENHANCED for better coverage and diversity
        
        # Level 1-2: Basic Protein Questions with IMPROVED DIVERSITY
        templates.extend([
            # Entity Factoid - More diverse templates
            QuestionTemplate(
                template="What is the gene name for protein {protein_accession}?",
                category=EvaluationCategory.ENTITY_FACTOID,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["gene_name", "protein_accession"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Identify the gene symbol associated with {gene_name}.",
                category=EvaluationCategory.ENTITY_FACTOID,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="What is the UniProt identifier for {gene_name}?",
                category=EvaluationCategory.ENTITY_FACTOID,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["uniprot_id", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
        
            # Property Extraction - Enhanced variety
            QuestionTemplate(
                template="What is the general function of protein {gene_name}?",
                category=EvaluationCategory.PROPERTY_EXTRACTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["general_function", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Describe the specific function of {gene_name}.",
                category=EvaluationCategory.PROPERTY_EXTRACTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["specific_function", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="What functional role does {gene_name} play in metabolism?",
                category=EvaluationCategory.PROPERTY_EXTRACTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["general_function", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            
            # Entity Description - More natural variations
            QuestionTemplate(
                template="Provide a functional description of protein {gene_name}.",
                category=EvaluationCategory.ENTITY_DESCRIPTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["general_function", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Explain the biological significance of {gene_name} in metabolic processes.",
                category=EvaluationCategory.ENTITY_DESCRIPTION,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["general_function", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="What are the key characteristics of protein {gene_name}?",
                category=EvaluationCategory.ENTITY_DESCRIPTION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["general_function", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            
            # Aggregation - Multiple question styles including zero-count questions
            QuestionTemplate(
                template="How many metabolites are associated with protein {gene_name}?",
                category=EvaluationCategory.AGGREGATION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.NUMBER,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=0,  # Accept entities with 0 associations
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Count the metabolite interactions for {gene_name}.",
                category=EvaluationCategory.AGGREGATION,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.NUMBER,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=0,  # Accept entities with 0 associations
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Does protein {gene_name} have any metabolite associations?",
                category=EvaluationCategory.AGGREGATION,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=0,  # Accept any count including 0
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Are there any documented metabolite interactions for {gene_name}?",
                category=EvaluationCategory.AGGREGATION,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=0,  # Accept any count including 0
                entity_type=EntityType.PROTEIN
            ),
            
            # One-hop relationships - Diverse phrasing
            QuestionTemplate(
                template="List all metabolites associated with protein {gene_name}.",
                category=EvaluationCategory.ONE_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.LIST,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=1,  # Must have metabolite associations
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Which metabolites interact with {gene_name}?",
                category=EvaluationCategory.ONE_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.LIST,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=1,  # Must have metabolite associations
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Name the metabolites that have associations with protein {gene_name}.",
                category=EvaluationCategory.ONE_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.LIST,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=1,  # Must have metabolite associations
                entity_type=EntityType.PROTEIN
            ),
            
            # Two-hop relationships - Enhanced
            QuestionTemplate(
                template="List all proteins that share metabolites with {gene_name}.",
                category=EvaluationCategory.TWO_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.LIST,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=2,  # Must have multiple associations for sharing
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Which proteins have overlapping metabolite interactions with {gene_name}?",
                category=EvaluationCategory.TWO_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.LIST,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=2,  # Must have multiple associations for sharing
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Identify proteins that share common metabolite substrates with {gene_name}.",
                category=EvaluationCategory.TWO_HOP_RELATIONSHIP,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.LIST,
                required_fields=["metabolite_associations_count", "gene_name"],
                min_field_count=2,  # Must have multiple associations for sharing
                entity_type=EntityType.PROTEIN
            ),
            
            # External Reference - Enhanced variety
            QuestionTemplate(
                template="What is the UniProt accession for {gene_name}?",
                category=EvaluationCategory.EXTERNAL_REFERENCE,
                complexity=ComplexityLevel.LEVEL_1,
                answer_type=AnswerType.STRING,
                required_fields=["uniprot_id", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Provide external database identifiers for protein {gene_name}.",
                category=EvaluationCategory.EXTERNAL_REFERENCE,
                complexity=ComplexityLevel.LEVEL_2,
                answer_type=AnswerType.STRING,
                required_fields=["uniprot_id", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            
            # Cross-linkage - Multi-database linkage questions for proteins
            QuestionTemplate(
                template="How is protein {gene_name} cross-referenced in external databases?",
                category=EvaluationCategory.CROSS_LINKAGE,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["uniprot_id", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            
            # Comparison - Enhanced diversity
            QuestionTemplate(
                template="How does {gene_name} compare to other proteins with similar functions?",
                category=EvaluationCategory.COMPARISON,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["general_function", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="What distinguishes {gene_name} from other metabolic enzymes?",
                category=EvaluationCategory.COMPARISON,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["general_function", "gene_name"],
                entity_type=EntityType.PROTEIN
            ),
            QuestionTemplate(
                template="Compare the metabolite associations of {gene_name} with similar proteins.",
                category=EvaluationCategory.COMPARISON,
                complexity=ComplexityLevel.LEVEL_3,
                answer_type=AnswerType.STRING,
                required_fields=["metabolite_associations_count", "gene_name"],
                entity_type=EntityType.PROTEIN
            )
        ])
        
        return templates
    
    def get_templates_by_category(self, category: EvaluationCategory) -> List[QuestionTemplate]:
        """Get all templates for a specific category"""
        return [t for t in self.templates if t.category == category]
    
    def get_templates_by_complexity(self, complexity: ComplexityLevel) -> List[QuestionTemplate]:
        """Get all templates for a specific complexity level"""
        return [t for t in self.templates if t.complexity == complexity]
    
    def get_templates_by_entity_type(self, entity_type: EntityType) -> List[QuestionTemplate]:
        """Get all templates for a specific entity type"""
        return [t for t in self.templates if t.entity_type == entity_type]

@dataclass
class EvaluationMetrics:
    """Metrics for tracking evaluation coverage and quality"""
    total_questions: int = 0
    questions_by_category: Dict[str, int] = field(default_factory=dict)
    questions_by_complexity: Dict[int, int] = field(default_factory=dict)
    questions_by_entity_type: Dict[str, int] = field(default_factory=dict)
    coverage_by_specimen_type: Dict[str, int] = field(default_factory=dict)
    coverage_by_chemical_class: Dict[str, int] = field(default_factory=dict)
    
    def add_question(self, question: GeneratedQuestion):
        """Add a question to metrics tracking"""
        self.total_questions += 1
        
        # Track by category
        cat_key = question.category.value
        self.questions_by_category[cat_key] = self.questions_by_category.get(cat_key, 0) + 1
        
        # Track by complexity
        comp_key = question.complexity.value
        self.questions_by_complexity[comp_key] = self.questions_by_complexity.get(comp_key, 0) + 1
        
        # Track by entity type
        entity_key = question.entity_type.value
        self.questions_by_entity_type[entity_key] = self.questions_by_entity_type.get(entity_key, 0) + 1
    
    def get_coverage_report(self) -> Dict[str, Any]:
        """Generate comprehensive coverage report"""
        return {
            "total_questions": self.total_questions,
            "category_distribution": self.questions_by_category,
            "complexity_distribution": self.questions_by_complexity,
            "entity_type_distribution": self.questions_by_entity_type,
            "specimen_coverage": self.coverage_by_specimen_type,
            "chemical_class_coverage": self.coverage_by_chemical_class,
            "generated_at": datetime.now().isoformat()
        } 
