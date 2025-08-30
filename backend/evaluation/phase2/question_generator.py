"""
Phase 2: Question Generator
Comprehensive question generation system for automated evaluation

This module generates question-answer pairs from sampled entities using
template-based generation and LLM-enhanced diversification.

Author: Senior Engineering Implementation
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Set, Optional, Tuple, Any, Union
import json
import logging
import re
import asyncio
from datetime import datetime
from pathlib import Path

from .evaluation_framework import (
    EvaluationCategory, ComplexityLevel, EntityType, AnswerType,
    QuestionTemplate, GeneratedQuestion, QuestionTemplateLibrary, EvaluationMetrics
)
from backend.services.llm_service import MultiLLMService, LLMProvider

# Configure logging
logger = logging.getLogger(__name__)

class AnswerExtractor:
    """Extract answers from parsed data based on question templates"""
    
    def __init__(self, metabolites_df: pd.DataFrame, proteins_df: pd.DataFrame, relationships_df: pd.DataFrame):
        self.metabolites_df = metabolites_df
        self.proteins_df = proteins_df
        self.relationships_df = relationships_df
        
        # Build lookup indexes for efficient queries
        self._build_lookup_indexes()
    
    def _build_lookup_indexes(self):
        """Build efficient lookup indexes"""
        # Metabolite lookups
        self.metabolite_by_accession = self.metabolites_df.set_index('accession').to_dict('index')
        self.metabolite_by_name = {}
        for _, row in self.metabolites_df.iterrows():
            if pd.notna(row['name']) and row['name']:
                self.metabolite_by_name[row['name']] = row.to_dict()
        
        # Protein lookups
        self.protein_by_accession = self.proteins_df.set_index('protein_accession').to_dict('index')
        self.protein_by_gene = {}
        for _, row in self.proteins_df.iterrows():
            if pd.notna(row['gene_name']) and row['gene_name']:
                self.protein_by_gene[row['gene_name']] = row.to_dict()
        
        # Relationship lookups
        self.protein_to_metabolites = {}
        self.metabolite_to_proteins = {}
        
        for _, row in self.relationships_df.iterrows():
            protein_acc = row['protein_accession']
            metabolite_acc = row['metabolite_accession']
            
            if protein_acc not in self.protein_to_metabolites:
                self.protein_to_metabolites[protein_acc] = []
            self.protein_to_metabolites[protein_acc].append(metabolite_acc)
            
            if metabolite_acc not in self.metabolite_to_proteins:
                self.metabolite_to_proteins[metabolite_acc] = []
            self.metabolite_to_proteins[metabolite_acc].append(protein_acc)
    
    def extract_answer(self, template: QuestionTemplate, entity_data: Dict[str, Any]) -> Optional[Any]:
        """Extract answer based on template and entity data"""
        try:
            if template.entity_type == EntityType.METABOLITE:
                return self._extract_metabolite_answer(template, entity_data)
            elif template.entity_type == EntityType.PROTEIN:
                return self._extract_protein_answer(template, entity_data)
            else:
                return None
        except Exception as e:
            logger.error(f"Error extracting answer: {e}")
            return None
    
    def _extract_metabolite_answer(self, template: QuestionTemplate, metabolite_data: Dict[str, Any]) -> Optional[Any]:
        """Extract answers for metabolite questions using ONLY real data - NO FABRICATION"""
        accession = metabolite_data.get('accession', '')
        name = metabolite_data.get('name', '')  # FIX: Ensure name is defined for all uses
        
        if template.answer_type == AnswerType.STRING:
            # FIXED: Use only real data fields, return None when no real data exists
            if template.category == EvaluationCategory.ENTITY_FACTOID:
                # Return actual field values ONLY
                for field in template.required_fields:
                    if field in metabolite_data and pd.notna(metabolite_data[field]):
                        value = str(metabolite_data[field]).strip()
                        if value and value.lower() not in ['nan', '', 'none', 'null']:
                            return value
                return None  # No fabrication - return None if no real data
            
            elif template.category == EvaluationCategory.ENTITY_DESCRIPTION:
                # Use only real description field - no fabrication
                description = metabolite_data.get('description', '')
                if description and pd.notna(description) and str(description).strip():
                    desc_str = str(description).strip()
                    if desc_str.lower() not in ['nan', '', 'none', 'null']:
                        return desc_str
                return None  # No real description available
                
            elif template.category == EvaluationCategory.PROPERTY_EXTRACTION:
                # Return actual chemical properties ONLY
                for field in template.required_fields:
                    if field in metabolite_data and pd.notna(metabolite_data[field]):
                        value = str(metabolite_data[field]).strip()
                        if value and value.lower() not in ['nan', '', 'none', 'null']:
                            return value
                return None  # Only return None after checking ALL fields
            
            elif template.category == EvaluationCategory.AGGREGATION:
                # FIXED: Provide real count-based answers only
                if "protein interactions" in template.template.lower() or "documented protein" in template.template.lower():
                    protein_count = metabolite_data.get('protein_associations_count', 0)
                    if pd.notna(protein_count) and protein_count > 0:
                        return f"Yes, {name} has {int(protein_count)} documented protein interactions"
                    else:
                        return f"No, {name} has no documented protein interactions in the database"
                elif "concentration data" in template.template.lower():
                    normal_count = metabolite_data.get('normal_concentrations_count', 0)
                    abnormal_count = metabolite_data.get('abnormal_concentrations_count', 0)
                    total_count = (normal_count or 0) + (abnormal_count or 0)
                    if total_count > 0:
                        return f"Yes, {name} has {int(total_count)} concentration measurements available"
                    else:
                        return f"No concentration data is available for {name}"
                else:
                    return None
            
            # REMOVED: All specimen-specific fabrication
            elif template.category == EvaluationCategory.SPECIMEN_SPECIFIC:
                # NO FABRICATION: Only return data if we have real specimen information
                normal_count = metabolite_data.get('normal_concentrations_count', 0)
                abnormal_count = metabolite_data.get('abnormal_concentrations_count', 0)
                if (normal_count or 0) > 0 or (abnormal_count or 0) > 0:
                    total_measurements = (normal_count or 0) + (abnormal_count or 0)
                    return f"Concentration data available from {int(total_measurements)} measurements (specific specimens not detailed in current dataset)"
                else:
                    return "No specimen-specific concentration data available in current dataset"
                    
            # REMOVED: All cross-linkage fabrication 
            elif template.category == EvaluationCategory.CROSS_LINKAGE:
                # Only return real external IDs that exist
                external_ids = []
                id_fields = ['pubchem_compound_id', 'chebi_id', 'kegg_id', 'cas_registry_number']
                for field in id_fields:
                    if field in metabolite_data and pd.notna(metabolite_data[field]):
                        value = str(metabolite_data[field]).strip()
                        if value and value.lower() not in ['nan', '', 'none', 'null']:
                            external_ids.append(f"{field}: {value}")
                
                if external_ids:
                    return f"Available external identifiers: {'; '.join(external_ids)}"
                else:
                    return "No external database identifiers available"
            
            # Handle other string fields with real data only
            else:
                for field in template.required_fields:
                    if field in metabolite_data and pd.notna(metabolite_data[field]):
                        value = str(metabolite_data[field]).strip()
                        if value and value.lower() not in ['nan', '', 'none', 'null']:
                            return value
                return None
        
        elif template.answer_type == AnswerType.NUMBER:
            # Numeric fields - use real data only
            for field in template.required_fields:
                if field in metabolite_data and pd.notna(metabolite_data[field]):
                    try:
                        value = float(metabolite_data[field])
                        if not np.isnan(value) and value >= 0:  # Valid positive numbers only
                            return value
                    except (ValueError, TypeError):
                        continue
            return None
        
        elif template.answer_type == AnswerType.LIST:
            # List-based answers using real relationship data only
            if template.category == EvaluationCategory.ONE_HOP_RELATIONSHIP:
                # Real protein associations only
                if accession in self.metabolite_to_proteins:
                    protein_accs = self.metabolite_to_proteins[accession]
                    protein_names = []
                    for p_acc in protein_accs:
                        if p_acc in self.protein_by_accession:
                            gene_name = self.protein_by_accession[p_acc].get('gene_name', '')
                            if gene_name and str(gene_name).lower() not in ['nan', '', 'none', 'null']:
                                protein_names.append(gene_name)
                            else:
                                protein_names.append(p_acc)  # Use accession if no gene name
                    return protein_names if protein_names else []
                else:
                    return []  # No associations found
            
            # REMOVED: All fabricated specimen lists
            elif template.category == EvaluationCategory.SPECIMEN_SPECIFIC:
                # NO FABRICATION: Return empty list since we don't have real specimen data
                return []  # Honest response - no specimen data available
            
            return []
        
        elif template.answer_type == AnswerType.DICT:
            # External references using real data only
            if template.category == EvaluationCategory.EXTERNAL_REFERENCE or template.category == EvaluationCategory.CROSS_LINKAGE:
                external_refs = {}
                ref_fields = ['pubchem_compound_id', 'chebi_id', 'kegg_id', 'cas_registry_number']
                for field in ref_fields:
                    if field in metabolite_data and pd.notna(metabolite_data[field]):
                        value = str(metabolite_data[field]).strip()
                        if value and value.lower() not in ['nan', '', 'none', 'null']:
                            external_refs[field] = value
                return external_refs if external_refs else {}  # Empty dict if no real references
        
        return None
    
    def _extract_protein_answer(self, template: QuestionTemplate, protein_data: Dict[str, Any]) -> Optional[Any]:
        """Extract answers for protein questions using ONLY real data - NO FABRICATION"""
        protein_accession = protein_data.get('protein_accession', '')
        
        if template.answer_type == AnswerType.STRING:
            # FIXED: Use only real data fields, no fabricated responses
            if template.category == EvaluationCategory.ENTITY_FACTOID:
                # Return actual field values ONLY
                for field in template.required_fields:
                    if field in protein_data and pd.notna(protein_data[field]):
                        value = str(protein_data[field]).strip()
                        if value and value.lower() not in ['nan', '', 'none', 'null']:
                            return value
                return None  # No fabrication - return None if no real data
                
            elif template.category == EvaluationCategory.ENTITY_DESCRIPTION:
                # Use only real function fields - no templated responses
                if "function" in template.template.lower():
                    # Return actual function description from data
                    general_function = protein_data.get('general_function', '')
                    specific_function = protein_data.get('specific_function', '')
                    
                    if general_function and pd.notna(general_function):
                        func_str = str(general_function).strip()
                        if func_str.lower() not in ['nan', '', 'none', 'null']:
                            return func_str
                    elif specific_function and pd.notna(specific_function):
                        func_str = str(specific_function).strip()
                        if func_str.lower() not in ['nan', '', 'none', 'null']:
                            return func_str
                    return None  # No real function data available
                else:
                    # For other description questions, return None if no specific data
                    return None
                    
            elif template.category == EvaluationCategory.PROPERTY_EXTRACTION:
                # Return actual protein properties ONLY
                for field in template.required_fields:
                        if field in protein_data and pd.notna(protein_data[field]):
                            value = str(protein_data[field]).strip()
                        if value and value.lower() not in ['nan', '', 'none', 'null']:
                                return value
                return None
            
            # REMOVED: All specimen/tissue expression fabrication
            elif template.category == EvaluationCategory.SPECIMEN_SPECIFIC:
                # NO FABRICATION: Only return data if we have real tissue/specimen information
                cellular_location = protein_data.get('cellular_location', '')
                if cellular_location and pd.notna(cellular_location):
                    loc_str = str(cellular_location).strip()
                    if loc_str.lower() not in ['nan', '', 'none', 'null']:
                        return f"Cellular location: {loc_str}"
                else:
                    return "No specific tissue/cellular location data available in current dataset"
                    
            elif template.category == EvaluationCategory.AGGREGATION:
                # FIXED: Provide real count-based answers only
                gene_name = protein_data.get('gene_name', '')
                if "metabolite associations" in template.template.lower():
                    metabolite_count = protein_data.get('metabolite_associations_count', 0)
                    if pd.notna(metabolite_count) and metabolite_count > 0:
                        return f"Yes, {gene_name} protein has {int(metabolite_count)} documented metabolite associations"
                    else:
                        return f"No, {gene_name} protein has no documented metabolite associations in the database"
                elif "pathway associations" in template.template.lower():
                    pathway_count = protein_data.get('pathway_associations_count', 0)
                    if pd.notna(pathway_count) and pathway_count > 0:
                        return f"Yes, {gene_name} protein has {int(pathway_count)} documented pathway associations"
                    else:
                        return f"No pathway association data available for {gene_name} protein"
                else:
                    return None
                    
            # REMOVED: All cross-linkage fabrication
            elif template.category == EvaluationCategory.CROSS_LINKAGE:
                # Only return real external IDs that exist
                external_ids = []
                if protein_data.get('uniprot_id') and pd.notna(protein_data.get('uniprot_id')):
                    uniprot_id = str(protein_data['uniprot_id']).strip()
                    if uniprot_id.lower() not in ['nan', '', 'none', 'null']:
                        external_ids.append(f"UniProt ID: {uniprot_id}")
                
                if external_ids:
                    return f"Available external identifiers: {'; '.join(external_ids)}"
                else:
                    return "No external database identifiers available"
            
            # REMOVED: All comparison fabrication
            elif template.category == EvaluationCategory.COMPARISON:
                # Only provide real count comparisons, no fabricated context
                gene_name = protein_data.get('gene_name', '')
                metabolite_count = protein_data.get('metabolite_associations_count', 0)
                if pd.notna(metabolite_count):
                    return f"{gene_name} has {int(metabolite_count)} metabolite associations"
                else:
                    return f"No association count data available for {gene_name}"
            
            # Handle other string fields with real data only
            else:
                for field in template.required_fields:
                    if field in protein_data and pd.notna(protein_data[field]):
                        value = str(protein_data[field]).strip()
                        if value and value.lower() not in ['nan', '', 'none', 'null']:
                            return value
                return None
        
        elif template.answer_type == AnswerType.NUMBER:
            # Numeric fields - use real data only
            for field in template.required_fields:
                if field in protein_data and pd.notna(protein_data[field]):
                    try:
                        value = int(protein_data[field])
                        if not np.isnan(value) and value >= 0:  # Valid non-negative numbers only
                            return value
                    except (ValueError, TypeError):
                        continue
            return None
        
        elif template.answer_type == AnswerType.LIST:
            # List-based answers using real relationship data only
            if template.category == EvaluationCategory.ONE_HOP_RELATIONSHIP:
                # Real metabolite associations only
                if protein_accession in self.protein_to_metabolites:
                    metabolite_accs = self.protein_to_metabolites[protein_accession]
                    metabolite_names = []
                    for m_acc in metabolite_accs:
                        if m_acc in self.metabolite_by_accession:
                            name = self.metabolite_by_accession[m_acc].get('name', '')
                            if name and str(name).lower() not in ['nan', '', 'none', 'null']:
                                metabolite_names.append(name)
                            else:
                                metabolite_names.append(m_acc)  # Use accession if no name
                    return metabolite_names if metabolite_names else []
                else:
                    return []  # No associations found
            
            # REMOVED: All fabricated lists
            else:
                return []  # Return empty list for categories without real data
        
        elif template.answer_type == AnswerType.DICT:
            # External references using real data only
            if template.category == EvaluationCategory.EXTERNAL_REFERENCE or template.category == EvaluationCategory.CROSS_LINKAGE:
                external_refs = {}
                if protein_data.get('uniprot_id') and pd.notna(protein_data.get('uniprot_id')):
                    uniprot_id = str(protein_data['uniprot_id']).strip()
                    if uniprot_id.lower() not in ['nan', '', 'none', 'null']:
                        external_refs['uniprot_id'] = uniprot_id
                return external_refs if external_refs else {}  # Empty dict if no real references
        
        return None
    
    def validate_answer(self, answer: Any, template: QuestionTemplate) -> bool:
        """Validate that an answer meets template requirements"""
        if answer is None:
            return False
        
        if template.answer_type == AnswerType.STRING:
            return isinstance(answer, str) and len(answer.strip()) > 0
        
        elif template.answer_type == AnswerType.NUMBER:
            return isinstance(answer, (int, float)) and not np.isnan(answer)
        
        elif template.answer_type == AnswerType.LIST:
            return isinstance(answer, list) and len(answer) >= template.min_field_count
        
        elif template.answer_type == AnswerType.DICT:
            return isinstance(answer, dict) and len(answer) > 0
        
        elif template.answer_type == AnswerType.BOOLEAN:
            return isinstance(answer, bool)
        
        return True

class QuestionGenerator:
    """Main question generation system"""
    
    def __init__(self, metabolites_df: pd.DataFrame, proteins_df: pd.DataFrame, relationships_df: pd.DataFrame, llm_service: Optional[MultiLLMService] = None):
        self.metabolites_df = metabolites_df
        self.proteins_df = proteins_df
        self.relationships_df = relationships_df
        self.llm_service = llm_service
        
        # Initialize components
        self.template_library = QuestionTemplateLibrary()
        self.answer_extractor = AnswerExtractor(metabolites_df, proteins_df, relationships_df)
        self.metrics = EvaluationMetrics()
        
        # Generated questions storage
        self.generated_questions: List[GeneratedQuestion] = []
    
    def generate_questions_for_entities(self, metabolite_sample: pd.DataFrame, protein_sample: pd.DataFrame) -> List[GeneratedQuestion]:
        """Generate questions for sampled entities"""
        logger.info(f"Generating questions for {len(metabolite_sample)} metabolites and {len(protein_sample)} proteins")
        
        questions = []
        
        # Generate metabolite questions
        for _, metabolite in metabolite_sample.iterrows():
            metabolite_questions = self._generate_metabolite_questions(metabolite.to_dict())
            questions.extend(metabolite_questions)
        
        # Generate protein questions
        for _, protein in protein_sample.iterrows():
            protein_questions = self._generate_protein_questions(protein.to_dict())
            questions.extend(protein_questions)
        
        self.generated_questions = questions
        logger.info(f"Generated {len(questions)} total questions")
        
        # Update metrics
        for question in questions:
            self.metrics.add_question(question)
        
        return questions
    
    def _generate_metabolite_questions(self, metabolite_data: Dict[str, Any]) -> List[GeneratedQuestion]:
        """Generate questions for a single metabolite"""
        questions = []
        metabolite_templates = self.template_library.get_templates_by_entity_type(EntityType.METABOLITE)
        
        for template in metabolite_templates:
            # Check if entity has required fields
            if not self._entity_has_required_fields(metabolite_data, template):
                continue
            
            # Generate question text
            question_text = self._format_question(template, metabolite_data)
            if not question_text:
                continue
            
            # Extract answer
            answer = self.answer_extractor.extract_answer(template, metabolite_data)
            if not self.answer_extractor.validate_answer(answer, template):
                continue
            
            # IMPROVED: Create metadata with proper NaN handling
            metadata = {
                "template": template.template,
                "entity_name": self._clean_metadata_value(metabolite_data.get('name', '')),
                "chemical_class": self._clean_metadata_value(metabolite_data.get('taxonomy_class', '')),
                "molecular_weight": self._clean_metadata_value(metabolite_data.get('average_molecular_weight', ''))
            }
            
            # Create question object
            question = GeneratedQuestion(
                question=question_text,
                answer=answer,
                entity_id=metabolite_data.get('accession', ''),
                entity_type=EntityType.METABOLITE,
                category=template.category,
                complexity=template.complexity,
                answer_type=template.answer_type,
                source_type="scripted",
                metadata=metadata
            )
            
            questions.append(question)
        
        return questions
    
    def _generate_protein_questions(self, protein_data: Dict[str, Any]) -> List[GeneratedQuestion]:
        """Generate questions for a single protein"""
        questions = []
        protein_templates = self.template_library.get_templates_by_entity_type(EntityType.PROTEIN)
        
        for template in protein_templates:
            # Check if entity has required fields
            if not self._entity_has_required_fields(protein_data, template):
                continue
            
            # Generate question text
            question_text = self._format_question(template, protein_data)
            if not question_text:
                continue
            
            # Extract answer
            answer = self.answer_extractor.extract_answer(template, protein_data)
            if not self.answer_extractor.validate_answer(answer, template):
                continue
            
            # IMPROVED: Create metadata with proper NaN handling
            metadata = {
                "template": template.template,
                "gene_name": self._clean_metadata_value(protein_data.get('gene_name', '')),
                "uniprot_id": self._clean_metadata_value(protein_data.get('uniprot_id', '')),
                "function": self._clean_metadata_value(protein_data.get('general_function', ''))
            }
            
            # Create question object
            question = GeneratedQuestion(
                question=question_text,
                answer=answer,
                entity_id=protein_data.get('protein_accession', ''),
                entity_type=EntityType.PROTEIN,
                category=template.category,
                complexity=template.complexity,
                answer_type=template.answer_type,
                source_type="scripted",
                metadata=metadata
            )
            
            questions.append(question)
        
        return questions
    
    def _entity_has_required_fields(self, entity_data: Dict[str, Any], template: QuestionTemplate) -> bool:
        """Check if entity has all required fields for template with improved validation"""
        # Check basic required fields
        for field in template.required_fields:
            if field not in entity_data or pd.isna(entity_data[field]) or str(entity_data[field]).strip() == '' or str(entity_data[field]).lower() == 'nan':
                return False
        
        # IMPROVED: Smart validation for specific template types with diversity consideration
        if template.category == EvaluationCategory.AGGREGATION:
            # For aggregation questions, ensure count >= 0 (accept zero counts for "no associations" questions)
            if 'protein_associations_count' in template.required_fields:
                count = entity_data.get('protein_associations_count', 0)
                if pd.isna(count) or count < 0:  # Accept 0 and positive values
                    return False
                # DIVERSITY: Accept different ranges
                return True
            if 'metabolite_associations_count' in template.required_fields:
                count = entity_data.get('metabolite_associations_count', 0)
                if pd.isna(count) or count < 0:  # Accept 0 and positive values
                    return False
                return True
        
        if template.category == EvaluationCategory.ONE_HOP_RELATIONSHIP:
            # For relationship questions, require actual associations (> 0)
            if 'protein_associations_count' in template.required_fields:
                count = entity_data.get('protein_associations_count', 0)
                if pd.isna(count) or count <= 0:
                    return False
                # DIVERSITY: Accept any positive count, don't bias toward high counts
                return True
            if 'metabolite_associations_count' in template.required_fields:
                count = entity_data.get('metabolite_associations_count', 0)
                if pd.isna(count) or count <= 0:
                    return False
                return True
        
        if template.category == EvaluationCategory.TWO_HOP_RELATIONSHIP:
            # For two-hop relationships, require multiple associations (>= 2)
            if 'protein_associations_count' in template.required_fields:
                count = entity_data.get('protein_associations_count', 0)
                if pd.isna(count) or count < 2:  # Need at least 2 for meaningful two-hop
                    return False
                return True
            if 'metabolite_associations_count' in template.required_fields:
                count = entity_data.get('metabolite_associations_count', 0)
                if pd.isna(count) or count < 2:
                    return False
                return True
        
        # For other categories, accept any entity with required fields present
        return True
    
    def _format_question(self, template: QuestionTemplate, entity_data: Dict[str, Any]) -> Optional[str]:
        """Format question template with entity data"""
        try:
            # Extract available formatting variables
            format_vars = {}
            
            # Add all entity fields as potential variables
            for key, value in entity_data.items():
                if pd.notna(value) and str(value).strip():
                    format_vars[key] = str(value).strip()
            
            # Format the template
            question_text = template.template.format(**format_vars)
            return question_text
        
        except KeyError as e:
            logger.warning(f"Missing format variable {e} for template: {template.template}")
            return None
        except Exception as e:
            logger.error(f"Error formatting question template: {e}")
            return None
    
    async def diversify_questions_with_llm(self, questions: List[GeneratedQuestion], max_variations: int = 2) -> List[GeneratedQuestion]:
        """Use LLM to create more natural variations of questions"""
        if not self.llm_service:
            logger.warning("No LLM service available for question diversification")
            return questions
        
        logger.info(f"Diversifying {len(questions)} questions with LLM...")
        
        diversified_questions = []
        
        for question in questions:
            # Always keep original
            diversified_questions.append(question)
            
            # Generate variations for certain complexity levels
            if question.complexity in [ComplexityLevel.LEVEL_4, ComplexityLevel.LEVEL_5]:
                variations = await self._generate_llm_variations(question, max_variations)
                diversified_questions.extend(variations)
        
        logger.info(f"Generated {len(diversified_questions)} total questions after diversification")
        return diversified_questions
    
    async def _generate_llm_variations(self, original_question: GeneratedQuestion, max_variations: int) -> List[GeneratedQuestion]:
        """Generate LLM variations of a question"""
        variations = []
        
        try:
            prompt = f"""
            Please create {max_variations} natural, human-like variations of this biomedical question. 
            Keep the same meaning and expected answer format, but make them sound more conversational or research-oriented.

            Original question: {original_question.question}
            Answer type: {original_question.answer_type.value}
            Category: {original_question.category.value}

            Provide variations as a JSON array: ["variation1", "variation2", ...]
            """
            
            # Use the LLM service to generate variations
            llm = self.llm_service.get_langchain_llm(streaming=False, temperature=0.7)
            response = await llm.ainvoke(prompt)
            
            # Parse response
            try:
                variations_text = response.content if hasattr(response, 'content') else str(response)
                
                # Extract JSON array from response
                import json
                json_match = re.search(r'\[.*?\]', variations_text, re.DOTALL)
                if json_match:
                    variations_list = json.loads(json_match.group())
                    
                    for variation_text in variations_list[:max_variations]:
                        variation = GeneratedQuestion(
                            question=variation_text.strip(),
                            answer=original_question.answer,
                            entity_id=original_question.entity_id,
                            entity_type=original_question.entity_type,
                            category=original_question.category,
                            complexity=original_question.complexity,
                            answer_type=original_question.answer_type,
                            source_type="llm_generated",
                            confidence=0.8,  # Slightly lower confidence for generated variations
                            metadata={
                                **original_question.metadata,
                                "original_question": original_question.question,
                                "generation_method": "llm_variation"
                            }
                        )
                        variations.append(variation)
            
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse LLM response for question: {original_question.question}")
        
        except Exception as e:
            logger.error(f"Error generating LLM variations: {e}")
        
        return variations
    
    def export_questions(self, output_path: str, format: str = "json"):
        """Export generated questions to file"""
        output_file = Path(output_path)
        
        if format.lower() == "json":
            questions_data = [q.to_dict() for q in self.generated_questions]
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump({
                    "metadata": {
                        "total_questions": len(questions_data),
                        "generated_at": datetime.now().isoformat(),
                        "metrics": self.metrics.get_coverage_report()
                    },
                    "questions": questions_data
                }, f, indent=2, ensure_ascii=False)
        
        elif format.lower() == "csv":
            questions_df = pd.DataFrame([q.to_dict() for q in self.generated_questions])
            questions_df.to_csv(output_file, index=False)
        
        logger.info(f"Exported {len(self.generated_questions)} questions to {output_file}")
    
    def get_generation_report(self) -> Dict[str, Any]:
        """Generate comprehensive question generation report"""
        return {
            "generation_summary": {
                "total_questions": len(self.generated_questions),
                "unique_entities": len(set(q.entity_id for q in self.generated_questions)),
                "template_coverage": len(set(q.metadata.get('template', '') for q in self.generated_questions)),
                "generated_at": datetime.now().isoformat()
            },
            "metrics": self.metrics.get_coverage_report(),
            "quality_stats": self._calculate_quality_stats(),
            "distribution_analysis": self._analyze_question_distribution()
        }
    
    def _calculate_quality_stats(self) -> Dict[str, Any]:
        """Calculate question quality statistics"""
        total = len(self.generated_questions)
        if total == 0:
            return {}
        
        answerable_count = sum(1 for q in self.generated_questions if q.answer is not None)
        high_confidence = sum(1 for q in self.generated_questions if q.confidence >= 0.8)
        
        return {
            "answerability_rate": answerable_count / total,
            "high_confidence_rate": high_confidence / total,
            "avg_confidence": np.mean([q.confidence for q in self.generated_questions]),
            "source_distribution": {
                source: sum(1 for q in self.generated_questions if q.source_type == source)
                for source in set(q.source_type for q in self.generated_questions)
            }
        }
    
    def _analyze_question_distribution(self) -> Dict[str, Any]:
        """Analyze question distribution across different dimensions"""
        if not self.generated_questions:
            return {}
        
        return {
            "complexity_distribution": {
                level.value: sum(1 for q in self.generated_questions if q.complexity == level)
                for level in ComplexityLevel
            },
            "category_distribution": {
                cat.value: sum(1 for q in self.generated_questions if q.category == cat)
                for cat in EvaluationCategory
            },
            "answer_type_distribution": {
                atype.value: sum(1 for q in self.generated_questions if q.answer_type == atype)
                for atype in AnswerType
            },
            "entity_coverage": {
                "metabolites": sum(1 for q in self.generated_questions if q.entity_type == EntityType.METABOLITE),
                "proteins": sum(1 for q in self.generated_questions if q.entity_type == EntityType.PROTEIN)
            }
        } 

    def _clean_metadata_value(self, value: Any) -> Any:
        """Clean metadata values to ensure JSON validity and handle NaN properly"""
        if pd.isna(value):
            return None  # Convert NaN to None (JSON null)
        
        if isinstance(value, str):
            value = value.strip()
            if value.lower() in ['nan', 'none', '']:
                return None
            return value
        
        if isinstance(value, (int, float)):
            if np.isnan(value):
                return None
            return value
        
        return value 