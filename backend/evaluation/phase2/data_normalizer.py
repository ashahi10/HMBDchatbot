"""
Data Normalizer for Phase 2 Evaluation
======================================

This module handles field name inconsistencies and data structure differences
between different data sources (Phase1, Approach2, main system).

"""

import pandas as pd
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class DataNormalizer:
    """Normalize data structures across different sources"""
    
    def __init__(self):
        # Field mapping from various sources to standardized names
        self.metabolite_field_map = {
            # From HMDB API
            'moldb_formula': 'chemical_formula',
            'moldb_average_mass': 'average_molecular_weight', 
            'moldb_mono_mass': 'monoisotopic_molecular_weight',
            'moldb_smiles': 'smiles',
            'moldb_inchi': 'inchi',
            'moldb_inchikey': 'inchikey',
            'moldb_alogps_solubility': 'solubility',
            'moldb_alogps_logp': 'logp',
            'cas': 'cas_registry_number',
            
            # Phase1 variations
            'hmdb_id': 'accession',
            'molecular_weight': 'average_molecular_weight',
            'cas_number': 'cas_registry_number',
            
            # Keep standard names as-is
            'name': 'name',
            'description': 'description',
            'chemical_formula': 'chemical_formula',
            'average_molecular_weight': 'average_molecular_weight'
        }
        
        self.protein_field_map = {
            # Ensure consistent protein field names
            'protein_accession': 'protein_accession',
            'gene_name': 'gene_name',
            'uniprot_id': 'uniprot_id',
            'general_function': 'general_function',
            'specific_function': 'specific_function'
        }
    
    def normalize_metabolite_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize metabolite dataframe to standard field names"""
        normalized_df = df.copy()
        
        # Apply field name mapping
        for old_name, new_name in self.metabolite_field_map.items():
            if old_name in normalized_df.columns and new_name != old_name:
                if new_name not in normalized_df.columns:
                    normalized_df[new_name] = normalized_df[old_name]
                    logger.debug(f"Mapped {old_name} -> {new_name}")
        
        # Ensure essential columns exist with defaults
        essential_columns = {
            'accession': '',
            'name': '',
            'chemical_formula': '',
            'average_molecular_weight': None,
            'smiles': '',
            'protein_associations_count': 0,
            'normal_concentrations_count': 0,
            'abnormal_concentrations_count': 0
        }
        
        for col, default_value in essential_columns.items():
            if col not in normalized_df.columns:
                normalized_df[col] = default_value
                logger.debug(f"Added missing column {col} with default value")
        
        return normalized_df
    
    def normalize_protein_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize protein dataframe to standard field names"""
        normalized_df = df.copy()
        
        # Apply field name mapping
        for old_name, new_name in self.protein_field_map.items():
            if old_name in normalized_df.columns and new_name != old_name:
                if new_name not in normalized_df.columns:
                    normalized_df[new_name] = normalized_df[old_name]
                    logger.debug(f"Mapped {old_name} -> {new_name}")
        
        # Ensure essential columns exist
        essential_columns = {
            'protein_accession': '',
            'gene_name': '',
            'uniprot_id': '',
            'general_function': '',
            'metabolite_associations_count': 0
        }
        
        for col, default_value in essential_columns.items():
            if col not in normalized_df.columns:
                normalized_df[col] = default_value
                logger.debug(f"Added missing protein column {col} with default value")
        
        return normalized_df
    
    def validate_required_fields(self, df: pd.DataFrame, entity_type: str) -> Dict[str, Any]:
        """Validate that required fields are present and report completeness"""
        if entity_type == 'metabolite':
            required_fields = ['accession', 'name']
            preferred_fields = ['chemical_formula', 'average_molecular_weight', 'smiles']
        elif entity_type == 'protein':
            required_fields = ['protein_accession']
            preferred_fields = ['gene_name', 'uniprot_id', 'general_function']
        else:
            return {'error': f'Unknown entity type: {entity_type}'}
        
        validation_report = {
            'total_records': len(df),
            'required_field_completeness': {},
            'preferred_field_completeness': {},
            'overall_completeness_score': 0.0
        }
        
        # Check required fields
        for field in required_fields:
            if field in df.columns:
                non_empty = df[field].notna().sum()
                completeness = non_empty / len(df) if len(df) > 0 else 0
                validation_report['required_field_completeness'][field] = {
                    'count': int(non_empty),
                    'percentage': round(completeness * 100, 2)
                }
            else:
                validation_report['required_field_completeness'][field] = {
                    'count': 0,
                    'percentage': 0.0,
                    'missing': True
                }
        
        # Check preferred fields
        for field in preferred_fields:
            if field in df.columns:
                non_empty = df[field].notna().sum()
                completeness = non_empty / len(df) if len(df) > 0 else 0
                validation_report['preferred_field_completeness'][field] = {
                    'count': int(non_empty),
                    'percentage': round(completeness * 100, 2)
                }
            else:
                validation_report['preferred_field_completeness'][field] = {
                    'count': 0,
                    'percentage': 0.0,
                    'missing': True
                }
        
        # Calculate overall completeness score
        all_fields = required_fields + preferred_fields
        available_fields = [f for f in all_fields if f in df.columns]
        if available_fields and len(df) > 0:
            total_completeness = sum(df[f].notna().sum() for f in available_fields)
            max_possible = len(available_fields) * len(df)
            validation_report['overall_completeness_score'] = round(
                total_completeness / max_possible * 100, 2
            ) if max_possible > 0 else 0.0
        
        return validation_report
