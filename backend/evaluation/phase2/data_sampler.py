"""
Phase 2: Data Sampler
Stratified sampling system for selecting representative subsets from Phase 1 parsed data

This module implements intelligent sampling strategies to ensure diversity and 
scalability while maintaining representativeness across different entity types,
chemical classes, and relationship densities.

Author: Senior Engineering Implementation
Version: 1.0.0
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Set, Optional, Tuple, Any
from dataclasses import dataclass
import random
import json
import logging
from pathlib import Path

from .evaluation_framework import EntityType, EvaluationMetrics
from .data_normalizer import DataNormalizer

# Configure logging
logger = logging.getLogger(__name__)

@dataclass
class SamplingConfig:
    """Configuration for sampling strategy"""
    metabolite_sample_size: int = 150
    protein_sample_size: int = 75
    min_relationship_count: int = 1
    max_relationship_count: Optional[int] = None
    diversity_weight: float = 0.3
    coverage_weight: float = 0.7
    random_seed: int = 42
    
    # Stratification parameters
    chemical_class_stratification: bool = True
    specimen_type_stratification: bool = True
    relationship_density_stratification: bool = True
    molecular_weight_stratification: bool = True
    
    # Quality filters
    require_name: bool = True
    require_formula: bool = True
    require_protein_associations: bool = False
    min_data_completeness: float = 0.5

class DataSampler:
    """Advanced stratified sampling for representative test set construction"""
    
    def __init__(self, data_dir: str, config: SamplingConfig = None):
        self.data_dir = Path(data_dir)
        self.config = config or SamplingConfig()
        self.metabolites_df = None
        self.proteins_df = None
        self.relationships_df = None
        self.sampling_stats = {}
        self.data_normalizer = DataNormalizer()
        
        # Set random seed for reproducibility
        random.seed(self.config.random_seed)
        np.random.seed(self.config.random_seed)
        
        self._load_data()
        self._analyze_data_distribution()
    
    def _load_data(self):
        """Load Phase 1 parsed data with validation"""
        try:
            logger.info("Loading Phase 1 parsed data...")
            
            # Load metabolites with fallback
            metabolites_path = self.data_dir / "metabolites_complete_final.csv"
            if not metabolites_path.exists():
                # Try alternative names
                alt_paths = [
                    self.data_dir / "metabolites_complete.csv",
                    self.data_dir / "metabolites_flat.csv"
                ]
                for alt_path in alt_paths:
                    if alt_path.exists():
                        metabolites_path = alt_path
                        break
                else:
                    raise FileNotFoundError(f"No metabolites file found in {self.data_dir}")
            
            self.metabolites_df = pd.read_csv(metabolites_path)
            # Normalize metabolite data
            self.metabolites_df = self.data_normalizer.normalize_metabolite_data(self.metabolites_df)
            logger.info(f"Loaded and normalized {len(self.metabolites_df)} metabolites from {metabolites_path.name}")
            
            # Load proteins with fallback
            proteins_path = self.data_dir / "proteins_complete_final_CORRECTED.csv"
            if not proteins_path.exists():
                alt_paths = [
                    self.data_dir / "proteins_complete_final.csv",
                    self.data_dir / "proteins_complete.csv",
                    self.data_dir / "proteins_flat.csv"
                ]
                for alt_path in alt_paths:
                    if alt_path.exists():
                        proteins_path = alt_path
                        break
                else:
                    raise FileNotFoundError(f"No proteins file found in {self.data_dir}")
            
            self.proteins_df = pd.read_csv(proteins_path)
            # Normalize protein data
            self.proteins_df = self.data_normalizer.normalize_protein_data(self.proteins_df)
            logger.info(f"Loaded and normalized {len(self.proteins_df)} proteins from {proteins_path.name}")
            
            # Load relationships with fallback
            relationships_path = self.data_dir / "protein_metabolite_relationships_CORRECTED.csv"
            if not relationships_path.exists():
                alt_paths = [
                    self.data_dir / "metabolite_protein_relationships.csv",
                    self.data_dir / "protein_metabolite_relationships.csv"
                ]
                for alt_path in alt_paths:
                    if alt_path.exists():
                        relationships_path = alt_path
                        break
                else:
                    logger.warning("No relationships file found - creating empty relationships")
                    self.relationships_df = pd.DataFrame(columns=['protein_accession', 'metabolite_accession'])
                    return
            
            self.relationships_df = pd.read_csv(relationships_path)
            logger.info(f"Loaded {len(self.relationships_df)} relationships from {relationships_path.name}")
            
            # Validate loaded data
            self._validate_data_structure()
            
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise
    
    def _validate_data_structure(self):
        """Validate that loaded data has expected structure"""
        # Check metabolites columns
        required_metabolite_cols = ['name']  # Only absolutely essential
        missing_met_cols = [col for col in required_metabolite_cols if col not in self.metabolites_df.columns]
        if missing_met_cols:
            logger.warning(f"Missing metabolite columns: {missing_met_cols}")
        
        # Check protein columns
        required_protein_cols = ['protein_accession']  # Only absolutely essential
        missing_prot_cols = [col for col in required_protein_cols if col not in self.proteins_df.columns]
        if missing_prot_cols:
            logger.warning(f"Missing protein columns: {missing_prot_cols}")
        
        # Log available columns for debugging
        logger.info(f"Metabolite columns: {list(self.metabolites_df.columns)}")
        logger.info(f"Protein columns: {list(self.proteins_df.columns)}")
        logger.info(f"Relationship columns: {list(self.relationships_df.columns)}")
    
    def _analyze_data_distribution(self):
        """Analyze data distribution for stratified sampling"""
        logger.info("Analyzing data distribution...")
        
        # Metabolite analysis
        metabolite_stats = {
            "total_count": len(self.metabolites_df),
            "chemical_class_distribution": self._get_chemical_class_distribution(),
            "molecular_weight_distribution": self._get_molecular_weight_distribution(),
            "relationship_density_distribution": self._get_relationship_density_distribution("metabolites"),
            "data_completeness_distribution": self._get_data_completeness_distribution("metabolites"),
            "specimen_coverage": self._get_specimen_coverage()
        }
        
        # Protein analysis
        protein_stats = {
            "total_count": len(self.proteins_df),
            "function_distribution": self._get_protein_function_distribution(),
            "relationship_density_distribution": self._get_relationship_density_distribution("proteins"),
            "data_completeness_distribution": self._get_data_completeness_distribution("proteins"),
            "gene_name_coverage": self._get_gene_name_coverage()
        }
        
        self.sampling_stats = {
            "metabolites": metabolite_stats,
            "proteins": protein_stats,
            "relationships": {
                "total_count": len(self.relationships_df),
                "protein_metabolite_pairs": len(self.relationships_df.groupby(['protein_accession', 'metabolite_accession']))
            }
        }
        
        logger.info("Data distribution analysis complete")
    
    def _get_chemical_class_distribution(self) -> Dict[str, int]:
        """Get distribution of chemical classes in metabolites"""
        if 'taxonomy_class' not in self.metabolites_df.columns:
            return {}
        
        # Handle missing values
        class_counts = self.metabolites_df['taxonomy_class'].fillna('Unknown').value_counts()
        return class_counts.to_dict()
    
    def _get_molecular_weight_distribution(self) -> Dict[str, int]:
        """Get molecular weight distribution buckets"""
        if 'average_molecular_weight' not in self.metabolites_df.columns:
            return {}
        
        # Create weight buckets
        weights = self.metabolites_df['average_molecular_weight'].dropna()
        if len(weights) == 0:
            return {}
        
        # Define buckets
        buckets = [0, 100, 300, 500, 1000, float('inf')]
        labels = ['<100', '100-300', '300-500', '500-1000', '>1000']
        
        weight_buckets = pd.cut(weights, bins=buckets, labels=labels, include_lowest=True)
        return weight_buckets.value_counts().to_dict()
    
    def _get_relationship_density_distribution(self, entity_type: str) -> Dict[str, int]:
        """Get relationship density distribution"""
        if entity_type == "metabolites":
            if 'protein_associations_count' not in self.metabolites_df.columns:
                return {}
            counts = self.metabolites_df['protein_associations_count'].fillna(0)
        elif entity_type == "proteins":
            if 'metabolite_associations_count' not in self.proteins_df.columns:
                return {}
            counts = self.proteins_df['metabolite_associations_count'].fillna(0)
        else:
            return {}
        
        # Create density buckets
        buckets = [0, 1, 5, 10, 20, float('inf')]
        labels = ['0', '1-5', '6-10', '11-20', '>20']
        
        density_buckets = pd.cut(counts, bins=buckets, labels=labels, include_lowest=True)
        return density_buckets.value_counts().to_dict()
    
    def _get_data_completeness_distribution(self, entity_type: str) -> Dict[str, int]:
        """Calculate data completeness distribution"""
        if entity_type == "metabolites":
            df = self.metabolites_df
            key_fields = ['name', 'chemical_formula', 'average_molecular_weight', 'smiles']
        elif entity_type == "proteins":
            df = self.proteins_df
            key_fields = ['gene_name', 'uniprot_id', 'general_function']
        else:
            return {}
        
        # Calculate completeness for each record
        completeness_scores = []
        for _, row in df.iterrows():
            present_fields = sum(1 for field in key_fields if field in row and pd.notna(row[field]) and str(row[field]).strip())
            completeness = present_fields / len(key_fields)
            completeness_scores.append(completeness)
        
        # Create completeness buckets
        buckets = [0, 0.25, 0.5, 0.75, 1.0]
        labels = ['<25%', '25-50%', '50-75%', '>75%']
        
        completeness_buckets = pd.cut(completeness_scores, bins=buckets, labels=labels, include_lowest=True)
        return completeness_buckets.value_counts().to_dict()
    
    def _get_specimen_coverage(self) -> Dict[str, int]:
        """Analyze specimen type coverage (inferred from filename patterns)"""
        # This would be enhanced with actual specimen data if available
        specimen_types = ['csf', 'feces', 'saliva', 'serum', 'sweat', 'urine', 'general']
        coverage = {specimen: 0 for specimen in specimen_types}
        
        # For now, we'll simulate based on the fact that Phase 1 processed multiple specimen files
        # In a real implementation, this would use actual specimen-specific data
        total_metabolites = len(self.metabolites_df)
        coverage['general'] = total_metabolites  # All metabolites are in general category
        
        return coverage
    
    def _get_protein_function_distribution(self) -> Dict[str, int]:
        """Get distribution of protein functions"""
        if 'general_function' not in self.proteins_df.columns:
            return {}
        
        # Extract key function categories
        functions = self.proteins_df['general_function'].fillna('Unknown')
        
        # Categorize functions
        function_categories = {}
        for func in functions:
            if pd.isna(func) or func == 'Unknown':
                category = 'Unknown'
            elif 'enzyme' in str(func).lower() or 'catalysis' in str(func).lower():
                category = 'Enzyme'
            elif 'transport' in str(func).lower() or 'carrier' in str(func).lower():
                category = 'Transport'
            elif 'binding' in str(func).lower():
                category = 'Binding'
            elif 'hydrolase' in str(func).lower():
                category = 'Hydrolase'
            else:
                category = 'Other'
            
            function_categories[category] = function_categories.get(category, 0) + 1
        
        return function_categories
    
    def _get_gene_name_coverage(self) -> Dict[str, int]:
        """Get gene name coverage statistics"""
        if 'gene_name' not in self.proteins_df.columns:
            return {"with_gene_name": 0, "without_gene_name": 0}
        
        gene_names = self.proteins_df['gene_name'].fillna('')
        with_gene = sum(1 for name in gene_names if str(name).strip())
        without_gene = len(gene_names) - with_gene
        
        return {"with_gene_name": with_gene, "without_gene_name": without_gene}
    
    def sample_metabolites(self) -> pd.DataFrame:
        """Perform stratified sampling of metabolites"""
        logger.info(f"Sampling {self.config.metabolite_sample_size} metabolites...")
        
        # Apply quality filters
        filtered_df = self._apply_metabolite_filters(self.metabolites_df)
        logger.info(f"After filtering: {len(filtered_df)} metabolites available")
        
        if len(filtered_df) <= self.config.metabolite_sample_size:
            logger.warning("Available metabolites less than requested sample size")
            return filtered_df
        
        # Reset index after filtering to ensure proper indexing
        filtered_df = filtered_df.reset_index(drop=True)
        
        # Stratified sampling
        sampled_indices = self._stratified_sample_metabolites(filtered_df)
        sampled_df = filtered_df.iloc[sampled_indices]
        
        logger.info(f"Sampled {len(sampled_df)} metabolites")
        return sampled_df
    
    def sample_proteins(self) -> pd.DataFrame:
        """Perform stratified sampling of proteins"""
        logger.info(f"Sampling {self.config.protein_sample_size} proteins...")
        
        # Apply quality filters
        filtered_df = self._apply_protein_filters(self.proteins_df)
        logger.info(f"After filtering: {len(filtered_df)} proteins available")
        
        if len(filtered_df) <= self.config.protein_sample_size:
            logger.warning("Available proteins less than requested sample size")
            return filtered_df
        
        # Reset index after filtering to ensure proper indexing
        filtered_df = filtered_df.reset_index(drop=True)
        
        # Stratified sampling
        sampled_indices = self._stratified_sample_proteins(filtered_df)
        sampled_df = filtered_df.iloc[sampled_indices]
        
        logger.info(f"Sampled {len(sampled_df)} proteins")
        return sampled_df
    
    def _apply_metabolite_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply enhanced quality filters to metabolites with better data selection"""
        filtered = df.copy()
        initial_count = len(filtered)
        
        # IMPROVED: Handle missing columns gracefully
        required_columns = ['name', 'chemical_formula', 'average_molecular_weight', 'smiles']
        missing_columns = [col for col in required_columns if col not in filtered.columns]
        if missing_columns:
            logger.warning(f"Missing metabolite columns: {missing_columns}. Adjusting filters accordingly.")
        
        # ENHANCED: More lenient name requirement (priority over strictness)
        if self.config.require_name and 'name' in filtered.columns:
            # Better NaN and empty string handling
            filtered = filtered[
                filtered['name'].notna() & 
                (filtered['name'] != '') & 
                (filtered['name'].astype(str).str.strip() != '') &
                (filtered['name'].astype(str).str.lower() != 'nan')
            ]
            logger.debug(f"After name filter: {len(filtered)}/{initial_count} metabolites remain")
        
        # RELAXED: Make chemical formula optional for diversity
        if self.config.require_formula and 'chemical_formula' in filtered.columns:
            # Create a quality boost for entities with formulas rather than requiring them
            filtered['has_formula'] = (
                filtered['chemical_formula'].notna() & 
                (filtered['chemical_formula'] != '') &
                (filtered['chemical_formula'].astype(str).str.strip() != '') &
                (filtered['chemical_formula'].astype(str).str.lower() != 'nan') &
                (filtered['chemical_formula'].astype(str).str.len() > 1)
            )
            # Don't filter out entities without formulas, just note them for scoring
            logger.debug(f"Entities with formulas: {filtered['has_formula'].sum()}/{len(filtered)}")
        
        # ENHANCED: Smarter quality scoring system
        if 'protein_associations_count' in filtered.columns:
            filtered['quality_score'] = 0
            filtered['protein_count'] = filtered['protein_associations_count'].fillna(0)
            
            # IMPROVED: Balanced scoring that encourages diversity over quantity
            # Base score for having any data
            filtered['quality_score'] += 1
            
            # Bonus for having associations (but don't over-penalize entities with zero)
            filtered.loc[filtered['protein_count'] > 0, 'quality_score'] += 1
            filtered.loc[filtered['protein_count'] >= 1, 'quality_score'] += 1  # 1+ associations
            filtered.loc[filtered['protein_count'] >= 3, 'quality_score'] += 1  # 3+ associations
            filtered.loc[filtered['protein_count'] >= 10, 'quality_score'] += 1  # 10+ associations
            # Cap at reasonable levels to avoid bias
            
            # DIVERSITY IMPROVEMENT: Don't over-penalize zero associations
            # Accept entities with zero associations for "no association" questions
            # FIX: Ensure quality_score is float type before adding float values
            filtered['quality_score'] = filtered['quality_score'].astype(float)
            filtered.loc[filtered['protein_count'] == 0, 'quality_score'] += 0.5  # Small bonus for zero-association questions
            
            # Bonus for having chemical formula
            if 'has_formula' in filtered.columns:
                filtered.loc[filtered['has_formula'], 'quality_score'] += 1
            
            # Higher score for entities with external references
            ref_fields = ['pubchem_compound_id', 'chebi_id', 'kegg_id', 'cas_registry_number']
            for field in ref_fields:
                if field in filtered.columns:
                    filtered.loc[filtered[field].notna() & (filtered[field] != '') & (filtered[field].astype(str).str.lower() != 'nan'), 'quality_score'] += 0.5
            
            # Higher score for entities with rich taxonomy
            if 'taxonomy_class' in filtered.columns:
                filtered.loc[filtered['taxonomy_class'].notna() & (filtered['taxonomy_class'] != '') & (filtered['taxonomy_class'].astype(str).str.lower() != 'nan'), 'quality_score'] += 1
            
            # Higher score for entities with molecular weight
            if 'average_molecular_weight' in filtered.columns:
                filtered.loc[filtered['average_molecular_weight'].notna() & (filtered['average_molecular_weight'] > 0), 'quality_score'] += 1
            
            # Higher score for entities with SMILES
            if 'smiles' in filtered.columns:
                filtered.loc[filtered['smiles'].notna() & (filtered['smiles'] != '') & (filtered['smiles'].astype(str).str.lower() != 'nan'), 'quality_score'] += 1
            
            # RELAXED: Keep more entities by lowering quality threshold
            quality_threshold = max(2, filtered['quality_score'].quantile(0.1))  # Keep top 90% by quality (much more inclusive)
            filtered = filtered[filtered['quality_score'] >= quality_threshold]
            logger.debug(f"After quality filter: {len(filtered)}/{initial_count} metabolites remain (quality >= {quality_threshold})")
            
            # Clean up temporary columns (only drop if they exist)
            columns_to_drop = [col for col in ['protein_count', 'has_formula'] if col in filtered.columns]
            if columns_to_drop:
                filtered = filtered.drop(columns=columns_to_drop)
        
        # RELAXED: More lenient molecular weight filtering
        if 'average_molecular_weight' in filtered.columns:
            # Don't require molecular weight - just filter out clearly invalid values
            invalid_weights = (
                filtered['average_molecular_weight'].notna() & 
                ((filtered['average_molecular_weight'] <= 0) | (filtered['average_molecular_weight'] > 50000))
            )
            filtered = filtered[~invalid_weights]
            logger.debug(f"After molecular weight filter: {len(filtered)}/{initial_count} metabolites remain")
        
        # RELAXED: Make SMILES optional
        if 'smiles' in filtered.columns:
            # Don't require SMILES - they're helpful but not essential
            # Just filter out clearly invalid SMILES
            valid_smiles_mask = (
                filtered['smiles'].isna() |  # Allow missing SMILES
                (filtered['smiles'] == '') |  # Allow empty SMILES
                (
                filtered['smiles'].notna() & 
                (filtered['smiles'] != '') &
                (filtered['smiles'].astype(str).str.strip() != '') &
                    (filtered['smiles'].astype(str).str.lower() != 'nan') &
                    (filtered['smiles'].astype(str).str.len() >= 3)  # Basic validation
                )
            )
            filtered = filtered[valid_smiles_mask]
            logger.debug(f"After SMILES filter: {len(filtered)}/{initial_count} metabolites remain")
        
        # FIXED: Prioritize metabolites WITH associations for meaningful evaluation
        if self.config.min_relationship_count > 0 and 'protein_associations_count' in filtered.columns:
            # CORRECTED STRATEGY: Prioritize entities WITH associations for evaluation
            if self.config.min_relationship_count == 1:
                # Prioritize entities with associations (80% of sample) for meaningful questions
                has_associations = filtered['protein_associations_count'].fillna(0) >= 1
                no_associations = filtered['protein_associations_count'].fillna(0) == 0
                
                # Take 80% with associations, 20% without for balanced evaluation
                with_assoc_sample = filtered[has_associations]
                without_assoc_sample = filtered[no_associations]
                
                # Calculate target sizes - prioritize entities with data
                target_with = int(len(filtered) * 0.8)  # 80% with associations
                target_without = int(len(filtered) * 0.2)  # 20% without associations
                
                # Ensure we have enough entities with associations
                if len(with_assoc_sample) >= target_with:
                    with_assoc_sample = with_assoc_sample.sample(n=min(target_with, len(with_assoc_sample)), random_state=self.config.random_seed)
                
                # Take smaller sample of entities without associations
                if len(without_assoc_sample) > target_without:
                    without_assoc_sample = without_assoc_sample.sample(n=target_without, random_state=self.config.random_seed)
                
                filtered = pd.concat([with_assoc_sample, without_assoc_sample], ignore_index=True)
                logger.info(f"CORRECTED: Prioritized {len(with_assoc_sample)} metabolites WITH associations, {len(without_assoc_sample)} without")
            else:
                # For higher minimum counts, apply as requested
                filtered = filtered[filtered['protein_associations_count'].fillna(0) >= self.config.min_relationship_count]
                logger.debug(f"After relationship filter: {len(filtered)}/{initial_count} metabolites remain")
        
        # RELAXED: Lower data completeness requirement
        if self.config.min_data_completeness > 0:
            # Calculate completeness score for core fields only
            core_fields = ['name', 'chemical_formula', 'average_molecular_weight', 'taxonomy_class']
            available_core_fields = [f for f in core_fields if f in filtered.columns]
            
            if available_core_fields:
                filtered['completeness'] = filtered[available_core_fields].notna().sum(axis=1) / len(available_core_fields)
                # RELAXED: Lower threshold for more diversity
                min_completeness = max(0.25, self.config.min_data_completeness - 0.25)  # Reduce by 25%
                filtered = filtered[filtered['completeness'] >= min_completeness]
                logger.debug(f"After completeness filter: {len(filtered)}/{initial_count} metabolites remain (completeness >= {min_completeness})")
        
        # Clean up temporary columns
        cols_to_drop = ['quality_score', 'completeness']
        filtered = filtered.drop(columns=[col for col in cols_to_drop if col in filtered.columns])
        
        retention_rate = len(filtered)/initial_count*100 if initial_count > 0 else 0
        logger.info(f"Metabolite filtering: {initial_count} → {len(filtered)} ({retention_rate:.1f}% retained)")
        
        # ALERT: If retention is still very low, warn user
        if retention_rate < 30:
            logger.warning(f"Low metabolite retention rate ({retention_rate:.1f}%). Consider relaxing quality filters.")
        
        return filtered
    
    def _apply_protein_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply enhanced quality filters to proteins with better data selection"""
        filtered = df.copy()
        initial_count = len(filtered)
        
        # IMPROVED: Handle missing columns gracefully
        required_columns = ['gene_name', 'uniprot_id', 'general_function']
        missing_columns = [col for col in required_columns if col not in filtered.columns]
        if missing_columns:
            logger.warning(f"Missing protein columns: {missing_columns}. Adjusting filters accordingly.")
        
        # ENHANCED: Prioritize proteins with richer data
        if 'protein_accession' in filtered.columns:
            filtered = filtered[
                filtered['protein_accession'].notna() & 
                (filtered['protein_accession'] != '') &
                (filtered['protein_accession'].astype(str).str.strip() != '') &
                (filtered['protein_accession'].astype(str).str.lower() != 'nan')
            ]
            logger.debug(f"After accession filter: {len(filtered)}/{initial_count} proteins remain")
        
        # ENHANCED: Prioritize proteins with metabolite associations
        if 'metabolite_associations_count' in filtered.columns:
            # Create a quality score based on associations and completeness
            filtered['quality_score'] = 0
            
            # Higher score for proteins with metabolite associations
            filtered.loc[filtered['metabolite_associations_count'].fillna(0) > 0, 'quality_score'] += 3
            filtered.loc[filtered['metabolite_associations_count'].fillna(0) > 5, 'quality_score'] += 2
            filtered.loc[filtered['metabolite_associations_count'].fillna(0) > 10, 'quality_score'] += 1
            
            # Higher score for proteins with gene names
            if 'gene_name' in filtered.columns:
                filtered.loc[filtered['gene_name'].notna() & (filtered['gene_name'] != '') & (filtered['gene_name'].astype(str).str.lower() != 'nan'), 'quality_score'] += 2
            
            # Higher score for proteins with functional annotations
            if 'general_function' in filtered.columns:
                filtered.loc[filtered['general_function'].notna() & (filtered['general_function'] != '') & (filtered['general_function'].astype(str).str.lower() != 'nan'), 'quality_score'] += 2
            
            if 'specific_function' in filtered.columns:
                filtered.loc[filtered['specific_function'].notna() & (filtered['specific_function'] != '') & (filtered['specific_function'].astype(str).str.lower() != 'nan'), 'quality_score'] += 1
            
            # Higher score for proteins with UniProt IDs
            if 'uniprot_id' in filtered.columns:
                filtered.loc[filtered['uniprot_id'].notna() & (filtered['uniprot_id'] != '') & (filtered['uniprot_id'].astype(str).str.lower() != 'nan'), 'quality_score'] += 1
            
            # PRIORITIZE: Keep proteins with higher quality scores
            quality_threshold = filtered['quality_score'].quantile(0.2)  # Keep top 80% by quality
            filtered = filtered[filtered['quality_score'] >= quality_threshold]
            logger.debug(f"After quality filter: {len(filtered)}/{initial_count} proteins remain (quality >= {quality_threshold})")
        
        # Require relationships for question generation
        if self.config.min_relationship_count > 0 and 'metabolite_associations_count' in filtered.columns:
            filtered = filtered[filtered['metabolite_associations_count'].fillna(0) >= self.config.min_relationship_count]
            logger.debug(f"After relationship filter: {len(filtered)}/{initial_count} proteins remain")
        
        # Data completeness filtering
        if self.config.min_data_completeness > 0:
            # Calculate completeness score
            total_fields = len(filtered.columns)
            filtered['completeness'] = filtered.notna().sum(axis=1) / total_fields
            filtered = filtered[filtered['completeness'] >= self.config.min_data_completeness]
            logger.debug(f"After completeness filter: {len(filtered)}/{initial_count} proteins remain")
        
        # Clean up temporary columns
        cols_to_drop = ['quality_score', 'completeness']
        filtered = filtered.drop(columns=[col for col in cols_to_drop if col in filtered.columns])
        
        logger.info(f"Protein filtering: {initial_count} → {len(filtered)} ({len(filtered)/initial_count*100:.1f}% retained)")
        return filtered
    
    def _stratified_sample_metabolites(self, df: pd.DataFrame) -> List[int]:
        """Perform stratified sampling on metabolites"""
        sample_size = min(self.config.metabolite_sample_size, len(df))
        strata = []
        
        # Chemical class stratification
        if self.config.chemical_class_stratification and 'taxonomy_class' in df.columns:
            class_strata = self._create_strata_by_column(df, 'taxonomy_class', max_strata=10)
            strata.append(class_strata)
        
        # Molecular weight stratification
        if self.config.molecular_weight_stratification and 'average_molecular_weight' in df.columns:
            weight_strata = self._create_molecular_weight_strata(df)
            strata.append(weight_strata)
        
        # Relationship density stratification
        if self.config.relationship_density_stratification and 'protein_associations_count' in df.columns:
            density_strata = self._create_relationship_density_strata(df, 'protein_associations_count')
            strata.append(density_strata)
        
        # Combine strata and sample
        if strata:
            combined_strata = self._combine_strata(strata, df.index)
            return self._sample_from_combined_strata(combined_strata, sample_size)
        else:
            # Fallback to random sampling
            return random.sample(list(df.index), sample_size)
    
    def _stratified_sample_proteins(self, df: pd.DataFrame) -> List[int]:
        """Perform stratified sampling on proteins"""
        sample_size = min(self.config.protein_sample_size, len(df))
        strata = []
        
        # Function category stratification
        if 'general_function' in df.columns:
            function_strata = self._create_strata_by_function(df)
            strata.append(function_strata)
        
        # Relationship density stratification
        if self.config.relationship_density_stratification and 'metabolite_associations_count' in df.columns:
            density_strata = self._create_relationship_density_strata(df, 'metabolite_associations_count')
            strata.append(density_strata)
        
        # Combine strata and sample
        if strata:
            combined_strata = self._combine_strata(strata, df.index)
            return self._sample_from_combined_strata(combined_strata, sample_size)
        else:
            # Fallback to random sampling
            return random.sample(list(df.index), sample_size)
    
    def _create_strata_by_column(self, df: pd.DataFrame, column: str, max_strata: int = 10) -> Dict[str, List[int]]:
        """Create strata based on column values with improved handling"""
        strata = {}
        
        if column not in df.columns:
            logger.warning(f"Column '{column}' not found in dataframe. Skipping stratification.")
            return strata
        
        # IMPROVED: Better handling of missing values
        values = df[column].fillna('Unknown')
        
        # Get value counts and limit to top categories
        value_counts = values.value_counts()
        
        # IMPROVED: Handle case where we have very few unique values
        if len(value_counts) <= max_strata:
            # Use all values if we have few enough
            for value in value_counts.index:
                mask = values == value
                indices = df[mask].index.tolist()
                if indices:  # Only add non-empty strata
                    strata[f"{column}_{value}"] = indices
                    logger.debug(f"Created stratum {column}_{value}: {len(indices)} items")
        else:
            # Use top categories and group the rest
            top_values = value_counts.head(max_strata - 1).index.tolist()
            
            # Create strata for top values
            for value in top_values:
                mask = values == value
                indices = df[mask].index.tolist()
                if indices:
                    strata[f"{column}_{value}"] = indices
                    logger.debug(f"Created stratum {column}_{value}: {len(indices)} items")
        
        # Add "Other" category for remaining values
            mask = ~values.isin(top_values)
            other_indices = df[mask].index.tolist()
            if other_indices:
                strata[f"{column}_Other"] = other_indices
                logger.debug(f"Created stratum {column}_Other: {len(other_indices)} items")
        
        return strata
    
    def _create_molecular_weight_strata(self, df: pd.DataFrame) -> Dict[str, List[int]]:
        """Create molecular weight-based strata"""
        if 'average_molecular_weight' not in df.columns:
            return {}
        
        weights = df['average_molecular_weight'].dropna()
        if len(weights) == 0:
            return {}
        
        # Define weight buckets
        buckets = [0, 100, 300, 500, 1000, float('inf')]
        labels = ['<100', '100-300', '300-500', '500-1000', '>1000']
        
        strata = {}
        for i, label in enumerate(labels):
            min_weight = buckets[i]
            max_weight = buckets[i + 1]
            
            if max_weight == float('inf'):
                mask = df['average_molecular_weight'] >= min_weight
            else:
                mask = (df['average_molecular_weight'] >= min_weight) & (df['average_molecular_weight'] < max_weight)
            
            strata[f"weight_{label}"] = df[mask].index.tolist()
        
        return strata
    
    def _create_relationship_density_strata(self, df: pd.DataFrame, count_column: str) -> Dict[str, List[int]]:
        """Create relationship density-based strata"""
        if count_column not in df.columns:
            return {}
        
        counts = df[count_column].fillna(0)
        
        # Define density buckets
        buckets = [0, 1, 5, 10, 20, float('inf')]
        labels = ['0', '1-5', '6-10', '11-20', '>20']
        
        strata = {}
        for i, label in enumerate(labels):
            min_count = buckets[i]
            max_count = buckets[i + 1]
            
            if max_count == float('inf'):
                mask = counts >= min_count
            else:
                mask = (counts >= min_count) & (counts < max_count)
            
            strata[f"density_{label}"] = df[mask].index.tolist()
        
        return strata
    
    def _create_strata_by_function(self, df: pd.DataFrame) -> Dict[str, List[int]]:
        """Create function-based strata for proteins"""
        if 'general_function' not in df.columns:
            return {}
        
        strata = {}
        functions = df['general_function'].fillna('Unknown')
        
        # Categorize functions
        function_categories = {
            'Enzyme': ['enzyme', 'catalysis', 'catalyze'],
            'Transport': ['transport', 'carrier', 'channel'],
            'Binding': ['binding', 'bind'],
            'Hydrolase': ['hydrolase', 'hydrolyze'],
            'Unknown': ['unknown', '']
        }
        
        for category, keywords in function_categories.items():
            indices = []
            for idx, func in functions.items():
                func_str = str(func).lower()
                if any(keyword in func_str for keyword in keywords):
                    indices.append(idx)
            
            if indices:
                strata[f"function_{category}"] = indices
        
        # Add "Other" for functions not categorized
        categorized_indices = set()
        for indices in strata.values():
            categorized_indices.update(indices)
        
        other_indices = [idx for idx in df.index if idx not in categorized_indices]
        if other_indices:
            strata["function_Other"] = other_indices
        
        return strata
    
    def _combine_strata(self, strata_list: List[Dict[str, List[int]]], all_indices: pd.Index) -> Dict[str, List[int]]:
        """Combine multiple stratification schemes"""
        if not strata_list:
            return {"all": list(all_indices)}
        
        if len(strata_list) == 1:
            return strata_list[0]
        
        # Create intersection of strata
        combined_strata = {}
        
        # Get all combinations of strata keys
        strata_keys = [list(strata.keys()) for strata in strata_list]
        
        import itertools
        for key_combination in itertools.product(*strata_keys):
            # Find intersection of indices for this combination
            indices_sets = []
            for i, key in enumerate(key_combination):
                indices_sets.append(set(strata_list[i][key]))
            
            intersection = set.intersection(*indices_sets) if indices_sets else set()
            
            if intersection:
                combined_key = "_".join(key_combination)
                combined_strata[combined_key] = list(intersection)
        
        return combined_strata
    
    def _sample_from_combined_strata(self, combined_strata: Dict[str, List[int]], sample_size: int) -> List[int]:
        """Sample from combined strata with improved handling of small strata"""
        if not combined_strata:
            logger.warning("No strata available for sampling. Using random sampling.")
            return []
        
        # IMPROVED: Calculate target samples per stratum with minimum guarantees
        total_items = sum(len(indices) for indices in combined_strata.values())
        min_samples_per_stratum = max(1, sample_size // (len(combined_strata) * 3))  # Minimum samples per stratum
        
        selected_indices = []
        remaining_sample_size = sample_size
        
        # Sort strata by size (largest first) to ensure better coverage
        sorted_strata = sorted(combined_strata.items(), key=lambda x: len(x[1]), reverse=True)
        
        for stratum_name, indices in sorted_strata:
            stratum_size = len(indices)
            
            if stratum_size == 0:
                logger.debug(f"Skipping empty stratum: {stratum_name}")
                continue
            
            # IMPROVED: Calculate proportional sample size with minimum guarantee
            proportional_size = max(
                min_samples_per_stratum,
                int((stratum_size / total_items) * sample_size)
            )
            
            # Don't sample more than what's available or what we need
            actual_sample_size = min(
                proportional_size,
                stratum_size,
                remaining_sample_size
            )
            
            if actual_sample_size > 0:
                # Handle small strata gracefully
                if stratum_size <= actual_sample_size:
                    # Take all items from small strata
                    stratum_sample = indices.copy()
                    logger.debug(f"Taking all {stratum_size} items from small stratum: {stratum_name}")
                else:
                    # Random sample from larger strata
                    stratum_sample = random.sample(indices, actual_sample_size)
                    logger.debug(f"Sampled {actual_sample_size}/{stratum_size} from stratum: {stratum_name}")
                
                selected_indices.extend(stratum_sample)
                remaining_sample_size -= len(stratum_sample)
                
                if remaining_sample_size <= 0:
                    break
        
        # IMPROVED: If we still need more samples and have remaining capacity
        if remaining_sample_size > 0 and len(selected_indices) < sample_size:
            # Get all unselected indices
            all_available = set()
            for indices in combined_strata.values():
                all_available.update(indices)
            
            unselected = list(all_available - set(selected_indices))
            
            if unselected:
                additional_needed = min(remaining_sample_size, len(unselected))
                additional_sample = random.sample(unselected, additional_needed)
                selected_indices.extend(additional_sample)
                logger.debug(f"Added {additional_needed} additional samples to reach target size")
        
        logger.info(f"Stratified sampling completed: {len(selected_indices)} samples from {len(combined_strata)} strata")
        
        # Log sampling distribution
        for stratum_name, indices in sorted_strata:
            stratum_selected = len([idx for idx in selected_indices if idx in indices])
            if stratum_selected > 0:
                logger.debug(f"  {stratum_name}: {stratum_selected}/{len(indices)} selected")
        
        return selected_indices[:sample_size]  # Ensure we don't exceed target
    
    def get_sampling_report(self) -> Dict[str, Any]:
        """Generate comprehensive sampling report"""
        return {
            "config": {
                "metabolite_sample_size": self.config.metabolite_sample_size,
                "protein_sample_size": self.config.protein_sample_size,
                "random_seed": self.config.random_seed,
                "stratification_enabled": {
                    "chemical_class": self.config.chemical_class_stratification,
                    "specimen_type": self.config.specimen_type_stratification,
                    "relationship_density": self.config.relationship_density_stratification,
                    "molecular_weight": self.config.molecular_weight_stratification
                }
            },
            "data_distribution": self.sampling_stats,
            "generated_at": pd.Timestamp.now().isoformat()
        } 