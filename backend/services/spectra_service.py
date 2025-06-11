"""
Spectra Service Module

This module provides comprehensive functionality for handling spectral data from HMDB API.
It includes data validation, normalization, and structured output for graph generation and LLM reasoning.

Author: Senior Engineering Implementation
Version: 1.0.0
"""

from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import logging
from datetime import datetime

# Configure logging
logger = logging.getLogger(__name__)

class SpectrumType(Enum):
    """Enumeration of supported spectrum types"""
    GC_MS = "Experimental GC-MS"
    LC_MS = "Experimental LC-MS"
    NMR_1H = "Experimental 1H NMR"
    NMR_13C = "Experimental 13C NMR"
    MS_MS = "Experimental MS/MS"
    UNKNOWN = "Unknown"

class InstrumentType(Enum):
    """Enumeration of supported instrument types"""
    GC_MS = "GC-MS"
    LC_MS = "LC-MS"
    NMR = "NMR"
    MS = "MS"
    UNKNOWN = "Unknown"

@dataclass
class SpectralPeak:
    """
    Data structure for individual spectral peaks
    
    Attributes:
        mass_charge: m/z value for MS data or ppm for NMR
        intensity: Relative intensity (normalized 0-1)
        annotation: Optional peak annotation
    """
    mass_charge: float
    intensity: float
    annotation: Optional[str] = None
    
    def __post_init__(self):
        """Validate peak data after initialization"""
        if self.mass_charge < 0:
            raise ValueError(f"Invalid mass_charge: {self.mass_charge}. Must be >= 0")
        if not (0 <= self.intensity <= 1):
            logger.warning(f"Intensity {self.intensity} outside normal range [0,1]")

@dataclass
class SpectrumMetadata:
    """
    Metadata for spectral data
    
    Attributes:
        spectrum_type: Type of spectrum (GC-MS, LC-MS, etc.)
        instrument_type: Instrument used for acquisition
        chromatography_type: Chromatography method if applicable
        sample_concentration: Sample concentration
        solvent: Solvent used
        temperature: Acquisition temperature
        ph: Sample pH
        splash_key: SPLASH spectrum identifier
        acquisition_date: When spectrum was acquired
        notes: Additional notes
    """
    spectrum_type: SpectrumType = SpectrumType.UNKNOWN
    instrument_type: InstrumentType = InstrumentType.UNKNOWN
    chromatography_type: Optional[str] = None
    sample_concentration: Optional[str] = None
    solvent: Optional[str] = None
    temperature: Optional[str] = None
    ph: Optional[float] = None
    splash_key: Optional[str] = None
    acquisition_date: Optional[datetime] = None
    notes: Optional[str] = None

@dataclass
class ProcessedSpectrum:
    """
    Complete processed spectrum data structure
    
    Attributes:
        hmdb_id: HMDB identifier
        peaks: List of spectral peaks
        metadata: Spectrum metadata
        quality_score: Data quality assessment (0-1)
        processing_info: Information about data processing
        raw_data: Original raw data for reference
    """
    hmdb_id: str
    peaks: List[SpectralPeak]
    metadata: SpectrumMetadata
    quality_score: float = 0.0
    processing_info: Dict[str, Any] = field(default_factory=dict)
    raw_data: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        """Validate processed spectrum data"""
        if not self.hmdb_id.startswith('HMDB'):
            raise ValueError(f"Invalid HMDB ID format: {self.hmdb_id}")
        if not (0 <= self.quality_score <= 1):
            raise ValueError(f"Quality score must be between 0 and 1: {self.quality_score}")

class SpectraValidator:
    """Utility class for validating spectral data"""
    
    @staticmethod
    def validate_raw_response(response: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate raw HMDB API response for spectra data
        
        Args:
            response: Raw API response dictionary
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        if not isinstance(response, dict):
            errors.append("Response is not a dictionary")
            return False, errors
            
        # Check for basic structure
        if not response:
            errors.append("Empty response")
            return False, errors
            
        # Check for peaks data
        peaks = response.get('peaks', [])
        if not isinstance(peaks, list):
            errors.append("Peaks data is not a list")
        elif len(peaks) == 0:
            errors.append("No peaks data found")
        else:
            # Validate individual peaks
            for i, peak in enumerate(peaks):
                if not isinstance(peak, dict):
                    errors.append(f"Peak {i} is not a dictionary")
                    continue
                    
                if 'mass_charge' not in peak:
                    errors.append(f"Peak {i} missing mass_charge")
                elif not isinstance(peak['mass_charge'], (int, float, str)):
                    errors.append(f"Peak {i} has invalid mass_charge type")
                    
                if 'intensity' not in peak:
                    errors.append(f"Peak {i} missing intensity")
                elif not isinstance(peak['intensity'], (int, float, str)):
                    errors.append(f"Peak {i} has invalid intensity type")
        
        return len(errors) == 0, errors
    
    @staticmethod
    def normalize_peaks(raw_peaks: List[Dict[str, Any]]) -> List[SpectralPeak]:
        """
        Normalize and validate peak data
        
        Args:
            raw_peaks: List of raw peak dictionaries
            
        Returns:
            List of validated SpectralPeak objects
        """
        normalized_peaks = []
        max_intensity = 0
        
        # First pass: find maximum intensity for normalization
        for peak in raw_peaks:
            try:
                intensity = float(peak.get('intensity', 0))
                max_intensity = max(max_intensity, intensity)
            except (ValueError, TypeError):
                continue
        
        # Second pass: normalize and create SpectralPeak objects
        for peak in raw_peaks:
            try:
                mass_charge = float(peak.get('mass_charge', 0))
                raw_intensity = float(peak.get('intensity', 0))
                
                # Normalize intensity to 0-1 range
                normalized_intensity = raw_intensity / max_intensity if max_intensity > 0 else 0
                
                annotation = peak.get('annotation', None)
                
                spectral_peak = SpectralPeak(
                    mass_charge=mass_charge,
                    intensity=normalized_intensity,
                    annotation=annotation
                )
                normalized_peaks.append(spectral_peak)
                
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid peak data: {peak}. Error: {e}")
                continue
        
        return normalized_peaks

class SpectraProcessor:
    """Main class for processing spectral data"""
    
    @staticmethod
    def parse_spectrum_type(raw_type: str) -> SpectrumType:
        """Parse spectrum type from raw string"""
        if not raw_type:
            return SpectrumType.UNKNOWN
            
        raw_type_lower = raw_type.lower()
        if 'gc-ms' in raw_type_lower or 'gc ms' in raw_type_lower:
            return SpectrumType.GC_MS
        elif 'lc-ms' in raw_type_lower or 'lc ms' in raw_type_lower:
            return SpectrumType.LC_MS
        elif '1h nmr' in raw_type_lower:
            return SpectrumType.NMR_1H
        elif '13c nmr' in raw_type_lower:
            return SpectrumType.NMR_13C
        elif 'ms/ms' in raw_type_lower or 'msms' in raw_type_lower:
            return SpectrumType.MS_MS
        else:
            return SpectrumType.UNKNOWN
    
    @staticmethod
    def parse_instrument_type(raw_instrument: str) -> InstrumentType:
        """Parse instrument type from raw string"""
        if not raw_instrument:
            return InstrumentType.UNKNOWN
            
        raw_instrument_lower = raw_instrument.lower()
        if 'gc-ms' in raw_instrument_lower:
            return InstrumentType.GC_MS
        elif 'lc-ms' in raw_instrument_lower:
            return InstrumentType.LC_MS
        elif 'nmr' in raw_instrument_lower:
            return InstrumentType.NMR
        elif 'ms' in raw_instrument_lower:
            return InstrumentType.MS
        else:
            return InstrumentType.UNKNOWN
    
    @staticmethod
    def calculate_quality_score(peaks: List[SpectralPeak], metadata: SpectrumMetadata) -> float:
        """
        Calculate quality score based on data completeness and validity
        
        Args:
            peaks: List of spectral peaks
            metadata: Spectrum metadata
            
        Returns:
            Quality score between 0 and 1
        """
        score = 0.0
        
        # Peak data quality (40% of total score)
        if peaks:
            peak_score = min(len(peaks) / 50, 1.0)  # Up to 50 peaks for full score
            intensity_range = max(peak.intensity for peak in peaks) - min(peak.intensity for peak in peaks)
            dynamic_range_score = min(intensity_range, 1.0)
            score += 0.4 * (peak_score * 0.7 + dynamic_range_score * 0.3)
        
        # Metadata completeness (60% of total score)
        metadata_fields = [
            metadata.spectrum_type != SpectrumType.UNKNOWN,
            metadata.instrument_type != InstrumentType.UNKNOWN,
            metadata.sample_concentration is not None,
            metadata.solvent is not None,
            metadata.temperature is not None,
            metadata.chromatography_type is not None
        ]
        
        metadata_score = sum(metadata_fields) / len(metadata_fields)
        score += 0.6 * metadata_score
        
        return round(score, 3)
    
    @classmethod
    def process_raw_spectrum(cls, hmdb_id: str, raw_data: Dict[str, Any]) -> Optional[ProcessedSpectrum]:
        """
        Process raw spectrum data into structured format
        
        Args:
            hmdb_id: HMDB identifier
            raw_data: Raw spectrum data from API
            
        Returns:
            ProcessedSpectrum object or None if processing fails
        """
        try:
            # Validate input data
            is_valid, errors = SpectraValidator.validate_raw_response(raw_data)
            if not is_valid:
                logger.error(f"Validation failed for {hmdb_id}: {errors}")
                return None
            
            # Process peaks
            raw_peaks = raw_data.get('peaks', [])
            processed_peaks = SpectraValidator.normalize_peaks(raw_peaks)
            
            if not processed_peaks:
                logger.warning(f"No valid peaks found for {hmdb_id}")
                return None
            
            # Process metadata
            metadata = SpectrumMetadata(
                spectrum_type=cls.parse_spectrum_type(raw_data.get('spectrum_type', '')),
                instrument_type=cls.parse_instrument_type(raw_data.get('instrument_type', '')),
                chromatography_type=raw_data.get('chromatography_type'),
                sample_concentration=raw_data.get('sample_concentration'),
                solvent=raw_data.get('solvent'),
                temperature=raw_data.get('sample_temperature'),
                ph=raw_data.get('sample_ph'),
                splash_key=raw_data.get('splash_key'),
                notes=raw_data.get('notes')
            )
            
            # Calculate quality score
            quality_score = cls.calculate_quality_score(processed_peaks, metadata)
            
            # Create processing info
            processing_info = {
                'processed_at': datetime.now().isoformat(),
                'num_peaks': len(processed_peaks),
                'peak_range': {
                    'min_mz': min(peak.mass_charge for peak in processed_peaks),
                    'max_mz': max(peak.mass_charge for peak in processed_peaks)
                },
                'validation_errors': errors if errors else None
            }
            
            return ProcessedSpectrum(
                hmdb_id=hmdb_id,
                peaks=processed_peaks,
                metadata=metadata,
                quality_score=quality_score,
                processing_info=processing_info,
                raw_data=raw_data
            )
            
        except Exception as e:
            logger.error(f"Error processing spectrum for {hmdb_id}: {e}")
            return None

    @staticmethod
    def format_for_graph_generation(spectrum: ProcessedSpectrum) -> Dict[str, Any]:
        """
        Format processed spectrum for graph generation
        
        Args:
            spectrum: ProcessedSpectrum object
            
        Returns:
            Dictionary formatted for graph plotting
        """
        return {
            'hmdb_id': spectrum.hmdb_id,
            'title': f"{spectrum.metadata.spectrum_type.value} - {spectrum.hmdb_id}",
            'x_data': [peak.mass_charge for peak in spectrum.peaks],
            'y_data': [peak.intensity for peak in spectrum.peaks],
            'x_label': 'm/z' if 'MS' in spectrum.metadata.spectrum_type.value else 'ppm',
            'y_label': 'Relative Intensity',
            'metadata': {
                'instrument': spectrum.metadata.instrument_type.value,
                'quality_score': spectrum.quality_score,
                'num_peaks': len(spectrum.peaks),
                'chromatography': spectrum.metadata.chromatography_type
            }
        }
    
    @staticmethod
    def format_for_llm_reasoning(spectrum: ProcessedSpectrum) -> Dict[str, Any]:
        """
        Format processed spectrum for LLM analysis and reasoning
        
        Args:
            spectrum: ProcessedSpectrum object
            
        Returns:
            Dictionary formatted for LLM consumption
        """
        # Get top 10 most intense peaks for LLM analysis
        top_peaks = sorted(spectrum.peaks, key=lambda x: x.intensity, reverse=True)[:10]
        
        return {
            'hmdb_id': spectrum.hmdb_id,
            'spectrum_summary': {
                'type': spectrum.metadata.spectrum_type.value,
                'instrument': spectrum.metadata.instrument_type.value,
                'chromatography': spectrum.metadata.chromatography_type,
                'quality_score': spectrum.quality_score,
                'total_peaks': len(spectrum.peaks)
            },
            'key_peaks': [
                {
                    'mz': peak.mass_charge,
                    'intensity': round(peak.intensity, 4),
                    'rank': i + 1,
                    'annotation': peak.annotation
                }
                for i, peak in enumerate(top_peaks)
            ],
            'experimental_conditions': {
                'solvent': spectrum.metadata.solvent,
                'concentration': spectrum.metadata.sample_concentration,
                'temperature': spectrum.metadata.temperature,
                'ph': spectrum.metadata.ph
            },
            'interpretation_hints': SpectraProcessor._generate_interpretation_hints(spectrum)
        }
    
    @staticmethod
    def _generate_interpretation_hints(spectrum: ProcessedSpectrum) -> List[str]:
        """Generate interpretation hints for LLM reasoning"""
        hints = []
        
        if spectrum.metadata.spectrum_type == SpectrumType.GC_MS:
            hints.append("GC-MS spectrum shows molecular fragmentation patterns useful for structural identification")
            if any(peak.mass_charge > 200 for peak in spectrum.peaks):
                hints.append("High m/z peaks may indicate molecular ion or large fragments")
        
        elif spectrum.metadata.spectrum_type == SpectrumType.LC_MS:
            hints.append("LC-MS spectrum provides molecular weight and adduct information")
        
        elif spectrum.metadata.spectrum_type in [SpectrumType.NMR_1H, SpectrumType.NMR_13C]:
            hints.append("NMR spectrum reveals structural and connectivity information")
        
        # Quality-based hints
        if spectrum.quality_score > 0.8:
            hints.append("High-quality spectrum suitable for detailed structural analysis")
        elif spectrum.quality_score < 0.5:
            hints.append("Limited quality spectrum - use with caution for identification")
        
        return hints