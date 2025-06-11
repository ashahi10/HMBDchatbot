"""
Phase 4: Spectra Graph Generation Service

This module provides comprehensive graph generation capabilities for spectral data visualization.
Supports multiple output formats including interactive HTML plots, static images, and embedded charts.

Key Features:
- Interactive spectra plotting with zoom and pan
- Professional scientific visualization standards
- Multiple output formats (HTML, PNG, SVG, PDF)
- Customizable styling and annotations
- Peak labeling and identification
- Multi-spectrum overlays for comparison
- Exportable data formats

Author: Senior Engineering Implementation
Version: 1.0.0
Phase: 4
"""

import io
import base64
import os
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import json
from datetime import datetime

# Core plotting libraries
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.backends.backend_pdf import PdfPages
import seaborn as sns

# Interactive plotting
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    import plotly.io as pio
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    logging.warning("Plotly not available - only matplotlib plots will be supported")

# Scientific analysis
import numpy as np
from scipy import signal
from scipy.stats import gaussian_kde

# Import our data structures
from backend.services.spectra_service import ProcessedSpectrum, SpectrumType, InstrumentType

# Configure logging
logger = logging.getLogger(__name__)

class PlotFormat(Enum):
    """Supported plot output formats"""
    INTERACTIVE_HTML = "interactive_html"
    STATIC_PNG = "static_png"
    STATIC_SVG = "static_svg"
    STATIC_PDF = "static_pdf"
    EMBEDDED_BASE64 = "embedded_base64"
    JSON_DATA = "json_data"

class PlotStyle(Enum):
    """Available plot styling themes"""
    SCIENTIFIC = "scientific"
    DARK_MODE = "dark_mode"
    PUBLICATION = "publication"
    COLORFUL = "colorful"
    MINIMAL = "minimal"

@dataclass
class GraphConfig:
    """Configuration for graph generation"""
    format: PlotFormat = PlotFormat.INTERACTIVE_HTML
    style: PlotStyle = PlotStyle.SCIENTIFIC
    width: int = 1200
    height: int = 600
    dpi: int = 300
    show_peaks: bool = True
    annotate_top_peaks: int = 10
    peak_threshold: float = 0.1
    smooth_spectrum: bool = False
    show_grid: bool = True
    show_legend: bool = True
    title_font_size: int = 16
    axis_font_size: int = 12
    custom_colors: Optional[Dict[str, str]] = None

@dataclass
class GeneratedGraph:
    """Container for generated graph data"""
    content: Union[str, bytes]
    format: PlotFormat
    metadata: Dict[str, Any]
    file_path: Optional[str] = None
    base64_encoded: Optional[str] = None
    
    def save_to_file(self, file_path: str) -> bool:
        """Save graph content to file"""
        try:
            mode = 'wb' if isinstance(self.content, bytes) else 'w'
            with open(file_path, mode) as f:
                f.write(self.content)
            self.file_path = file_path
            return True
        except Exception as e:
            logger.error(f"Failed to save graph to {file_path}: {e}")
            return False

class SpectraGraphGenerator:
    """Main class for generating spectral graphs"""
    
    def __init__(self, config: Optional[GraphConfig] = None):
        """
        Initialize the graph generator
        
        Args:
            config: Graph configuration settings
        """
        self.config = config or GraphConfig()
        self._setup_matplotlib_style()
        
        # Color schemes for different styles
        self.color_schemes = {
            PlotStyle.SCIENTIFIC: {
                'primary': '#1f77b4',
                'secondary': '#ff7f0e',
                'background': '#ffffff',
                'grid': '#cccccc',
                'text': '#000000'
            },
            PlotStyle.DARK_MODE: {
                'primary': '#00d4ff',
                'secondary': '#ff6b6b',
                'background': '#1e1e1e',
                'grid': '#444444',
                'text': '#ffffff'
            },
            PlotStyle.PUBLICATION: {
                'primary': '#000000',
                'secondary': '#666666',
                'background': '#ffffff',
                'grid': '#e0e0e0',
                'text': '#000000'
            },
            PlotStyle.COLORFUL: {
                'primary': '#e74c3c',
                'secondary': '#3498db',
                'background': '#ffffff',
                'grid': '#ecf0f1',
                'text': '#2c3e50'
            },
            PlotStyle.MINIMAL: {
                'primary': '#555555',
                'secondary': '#888888',
                'background': '#ffffff',
                'grid': '#f5f5f5',
                'text': '#333333'
            }
        }
    
    def _setup_matplotlib_style(self):
        """Configure matplotlib for high-quality scientific plots"""
        plt.style.use('default')
        sns.set_palette("husl")
        
        # High-quality settings
        plt.rcParams['figure.dpi'] = self.config.dpi
        plt.rcParams['savefig.dpi'] = self.config.dpi
        plt.rcParams['font.size'] = self.config.axis_font_size
        plt.rcParams['axes.labelsize'] = self.config.axis_font_size
        plt.rcParams['xtick.labelsize'] = self.config.axis_font_size - 2
        plt.rcParams['ytick.labelsize'] = self.config.axis_font_size - 2
        plt.rcParams['legend.fontsize'] = self.config.axis_font_size - 2
        plt.rcParams['axes.titlesize'] = self.config.title_font_size
        
    def generate_spectrum_plot(self, spectrum: ProcessedSpectrum, 
                             config: Optional[GraphConfig] = None) -> GeneratedGraph:
        """
        Generate a single spectrum plot
        
        Args:
            spectrum: ProcessedSpectrum object
            config: Optional custom configuration
            
        Returns:
            GeneratedGraph object containing the plot
        """
        if config:
            self.config = config
            
        if self.config.format == PlotFormat.INTERACTIVE_HTML and PLOTLY_AVAILABLE:
            return self._generate_interactive_plot(spectrum)
        else:
            return self._generate_static_plot(spectrum)
    
    def generate_comparison_plot(self, spectra: List[ProcessedSpectrum],
                               config: Optional[GraphConfig] = None) -> GeneratedGraph:
        """
        Generate a comparison plot with multiple spectra
        
        Args:
            spectra: List of ProcessedSpectrum objects
            config: Optional custom configuration
            
        Returns:
            GeneratedGraph object containing the comparison plot
        """
        if config:
            self.config = config
            
        if self.config.format == PlotFormat.INTERACTIVE_HTML and PLOTLY_AVAILABLE:
            return self._generate_interactive_comparison(spectra)
        else:
            return self._generate_static_comparison(spectra)
    
    def _generate_interactive_plot(self, spectrum: ProcessedSpectrum) -> GeneratedGraph:
        """Generate interactive Plotly-based spectrum plot"""
        if not PLOTLY_AVAILABLE:
            raise ImportError("Plotly is required for interactive plots")
        
        # Extract data
        x_data = [peak.mass_charge for peak in spectrum.peaks]
        y_data = [peak.intensity for peak in spectrum.peaks]
        
        # Apply smoothing if requested
        if self.config.smooth_spectrum and len(x_data) > 3:
            x_smooth, y_smooth = self._smooth_spectrum(x_data, y_data)
        else:
            x_smooth, y_smooth = x_data, y_data
        
        # Create figure
        fig = go.Figure()
        
        # Add main spectrum trace
        colors = self.color_schemes[self.config.style]
        
        fig.add_trace(go.Scatter(
            x=x_smooth,
            y=y_smooth,
            mode='lines+markers',
            name=f"{spectrum.hmdb_id} - {spectrum.metadata.spectrum_type.value}",
            line=dict(color=colors['primary'], width=2),
            marker=dict(color=colors['primary'], size=4),
            hovertemplate='<b>m/z:</b> %{x:.3f}<br><b>Intensity:</b> %{y:.4f}<extra></extra>'
        ))
        
        # Add peak annotations for top peaks
        if self.config.show_peaks and self.config.annotate_top_peaks > 0:
            top_peaks = sorted(spectrum.peaks, key=lambda p: p.intensity, reverse=True)
            top_peaks = top_peaks[:self.config.annotate_top_peaks]
            
            for peak in top_peaks:
                if peak.intensity >= self.config.peak_threshold:
                    fig.add_annotation(
                        x=peak.mass_charge,
                        y=peak.intensity,
                        text=f"{peak.mass_charge:.1f}",
                        showarrow=True,
                        arrowhead=2,
                        arrowsize=1,
                        arrowwidth=1,
                        arrowcolor=colors['secondary'],
                        ax=0,
                        ay=-30,
                        font=dict(size=10, color=colors['text'])
                    )
        
        # Customize layout
        x_label = 'm/z' if 'MS' in spectrum.metadata.spectrum_type.value else 'Chemical Shift (ppm)'
        
        fig.update_layout(
            title={
                'text': f"{spectrum.metadata.spectrum_type.value} Spectrum - {spectrum.hmdb_id}",
                'x': 0.5,
                'font': {'size': self.config.title_font_size, 'color': colors['text']}
            },
            xaxis_title=x_label,
            yaxis_title='Relative Intensity',
            width=self.config.width,
            height=self.config.height,
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font=dict(color=colors['text']),
            showlegend=self.config.show_legend,
            hovermode='closest'
        )
        
        # Grid configuration
        if self.config.show_grid:
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor=colors['grid'])
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=colors['grid'])
        
        # Generate output
        if self.config.format == PlotFormat.INTERACTIVE_HTML:
            html_content = pio.to_html(fig, include_plotlyjs=True, div_id="spectrum-plot")
            metadata = {
                'spectrum_id': spectrum.hmdb_id,
                'plot_type': 'interactive_single',
                'peaks_count': len(spectrum.peaks),
                'quality_score': spectrum.quality_score,
                'generated_at': datetime.now().isoformat()
            }
            return GeneratedGraph(
                content=html_content,
                format=PlotFormat.INTERACTIVE_HTML,
                metadata=metadata
            )
        
        elif self.config.format == PlotFormat.EMBEDDED_BASE64:
            img_bytes = pio.to_image(fig, format='png', width=self.config.width, height=self.config.height)
            base64_string = base64.b64encode(img_bytes).decode('utf-8')
            metadata = {
                'spectrum_id': spectrum.hmdb_id,
                'plot_type': 'embedded_single',
                'peaks_count': len(spectrum.peaks),
                'quality_score': spectrum.quality_score,
                'generated_at': datetime.now().isoformat()
            }
            return GeneratedGraph(
                content=img_bytes,
                format=PlotFormat.EMBEDDED_BASE64,
                metadata=metadata,
                base64_encoded=base64_string
            )
    
    def _generate_static_plot(self, spectrum: ProcessedSpectrum) -> GeneratedGraph:
        """Generate static matplotlib-based spectrum plot"""
        
        # Set up the plot
        fig, ax = plt.subplots(figsize=(self.config.width/100, self.config.height/100))
        
        colors = self.color_schemes[self.config.style]
        
        # Set background color
        fig.patch.set_facecolor(colors['background'])
        ax.set_facecolor(colors['background'])
        
        # Extract and plot data
        x_data = [peak.mass_charge for peak in spectrum.peaks]
        y_data = [peak.intensity for peak in spectrum.peaks]
        
        # Apply smoothing if requested
        if self.config.smooth_spectrum and len(x_data) > 3:
            x_smooth, y_smooth = self._smooth_spectrum(x_data, y_data)
        else:
            x_smooth, y_smooth = x_data, y_data
        
        # Plot spectrum
        ax.plot(x_smooth, y_smooth, color=colors['primary'], linewidth=2, marker='o', markersize=3)
        
        # Add peak annotations
        if self.config.show_peaks and self.config.annotate_top_peaks > 0:
            top_peaks = sorted(spectrum.peaks, key=lambda p: p.intensity, reverse=True)
            top_peaks = top_peaks[:self.config.annotate_top_peaks]
            
            for peak in top_peaks:
                if peak.intensity >= self.config.peak_threshold:
                    ax.annotate(f'{peak.mass_charge:.1f}', 
                              xy=(peak.mass_charge, peak.intensity),
                              xytext=(peak.mass_charge, peak.intensity + 0.05),
                              ha='center', va='bottom',
                              fontsize=8,
                              color=colors['text'],
                              arrowprops=dict(arrowstyle='->', color=colors['secondary'], lw=1))
        
        # Customize plot
        x_label = 'm/z' if 'MS' in spectrum.metadata.spectrum_type.value else 'Chemical Shift (ppm)'
        ax.set_xlabel(x_label, color=colors['text'], fontsize=self.config.axis_font_size)
        ax.set_ylabel('Relative Intensity', color=colors['text'], fontsize=self.config.axis_font_size)
        ax.set_title(f"{spectrum.metadata.spectrum_type.value} Spectrum - {spectrum.hmdb_id}",
                    color=colors['text'], fontsize=self.config.title_font_size, pad=20)
        
        # Grid
        if self.config.show_grid:
            ax.grid(True, color=colors['grid'], alpha=0.7, linestyle='-', linewidth=0.5)
        
        # Style axes
        ax.tick_params(colors=colors['text'])
        for spine in ax.spines.values():
            spine.set_color(colors['text'])
        
        # Generate output based on format
        if self.config.format == PlotFormat.STATIC_PNG:
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=self.config.dpi, bbox_inches='tight',
                       facecolor=colors['background'], edgecolor='none')
            buffer.seek(0)
            content = buffer.getvalue()
            plt.close(fig)
            
        elif self.config.format == PlotFormat.STATIC_SVG:
            buffer = io.BytesIO()
            plt.savefig(buffer, format='svg', dpi=self.config.dpi, bbox_inches='tight',
                       facecolor=colors['background'], edgecolor='none')
            buffer.seek(0)
            content = buffer.getvalue()
            plt.close(fig)
            
        elif self.config.format == PlotFormat.STATIC_PDF:
            buffer = io.BytesIO()
            plt.savefig(buffer, format='pdf', dpi=self.config.dpi, bbox_inches='tight',
                       facecolor=colors['background'], edgecolor='none')
            buffer.seek(0)
            content = buffer.getvalue()
            plt.close(fig)
            
        elif self.config.format == PlotFormat.EMBEDDED_BASE64:
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=self.config.dpi, bbox_inches='tight',
                       facecolor=colors['background'], edgecolor='none')
            buffer.seek(0)
            img_bytes = buffer.getvalue()
            base64_string = base64.b64encode(img_bytes).decode('utf-8')
            plt.close(fig)
            
            metadata = {
                'spectrum_id': spectrum.hmdb_id,
                'plot_type': 'embedded_single',
                'peaks_count': len(spectrum.peaks),
                'quality_score': spectrum.quality_score,
                'generated_at': datetime.now().isoformat(),
                'format': self.config.format.value
            }
            
            return GeneratedGraph(
                content=img_bytes,
                format=PlotFormat.EMBEDDED_BASE64,
                metadata=metadata,
                base64_encoded=base64_string
            )
            
        else:  # Default to PNG
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=self.config.dpi, bbox_inches='tight',
                       facecolor=colors['background'], edgecolor='none')
            buffer.seek(0)
            content = buffer.getvalue()
            plt.close(fig)
        
        metadata = {
            'spectrum_id': spectrum.hmdb_id,
            'plot_type': 'static_single',
            'peaks_count': len(spectrum.peaks),
            'quality_score': spectrum.quality_score,
            'generated_at': datetime.now().isoformat(),
            'format': self.config.format.value
        }
        
        return GeneratedGraph(
            content=content,
            format=self.config.format,
            metadata=metadata
        )
    
    def _generate_interactive_comparison(self, spectra: List[ProcessedSpectrum]) -> GeneratedGraph:
        """Generate interactive comparison plot for multiple spectra"""
        if not PLOTLY_AVAILABLE:
            raise ImportError("Plotly is required for interactive plots")
        
        fig = go.Figure()
        colors = self.color_schemes[self.config.style]
        
        # Color palette for multiple spectra
        color_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
        
        for i, spectrum in enumerate(spectra):
            x_data = [peak.mass_charge for peak in spectrum.peaks]
            y_data = [peak.intensity for peak in spectrum.peaks]
            
            # Apply smoothing if requested
            if self.config.smooth_spectrum and len(x_data) > 3:
                x_smooth, y_smooth = self._smooth_spectrum(x_data, y_data)
            else:
                x_smooth, y_smooth = x_data, y_data
            
            # Use different color for each spectrum
            spectrum_color = color_palette[i % len(color_palette)]
            
            fig.add_trace(go.Scatter(
                x=x_smooth,
                y=y_smooth,
                mode='lines+markers',
                name=f"{spectrum.hmdb_id} - {spectrum.metadata.spectrum_type.value}",
                line=dict(color=spectrum_color, width=2),
                marker=dict(color=spectrum_color, size=4),
                hovertemplate=f'<b>{spectrum.hmdb_id}</b><br><b>m/z:</b> %{{x:.3f}}<br><b>Intensity:</b> %{{y:.4f}}<extra></extra>'
            ))
        
        # Customize layout for comparison
        fig.update_layout(
            title={
                'text': f"Spectrum Comparison ({len(spectra)} spectra)",
                'x': 0.5,
                'font': {'size': self.config.title_font_size, 'color': colors['text']}
            },
            xaxis_title='m/z',
            yaxis_title='Relative Intensity',
            width=self.config.width,
            height=self.config.height,
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font=dict(color=colors['text']),
            showlegend=True,
            hovermode='closest'
        )
        
        # Grid configuration
        if self.config.show_grid:
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor=colors['grid'])
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor=colors['grid'])
        
        html_content = pio.to_html(fig, include_plotlyjs=True, div_id="spectrum-comparison")
        
        metadata = {
            'spectra_ids': [spectrum.hmdb_id for spectrum in spectra],
            'plot_type': 'interactive_comparison',
            'spectra_count': len(spectra),
            'generated_at': datetime.now().isoformat()
        }
        
        return GeneratedGraph(
            content=html_content,
            format=PlotFormat.INTERACTIVE_HTML,
            metadata=metadata
        )
    
    def _generate_static_comparison(self, spectra: List[ProcessedSpectrum]) -> GeneratedGraph:
        """Generate static comparison plot for multiple spectra"""
        
        fig, ax = plt.subplots(figsize=(self.config.width/100, self.config.height/100))
        colors = self.color_schemes[self.config.style]
        
        # Set background
        fig.patch.set_facecolor(colors['background'])
        ax.set_facecolor(colors['background'])
        
        # Color palette for multiple spectra
        color_palette = plt.cm.tab10(np.linspace(0, 1, len(spectra)))
        
        for i, spectrum in enumerate(spectra):
            x_data = [peak.mass_charge for peak in spectrum.peaks]
            y_data = [peak.intensity for peak in spectrum.peaks]
            
            # Apply smoothing if requested
            if self.config.smooth_spectrum and len(x_data) > 3:
                x_smooth, y_smooth = self._smooth_spectrum(x_data, y_data)
            else:
                x_smooth, y_smooth = x_data, y_data
            
            ax.plot(x_smooth, y_smooth, color=color_palette[i], linewidth=2, 
                   label=f"{spectrum.hmdb_id} - {spectrum.metadata.spectrum_type.value}",
                   marker='o', markersize=2)
        
        # Customize plot
        ax.set_xlabel('m/z', color=colors['text'], fontsize=self.config.axis_font_size)
        ax.set_ylabel('Relative Intensity', color=colors['text'], fontsize=self.config.axis_font_size)
        ax.set_title(f"Spectrum Comparison ({len(spectra)} spectra)",
                    color=colors['text'], fontsize=self.config.title_font_size, pad=20)
        
        # Grid and legend
        if self.config.show_grid:
            ax.grid(True, color=colors['grid'], alpha=0.7, linestyle='-', linewidth=0.5)
        
        if self.config.show_legend:
            ax.legend(loc='upper right', fontsize=self.config.axis_font_size-2,
                     facecolor=colors['background'], edgecolor=colors['text'])
        
        # Style axes
        ax.tick_params(colors=colors['text'])
        for spine in ax.spines.values():
            spine.set_color(colors['text'])
        
        # Save to buffer
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png', dpi=self.config.dpi, bbox_inches='tight',
                   facecolor=colors['background'], edgecolor='none')
        buffer.seek(0)
        content = buffer.getvalue()
        plt.close(fig)
        
        metadata = {
            'spectra_ids': [spectrum.hmdb_id for spectrum in spectra],
            'plot_type': 'static_comparison',
            'spectra_count': len(spectra),
            'generated_at': datetime.now().isoformat()
        }
        
        return GeneratedGraph(
            content=content,
            format=self.config.format,
            metadata=metadata
        )
    
    def _smooth_spectrum(self, x_data: List[float], y_data: List[float]) -> Tuple[List[float], List[float]]:
        """Apply smoothing to spectrum data"""
        try:
            # Convert to numpy arrays
            x = np.array(x_data)
            y = np.array(y_data)
            
            # Sort by x values
            sort_idx = np.argsort(x)
            x_sorted = x[sort_idx]
            y_sorted = y[sort_idx]
            
            # Apply Savitzky-Golay filter for smoothing
            window_length = min(11, len(x_sorted) // 2 * 2 + 1)  # Ensure odd number
            if window_length >= 3:
                y_smooth = signal.savgol_filter(y_sorted, window_length, 2)
                return x_sorted.tolist(), y_smooth.tolist()
            else:
                return x_data, y_data
                
        except Exception as e:
            logger.warning(f"Smoothing failed: {e}, returning original data")
            return x_data, y_data
    
    def export_data(self, spectrum: ProcessedSpectrum, format: str = "json") -> Dict[str, Any]:
        """
        Export spectrum data in various formats
        
        Args:
            spectrum: ProcessedSpectrum object
            format: Export format ("json", "csv", "tsv")
            
        Returns:
            Dictionary containing exported data
        """
        base_data = {
            'hmdb_id': spectrum.hmdb_id,
            'spectrum_type': spectrum.metadata.spectrum_type.value,
            'instrument_type': spectrum.metadata.instrument_type.value,
            'quality_score': spectrum.quality_score,
            'peaks': [
                {
                    'mass_charge': peak.mass_charge,
                    'intensity': peak.intensity,
                    'annotation': peak.annotation
                }
                for peak in spectrum.peaks
            ],
            'metadata': {
                'chromatography_type': spectrum.metadata.chromatography_type,
                'sample_concentration': spectrum.metadata.sample_concentration,
                'solvent': spectrum.metadata.solvent,
                'temperature': spectrum.metadata.temperature,
                'ph': spectrum.metadata.ph
            },
            'export_timestamp': datetime.now().isoformat()
        }
        
        if format.lower() == "json":
            return base_data
        
        elif format.lower() == "csv":
            csv_data = []
            csv_data.append(f"# HMDB ID: {spectrum.hmdb_id}")
            csv_data.append(f"# Spectrum Type: {spectrum.metadata.spectrum_type.value}")
            csv_data.append(f"# Quality Score: {spectrum.quality_score}")
            csv_data.append("mass_charge,intensity,annotation")
            
            for peak in spectrum.peaks:
                annotation = peak.annotation or ""
                csv_data.append(f"{peak.mass_charge},{peak.intensity},{annotation}")
            
            return {"csv_content": "\n".join(csv_data)}
        
        else:
            return base_data

class SpectraVisualizationPipeline:
    """Complete pipeline for spectra visualization"""
    
    def __init__(self, default_config: Optional[GraphConfig] = None):
        """
        Initialize the visualization pipeline
        
        Args:
            default_config: Default graph configuration
        """
        self.default_config = default_config or GraphConfig()
        self.generator = SpectraGraphGenerator(self.default_config)
    
    def create_spectrum_visualization(self, spectrum: ProcessedSpectrum,
                                    output_format: PlotFormat = PlotFormat.INTERACTIVE_HTML,
                                    style: PlotStyle = PlotStyle.SCIENTIFIC,
                                    save_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a complete spectrum visualization
        
        Args:
            spectrum: ProcessedSpectrum object
            output_format: Desired output format
            style: Visual style
            save_path: Optional file path to save the plot
            
        Returns:
            Dictionary containing visualization data and metadata
        """
        # Create custom config
        config = GraphConfig(
            format=output_format,
            style=style,
            width=self.default_config.width,
            height=self.default_config.height
        )
        
        # Generate the plot
        graph = self.generator.generate_spectrum_plot(spectrum, config)
        
        # Save if path provided
        if save_path:
            success = graph.save_to_file(save_path)
            graph.metadata['saved_to_file'] = success
            graph.metadata['file_path'] = save_path if success else None
        
        # Prepare response
        result = {
            'success': True,
            'graph_data': {
                'content': graph.content if output_format != PlotFormat.EMBEDDED_BASE64 else None,
                'base64': graph.base64_encoded if output_format == PlotFormat.EMBEDDED_BASE64 else None,
                'metadata': graph.metadata,
                'format': output_format.value
            },
            'spectrum_info': {
                'hmdb_id': spectrum.hmdb_id,
                'spectrum_type': spectrum.metadata.spectrum_type.value,
                'quality_score': spectrum.quality_score,
                'peaks_count': len(spectrum.peaks)
            },
            'processing_info': {
                'generated_at': datetime.now().isoformat(),
                'config_used': {
                    'format': output_format.value,
                    'style': style.value,
                    'width': config.width,
                    'height': config.height
                }
            }
        }
        
        return result
    
    def create_comparison_visualization(self, spectra: List[ProcessedSpectrum],
                                      output_format: PlotFormat = PlotFormat.INTERACTIVE_HTML,
                                      style: PlotStyle = PlotStyle.SCIENTIFIC,
                                      save_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a comparison visualization for multiple spectra
        
        Args:
            spectra: List of ProcessedSpectrum objects
            output_format: Desired output format
            style: Visual style
            save_path: Optional file path to save the plot
            
        Returns:
            Dictionary containing visualization data and metadata
        """
        if not spectra:
            return {
                'success': False,
                'error': 'No spectra provided for comparison'
            }
        
        # Create custom config
        config = GraphConfig(
            format=output_format,
            style=style,
            width=self.default_config.width,
            height=self.default_config.height
        )
        
        # Generate the comparison plot
        graph = self.generator.generate_comparison_plot(spectra, config)
        
        # Save if path provided
        if save_path:
            success = graph.save_to_file(save_path)
            graph.metadata['saved_to_file'] = success
            graph.metadata['file_path'] = save_path if success else None
        
        # Prepare response
        result = {
            'success': True,
            'graph_data': {
                'content': graph.content,
                'metadata': graph.metadata,
                'format': output_format.value
            },
            'spectra_info': [
                {
                    'hmdb_id': spectrum.hmdb_id,
                    'spectrum_type': spectrum.metadata.spectrum_type.value,
                    'quality_score': spectrum.quality_score,
                    'peaks_count': len(spectrum.peaks)
                }
                for spectrum in spectra
            ],
            'processing_info': {
                'generated_at': datetime.now().isoformat(),
                'spectra_count': len(spectra),
                'config_used': {
                    'format': output_format.value,
                    'style': style.value,
                    'width': config.width,
                    'height': config.height
                }
            }
        }
        
        return result