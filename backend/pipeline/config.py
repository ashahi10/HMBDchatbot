from dataclasses import dataclass
from typing import Any

@dataclass
class ModelConfig:
    entity_model: str = "mistral-nemo:latest"
    query_plan_model: str = "mistral-nemo:latest"
    query_model: str = "mistral-nemo:latest"
    summary_model: str = "mistral-nemo:latest"
    other_model: str = "mistral-nemo:latest"
    retry_model: str = "mistral-nemo:latest"
    sufficiency_model: str = "mistral-nemo:latest"
    temperature: float = 0.4
    num_ctx: int = 4096

@dataclass
class ChainConfig:
    streaming: bool = True
    streaming_model: bool = False
    format: str = "json"
    base_url: str = "https://2vlm5q6h-11434.usw2.devtunnels.ms/"

@dataclass
class EntityConfig:
    confidence_threshold: float = 0.5
    max_results: int = 3
    fuzzy_threshold: float = 0.3
    synonym_threshold: float = 2.0

@dataclass
class PipelineConfig:
    models: ModelConfig
    chains: ChainConfig
    entities: EntityConfig
    neo4j_schema_text: str
    neo4j_connection: Any