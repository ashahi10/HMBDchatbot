from typing import Dict, Any
from pipeline.prompts import entity_prompt, query_plan_prompt, query_prompt, summary_prompt, other_prompt, retry_prompt, sufficiency_prompt
from pipeline.config import PipelineConfig
from pipeline.model_manager import ModelManager

class ChainManager:
    def __init__(self, config: PipelineConfig, model_manager: ModelManager):
        self.config = config
        self.model_manager = model_manager

    def create_entity_chain(self) -> Any:
        return self.model_manager.create_chain(
            {
                "question": lambda inp: inp["question"],
                "schema": lambda _: self.config.neo4j_schema_text
            },
            entity_prompt,
            streaming=True,
            parser=None,
            streaming_model=False,
            format="json",
            model_name=self.config.models.entity_model
        )

    def create_query_plan_chain(self) -> Any:
        return self.model_manager.create_chain(
            {
                "question": lambda inp: inp["question"],
                "entities": lambda inp: inp["entities"],
                "schema": lambda _: self.config.neo4j_schema_text
            },
            query_plan_prompt,
            streaming=True,
            parser=None,
            streaming_model=False,
            format="json",
            model_name=self.config.models.query_plan_model
        )

    def create_query_chain(self) -> Any:
        return self.model_manager.create_chain(
            {
                "query_plan": lambda inp: inp["query_plan"],
                "schema": lambda _: self.config.neo4j_schema_text
            },
            query_prompt,
            streaming=True,
            parser=None,
            streaming_model=False,
            format="",
            model_name=self.config.models.query_model
        )

    def create_summary_chain(self) -> Any:
        return self.model_manager.create_chain(
            {
                "query_results": lambda inp: inp["query_results"],
                "question": lambda inp: inp["question"]
            },
            summary_prompt,
            streaming=True,
            parser=None,
            streaming_model=True,
            format="",
            model_name=self.config.models.summary_model
        )

    def create_other_chain(self) -> Any:
        return self.model_manager.create_chain(
            {
                "question": lambda inp: inp["question"]
            },
            other_prompt,
            streaming=True,
            parser=None,
            streaming_model=True,
            format="",
            model_name=self.config.models.other_model
        )

    def create_retry_chain(self) -> Any:
        return self.model_manager.create_chain(
            {
                "query_plan": lambda inp: inp["query_plan"],
                "schema": lambda _: self.config.neo4j_schema_text,
                "old_query": lambda inp: inp["old_query"],
                "error": lambda inp: inp["error"]
            },
            retry_prompt,
            streaming=True,
            parser=None,
            streaming_model=False,
            format="",
            model_name=self.config.models.retry_model
        )

    def create_sufficiency_chain(self) -> Any:
        return self.model_manager.create_chain(
            {
                "neo4j_results": lambda inp: inp["neo4j_results"],
                "question": lambda inp: inp["question"],
                "schema": lambda _: self.config.neo4j_schema_text
            },
            sufficiency_prompt,
            streaming=True,
            parser=None,
            streaming_model=True,
            format="json",
            model_name=self.config.models.sufficiency_model
        )

    def initialize_chains(self) -> Dict[str, Any]:
        return {
            "entity_chain": self.create_entity_chain(),
            "query_plan_chain": self.create_query_plan_chain(),
            "query_chain": self.create_query_chain(),
            "summary_chain": self.create_summary_chain(),
            "other_chain": self.create_other_chain(),
            "retry_chain": self.create_retry_chain(),
            "sufficiency_chain": self.create_sufficiency_chain()
        } 