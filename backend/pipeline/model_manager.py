from typing import Dict, Any, Optional
from langchain_core.runnables import RunnableSequence
from langchain_core.output_parsers import PydanticOutputParser
from langchain_ollama import ChatOllama
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler


from langchain_core.runnables import RunnableSequence, RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser, PydanticOutputParser

from pipeline.config import PipelineConfig

class ModelManager:
    def __init__(self, config: PipelineConfig):
        self.config = config

    def get_llm(self, model_name: str, streaming: bool = False, format: str = "json") -> ChatOllama:
        return ChatOllama(
            base_url=self.config.chains.base_url,
            model=model_name,
            temperature=self.config.models.temperature,
            num_ctx=self.config.models.num_ctx,
            callbacks=[StreamingStdOutCallbackHandler()] if streaming else None,
            format=format
        )

    def create_chain(
        self,
        assignment_funcs: Dict[str, Any],
        chain_prompt: Any,
        streaming: bool,
        parser: Optional[PydanticOutputParser] = None,
        streaming_model: bool = False,
        format: str = "",
        model_name: str = "mistral-nemo:latest"
    ) -> RunnableSequence:

        llm = self.get_llm(
            streaming=streaming_model or self.config.chains.streaming_model,
            format=format,
            model_name=model_name
        )
        chain = RunnablePassthrough.assign(**assignment_funcs) | chain_prompt | llm
        chain |= parser if parser else StrOutputParser()
        return chain 