from typing import Optional
from langchain_community.chat_models import ChatOllama
from langchain_openai import ChatOpenAI
from langchain.callbacks.streaming_stdout import StreamingStdOutCallbackHandler

class LLMProvider:
    GROQ = "groq"
    OLLAMA = "ollama"

class MultiLLMService:
    def __init__(self, provider: str, api_key: Optional[str] = None, 
                 query_model: Optional[str] = None, summary_model: Optional[str] = None):
        self.provider = provider
        self.api_key = api_key
        self.default_query_model = query_model or "deepseek-chat"
        self.default_summary_model = summary_model or "deepseek-chat"
        
        self.llm = self._get_llm(self.default_query_model)
        self.summary_llm = self._get_llm(self.default_summary_model, streaming=True)

    def _get_llm(self, model_name: str, streaming: bool = False):
        if self.provider == LLMProvider.GROQ:
            return ChatOpenAI(
                api_key=self.api_key,
                base_url="https://api.groq.com/openai/v1",
                model_name=model_name,
                temperature=0.2,
                max_tokens=2048,
                streaming=streaming
            )
        elif self.provider == LLMProvider.OLLAMA:
            return ChatOllama(
                model=model_name,
                temperature=0.2,
                num_ctx=4096,
                callbacks=[StreamingStdOutCallbackHandler()] if streaming else None,
                format=""
            )
        raise ValueError(f"Unsupported provider: {self.provider}")