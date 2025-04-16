import asyncio
from typing import Optional, AsyncGenerator, Iterable, Dict, List, Any
from groq import Groq
from ollama import Client as OllamaClient
from langchain_community.llms import Ollama
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from openai import OpenAI
import dashscope
from dashscope import Generation

# Set dashscope international base URL
dashscope.base_http_api_url = 'https://dashscope-intl.aliyuncs.com/api/v1'

class LLMProvider:
    GROQ = "groq"
    OLLAMA = "ollama"
    DEEPSEEK = "deepseek"
    QWEN = "qwen"

class MultiLLMService:
    def __init__(self, provider: str, api_key: Optional[str] = None, query_generator_model_name: Optional[str] = None,
                 query_summarizer_model: Optional[str] = None):
        self.provider = provider
        self.api_key = api_key
        self.default_query_model = query_generator_model_name or "deepseek-chat"
        self.default_summary_model = query_summarizer_model or "deepseek-chat"
        
        if self.provider == LLMProvider.GROQ:
            self.client = Groq(api_key=self.api_key)
        elif self.provider == LLMProvider.OLLAMA:
            self.client = OllamaClient()
        elif self.provider == LLMProvider.DEEPSEEK:
            self.client = OpenAI(api_key=self.api_key, base_url="https://api.deepseek.com/v1")
        elif self.provider == LLMProvider.QWEN:
            # Qwen doesn't need a client instantiation as we'll use dashscope.Generation directly
            # Set the dashscope API key
            dashscope.api_key = self.api_key
            self.client = None
        else:
            raise ValueError(f"Unsupported provider: {provider}")

    def get_langchain_llm(self, streaming: bool = False, temperature: float = None, max_tokens: int = None):
        if self.provider == LLMProvider.GROQ:
            return ChatGroq(
                api_key=self.api_key,
                model_name=self.default_query_model,
                temperature=temperature if temperature is not None else 0.2,
                max_tokens=max_tokens if max_tokens is not None else 2048,
                streaming=streaming
            )
        elif self.provider == LLMProvider.OLLAMA:
            return Ollama(
                model=self.default_query_model,
                temperature=0.2,
                num_ctx=4096,
                callbacks=None
            )
        elif self.provider == LLMProvider.DEEPSEEK:
            return ChatOpenAI(
                api_key=self.api_key,
                base_url="https://api.deepseek.com/v1",
                model_name=self.default_query_model,
                temperature=0.2,
                max_tokens=1024,
                streaming=streaming
            )
        elif self.provider == LLMProvider.QWEN:
            # For Qwen, we'll use a custom class adapter that proxies to DashScope directly
            from langchain_core.language_models.chat_models import BaseChatModel
            from langchain_core.callbacks.manager import CallbackManagerForLLMRun
            from langchain_core.messages import (
                BaseMessage, SystemMessage, HumanMessage, AIMessage
            )
            from langchain_core.outputs import ChatGeneration, ChatResult
            from typing import Any, Dict, List, Optional, Type, cast

            class DashScopeChatModel(BaseChatModel):
                api_key: str
                model_name: str
                temperature: float = 0.2
                max_tokens: int = 2048
                streaming: bool = False

                def _generate(
                    self, messages: List[BaseMessage], stop: Optional[List[str]] = None,
                    run_manager: Optional[CallbackManagerForLLMRun] = None,
                    **kwargs: Any
                ) -> ChatResult:
                    dashscope_messages = []
                    for message in messages:
                        if isinstance(message, SystemMessage):
                            dashscope_messages.append({"role": "system", "content": message.content})
                        elif isinstance(message, HumanMessage):
                            dashscope_messages.append({"role": "user", "content": message.content})
                        elif isinstance(message, AIMessage):
                            dashscope_messages.append({"role": "assistant", "content": message.content})
                    
                    # Extract prompt from the last user message
                    prompt = ""
                    for message in reversed(messages):
                        if isinstance(message, HumanMessage):
                            prompt = message.content
                            break

                    response = Generation.call(
                        api_key=self.api_key,
                        model=self.model_name,
                        messages=dashscope_messages,
                        temperature=self.temperature,
                        max_tokens=self.max_tokens
                    )
                    
                    if response.status_code != 200:
                        raise Exception(f"DashScope API error: {response.status_code} - {response.message}")
                    
                    message = AIMessage(content=response.output.text)
                    return ChatResult(generations=[ChatGeneration(message=message)])
                
                async def _agenerate(
                    self, messages: List[BaseMessage], stop: Optional[List[str]] = None,
                    run_manager: Optional[CallbackManagerForLLMRun] = None,
                    **kwargs: Any
                ) -> ChatResult:
                    # Just call the synchronous version for now
                    return self._generate(messages, stop, run_manager, **kwargs)
                
                @property
                def _llm_type(self) -> str:
                    return "dashscope-chat"

            # Return our custom DashScope adapter
            return DashScopeChatModel(
                api_key=self.api_key,
                model_name=self.default_query_model,
                temperature=temperature if temperature is not None else 0.2,
                max_tokens=max_tokens if max_tokens is not None else 2048,
                streaming=streaming
            )
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")

    def generate_query_completion(self, system_prompt: str, user_prompt: str,
                                 model_name: Optional[str] = None) -> str:
        chosen_model = model_name or self.default_query_model
        try:
            if self.provider == LLMProvider.GROQ:
                resp = self.client.chat.completions.create(
                    model=chosen_model, 
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.2,
                    max_tokens=512,
                    top_p=1.0
                )
                return resp.choices[0].message.content.strip()
            elif self.provider == LLMProvider.OLLAMA:
                prompt_text = f"SYSTEM: {system_prompt}\n\nUSER: {user_prompt}"
                resp = self.client.generate(
                    model=chosen_model, 
                    prompt=prompt_text,
                    # options={"temperature": 0.2, "top_p": 1.0, "num_ctx": 512}
                )
                model_name = resp.model
                return resp.response.strip()
            elif self.provider == LLMProvider.DEEPSEEK:
                resp = self.client.chat.completions.create(
                    model="deepseek-chat", 
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.2,
                    max_tokens=512,
                    top_p=1.0
                )
                return resp.choices[0].message.content.strip()
            elif self.provider == LLMProvider.QWEN:
                # Use dashscope.Generation for Qwen API call
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ]
                resp = Generation.call(
                    api_key=self.api_key,
                    model=chosen_model,
                    messages=messages,
                    temperature=0.2,
                    max_tokens=512,
                    top_p=1.0
                )
                
                if resp.status_code == 200:
                    return resp.output.text.strip()
                else:
                    raise Exception(f"Qwen API error: {resp.status_code} - {resp.message}")
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")
        except Exception as err:
            print(f"Query generation error: {err}")
            raise

    async def generate_summary_completion_stream(self, system_prompt: str, user_prompt: str,
                                                model_name: Optional[str] = None) -> AsyncGenerator[str, None]:
        chosen_model = model_name or self.default_summary_model
        try:
            if self.provider == LLMProvider.GROQ:
                stream = self.client.chat.completions.create(
                    model=chosen_model, 
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.7,
                    max_tokens=4096,
                    top_p=1.0,
                    stop=["\n\nReferences:"],
                    stream=True
                )
                async for chunk in self._async_iter(stream):
                    delta = chunk.choices[0].delta
                    if delta and delta.content:
                        yield delta.content
            elif self.provider == LLMProvider.OLLAMA:
                prompt_text = f"SYSTEM: {system_prompt}\n\nUSER: {user_prompt}"
                stream = self.client.generate(
                    model=chosen_model,
                    prompt=prompt_text,
                    stream=True,
                    options={"temperature": 0.8,
                              "num_predict":  4096,
                                "top_p": 1.0}
                )
                async for resp in self._async_iter(stream):
                    if resp.response:
                        yield resp.response
            elif self.provider == LLMProvider.DEEPSEEK:
                stream = self.client.chat.completions.create(
                    model="deepseek-chat", 
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt}
                    ],
                    temperature=0.7,
                    max_tokens=1024,
                    top_p=1.0,
                    stream=True
                )
                async for chunk in self._async_iter(stream):
                    delta = chunk.choices[0].delta
                    if delta and delta.content:
                        yield delta.content
            elif self.provider == LLMProvider.QWEN:
                # Use dashscope.Generation for Qwen API call with streaming
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ]
                stream = Generation.call(
                    api_key=self.api_key,
                    model=chosen_model,
                    messages=messages,
                    temperature=0.7,
                    max_tokens=4096,
                    top_p=1.0,
                    stream=True,
                    incremental_output=True
                )
                
                async for chunk in self._async_iter(stream):
                    if hasattr(chunk, 'output') and hasattr(chunk.output, 'text'):
                        yield chunk.output.text
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")
        except Exception as stream_err:
            err_msg = f"Streaming error: {stream_err}"
            print(err_msg)
            yield err_msg

    async def answer_general_question(self, question: str, context: str = "", 
                                     model_name: Optional[str] = None) -> AsyncGenerator[str, None]:
        """
        Generate a streaming response for general questions that don't require database queries
        
        Args:
            question: The user's question
            context: Optional context from conversation history
            model_name: Optional model name to override the default
            
        Returns:
            AsyncGenerator yielding response chunks
        """
        chosen_model = model_name or self.default_summary_model
        system_prompt = """You are an expert metabolomics assistant specializing in the Human Metabolome Database (HMDB), 
        biochemical databases, and molecular biology. Your goal is to provide scientifically accurate, 
        well-structured answers to general questions about metabolomics, biochemistry, and related fields."""
        
        try:
            if self.provider == LLMProvider.GROQ:
                stream = self.client.chat.completions.create(
                    model=chosen_model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"Question: {question}\nContext: {context}"}
                    ],
                    temperature=0.7,
                    max_tokens=2048,
                    top_p=1.0,
                    stream=True
                )
                async for chunk in self._async_iter(stream):
                    delta = chunk.choices[0].delta
                    if delta and delta.content:
                        yield delta.content
            elif self.provider == LLMProvider.OLLAMA:
                prompt_text = f"SYSTEM: {system_prompt}\n\nUSER: Question: {question}\nContext: {context}"
                stream = self.client.generate(
                    model=chosen_model,
                    prompt=prompt_text,
                    stream=True,
                    options={"temperature": 0.7,
                             "num_predict": 2048,
                             "top_p": 1.0}
                )
                async for resp in self._async_iter(stream):
                    if resp.response:
                        yield resp.response
            elif self.provider == LLMProvider.DEEPSEEK:
                stream = self.client.chat.completions.create(
                    model="deepseek-chat",
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": f"Question: {question}\nContext: {context}"}
                    ],
                    temperature=0.7,
                    max_tokens=1024,
                    top_p=1.0,
                    stream=True
                )
                async for chunk in self._async_iter(stream):
                    delta = chunk.choices[0].delta
                    if delta and delta.content:
                        yield delta.content
            elif self.provider == LLMProvider.QWEN:
                # Use dashscope.Generation for Qwen API call with streaming
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Question: {question}\nContext: {context}"}
                ]
                stream = Generation.call(
                    api_key=self.api_key,
                    model=chosen_model,
                    messages=messages,
                    temperature=0.7,
                    max_tokens=2048,
                    top_p=1.0,
                    stream=True,
                    incremental_output=True
                )
                
                async for chunk in self._async_iter(stream):
                    if hasattr(chunk, 'output') and hasattr(chunk.output, 'text'):
                        yield chunk.output.text
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")
        except Exception as stream_err:
            err_msg = f"General question streaming error: {stream_err}"
            print(err_msg)
            yield err_msg

    async def _async_iter(self, iterable: Iterable) -> AsyncGenerator:
        for item in iterable:
            yield item
            await asyncio.sleep(0)