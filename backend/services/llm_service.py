import asyncio
from typing import Optional, AsyncGenerator, Iterable
from groq import Groq
from ollama import Client as OllamaClient
from langchain_community.llms import Ollama
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from openai import OpenAI

class LLMProvider:
    GROQ = "groq"
    OLLAMA = "ollama"
    DEEPSEEK = "deepseek"

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
        else:
            raise ValueError(f"Unsupported provider: {provider}")

    def get_langchain_llm(self, streaming: bool = False):
        if self.provider == LLMProvider.GROQ:
            return ChatGroq(
                api_key=self.api_key,
                model_name=self.default_query_model,
                temperature=0.2,
                max_tokens=2048,
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
            else:
                raise ValueError(f"Unsupported provider: {self.provider}")
        except Exception as stream_err:
            err_msg = f"Streaming error: {stream_err}"
            print(err_msg)
            yield err_msg

    async def _async_iter(self, iterable: Iterable) -> AsyncGenerator:
        for item in iterable:
            yield item
            await asyncio.sleep(0)