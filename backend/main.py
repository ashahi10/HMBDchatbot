from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import os
import sys

# Add the parent directory to sys.path so Python can find the modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

from backend.utils.neo4j_connection import Neo4jConnection
from backend.utils.schema_generator import generate_text_schema, get_or_cache_schema
from backend.utils.cache_manager import CacheManager
from backend.services.llm_service import MultiLLMService, LLMProvider
from backend.services.memory_service import MemoryService
from backend.pipeline.langchain_pipeline import LangChainPipeline
from backend.api.query_controller import router as query_router
from backend.pipeline.hmdb_api import HMDBApiClient, RateLimiter

import time

load_dotenv()

# Memory debug mode - set to "true" to enable memory relevance debugging
os.environ["DEBUG_MEMORY_RANKING"] = os.getenv("DEBUG_MEMORY_RANKING", "true")

@asynccontextmanager
async def lifespan(app: FastAPI):
    start = time.time()

    # Create cache directories if they don't exist
    cache_manager = CacheManager()
    # You can optionally customize TTLs here
    # cache_manager = CacheManager(
    #     schema_cache_ttl=86400,   # 1 day
    #     query_cache_ttl=3600,     # 1 hour
    #     api_cache_ttl=43200       # 12 hours
    # )
    
    # Create memory service
    memory_service = MemoryService()
    
    # Check if caching is enabled via environment variable
    # Default to True if not specified
    enable_caching = os.getenv("ENABLE_CACHING", "true").lower() == "true"

    # 1) Create rate limiter
    rate_limiter = RateLimiter()
    
    # 2) Create HMDB Client with caching enabled
    hmdb_client = HMDBApiClient(rate_limiter=rate_limiter, use_cache=enable_caching)
    
    # 3) Store in app.state
    app.state.hmdb_client = hmdb_client


    # load env variables, changing .env file will allow swapping bw kgs
    neo4j_uri = os.getenv("NEO4J_URI")
    print(neo4j_uri)
    neo4j_user = os.getenv("NEO4J_USERNAME")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    # groq_api_key = os.getenv("GROQ_API_KEY")
    groq_api_key = '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'
    deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")

    # make original connection to neo4j with caching enabled
    neo4j_connection = Neo4jConnection(
        uri=neo4j_uri,
        user=neo4j_user,
        password=neo4j_password,
        use_cache=enable_caching
    )
    
    configs = {
        "groq": {
            "provider": LLMProvider.GROQ,
            "api_key": groq_api_key,
            "query_generator_model_name": "qwen-2.5-32b",
            "query_summarizer_model": "qwen-2.5-32b",
        },
        "ollama": {
            "provider": LLMProvider.OLLAMA,
            "query_generator_model_name": "deepseek-r1:1.5b",
            "query_summarizer_model": "deepseek-r1:1.5b",
        },
        "deepseek": {
            "provider": LLMProvider.DEEPSEEK,
            "api_key": deepseek_api_key,
            "query_generator_model_name": "deepseek-chat",
            "query_summarizer_model": "deepseek-chat",
        }
    }

    # provider = "ollama"
    provider = "groq"

    # create llm service - local or remote (ollama or groq)
    llm_service = MultiLLMService(**configs[provider])

    # Generate or load db schema from cache
    start = time.time()
    neo4j_schema_text = get_or_cache_schema(neo4j_connection, force_reload=False)
    print(f"[PERF] Schema load complete in {time.time() - start:.2f}s")
    # print(neo4j_schema_text)

    # create query pipeline
    query_pipeline = LangChainPipeline(
        llm_service=llm_service,
        neo4j_connection=neo4j_connection,
        neo4j_schema_text=neo4j_schema_text,
        hmdb_client=hmdb_client
    )
    
    # store these in app state to use in routers
    app.state.neo4j_connection = neo4j_connection
    app.state.llm_service = llm_service
    app.state.query_pipeline = query_pipeline
    app.state.neo4j_schema_text = neo4j_schema_text
    app.state.cache_manager = cache_manager
    app.state.memory_service = memory_service
    
    # Clean up expired memory sessions
    cleaned_sessions = memory_service.clean_expired_sessions()
    if cleaned_sessions > 0:
        print(f"Cleaned {cleaned_sessions} expired memory sessions")
    
    print("running fine")
    print(f"app startup time: {time.time() - start}")
    yield
    # close neo4j connection before shutting down
    neo4j_connection.close()
    print("app shutdown, neo4j connection closed")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

app.include_router(query_router)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)