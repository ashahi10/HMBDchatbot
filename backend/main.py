from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import os

from dotenv import load_dotenv

from utils.neo4j_connection import Neo4jConnection
from utils.schema_generator import generate_text_schema
from services.llm_service import MultiLLMService, LLMProvider
from pipeline.langchain_pipeline import LangChainPipeline
from api.query_controller import router as query_router

import time

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    start = time.time()
    # load env variables, changing .env file will allow swapping bw kgs
    neo4j_uri = os.getenv("NEO4J_URI")
    print(neo4j_uri)
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    # groq_api_key = os.getenv("GROQ_API_KEY")
    groq_api_key = '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'
    deepseek_api_key = os.getenv("DEEPSEEK_API_KEY")

    # make original connection to neo4j
    neo4j_connection = Neo4jConnection(
        uri=neo4j_uri,
        user=neo4j_user,
        password=neo4j_password
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
            "query_generator_model_name": "tomasonjo/llama3-text2cypher-demo",
            "query_summarizer_model": "mistral-nemo:latest",
        },
        "deepseek": {
            "provider": LLMProvider.DEEPSEEK,
            "api_key": deepseek_api_key,
            "query_generator_model_name": "deepseek-chat",
            "query_summarizer_model": "deepseek-chat",
        }
    }

    provider = "ollama"

    # create llm service - local or remote (ollama or groq)
    llm_service = MultiLLMService(**configs[provider])

    # load db schema - generated on app launch
    neo4j_schema_text = generate_text_schema(neo4j_connection)
    # print(neo4j_schema_text)

    # create query pipeline
    query_pipeline = LangChainPipeline(
        llm_service=llm_service,
        neo4j_connection=neo4j_connection,
        neo4j_schema_text=neo4j_schema_text
    )
    
    # store these in app state to use in routers
    app.state.neo4j_connection = neo4j_connection
    app.state.llm_service = llm_service
    app.state.query_pipeline = query_pipeline
    app.state.neo4j_schema_text = neo4j_schema_text
    
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