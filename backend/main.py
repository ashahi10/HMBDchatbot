from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import uvicorn
import os

from dotenv import load_dotenv

from utils.neo4j_connection import Neo4jConnection
from utils.schema_generator import generate_text_schema
from pipeline.langchain_pipeline import LangChainPipeline
from pipeline.config import PipelineConfig, ModelConfig, ChainConfig, EntityConfig
from api.query_controller import router as query_router

import time

load_dotenv()

@asynccontextmanager
async def lifespan(app: FastAPI):
    start = time.time()
    # load env variables, changing .env file will allow swapping bw kgs
    neo4j_uri = os.getenv("NEO4J_URI")
    neo4j_user = os.getenv("NEO4J_USER")
    neo4j_password = os.getenv("NEO4J_PASSWORD")

    neo4j_connection = Neo4jConnection(
        uri=neo4j_uri,
        user=neo4j_user,
        password=neo4j_password
    )
    
    # load db schema - generated on app launch
    neo4j_schema_text = generate_text_schema(neo4j_connection)

    print(neo4j_schema_text)

    # Create pipeline configuration
    pipeline_config = PipelineConfig(
        models=ModelConfig(),
        chains=ChainConfig(),
        entities=EntityConfig(),
        neo4j_schema_text=neo4j_schema_text,
        neo4j_connection=neo4j_connection
    )

    query_pipeline = LangChainPipeline(config=pipeline_config)
    
    # store these in app state to use in routers
    app.state.neo4j_connection = neo4j_connection
    app.state.query_pipeline = query_pipeline
    app.state.neo4j_schema_text = neo4j_schema_text
    
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