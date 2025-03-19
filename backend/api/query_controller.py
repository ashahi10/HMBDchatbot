from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

router = APIRouter()

class QueryRequest(BaseModel):
    question: str


@router.post("/query")
async def query_endpoint(query_request: QueryRequest, request: Request):
    pipeline = request.app.state.query_pipeline

    pipeline_stream = pipeline.run_pipeline(
        user_question=query_request.question,
    )
    
    return StreamingResponse(pipeline_stream, media_type="text/event-stream")
