from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List

router = APIRouter()

class QueryRequest(BaseModel):
    question: str

class CacheClearRequest(BaseModel):
    cache_type: Optional[str] = None

@router.post("/query")
async def query_endpoint(query_request: QueryRequest, request: Request):
    pipeline = request.app.state.query_pipeline

    pipeline_stream = pipeline.run_pipeline(
        user_question=query_request.question,
    )
    
    return StreamingResponse(pipeline_stream, media_type="text/event-stream")

@router.get("/cache/status")
async def cache_status(request: Request):
    """Get the status of all caches"""
    try:
        cache_manager = request.app.state.cache_manager
        
        # Count files in each cache directory
        schema_count = len(list(cache_manager.schema_cache_path.glob('*.json')))
        query_count = len(list(cache_manager.query_cache_path.glob('*.pickle')))
        api_count = len(list(cache_manager.api_cache_path.glob('*.json')))
        
        # Calculate approximate total size
        schema_size = sum(f.stat().st_size for f in cache_manager.schema_cache_path.glob('*.json'))
        query_size = sum(f.stat().st_size for f in cache_manager.query_cache_path.glob('*.pickle'))
        api_size = sum(f.stat().st_size for f in cache_manager.api_cache_path.glob('*.json'))
        
        return JSONResponse({
            "status": "active",
            "schema_cache": {
                "count": schema_count,
                "size_bytes": schema_size,
                "ttl_seconds": cache_manager.ttl['schema']
            },
            "query_cache": {
                "count": query_count,
                "size_bytes": query_size,
                "ttl_seconds": cache_manager.ttl['query']
            },
            "api_cache": {
                "count": api_count,
                "size_bytes": api_size,
                "ttl_seconds": cache_manager.ttl['api']
            },
            "total_size_bytes": schema_size + query_size + api_size
        })
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting cache status: {str(e)}")

@router.post("/cache/clear")
async def clear_cache(request: Request, clear_request: CacheClearRequest = None):
    """Clear specific or all caches"""
    try:
        cache_manager = request.app.state.cache_manager
        cache_type = clear_request.cache_type if clear_request else None
        
        # Validate cache type
        if cache_type and cache_type not in ['schema', 'query', 'api']:
            raise HTTPException(
                status_code=400, 
                detail="Invalid cache type. Must be one of: 'schema', 'query', 'api', or null to clear all"
            )
        
        cache_manager.clear_cache(cache_type)
        
        message = f"Cleared {cache_type} cache" if cache_type else "Cleared all caches"
        return JSONResponse({"status": "success", "message": message})
    except Exception as e:
        if isinstance(e, HTTPException):
            raise e
        raise HTTPException(status_code=500, detail=f"Error clearing cache: {str(e)}")
