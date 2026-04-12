from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.config import cfg
from app.database import (
    add_chunk,
    delete_chunk,
    get_chunk,
    get_status,
    init_db,
    list_chunks,
    search_chunks,
    update_chunk,
)

@asynccontextmanager
async def _lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="KoreRAG",
    description="Retrieval-augmented generation chunk storage service",
    lifespan=_lifespan,
)


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ChunkCreate(BaseModel):
    content: str
    title: Optional[str] = None
    source: Optional[str] = None
    tags: Optional[str] = None


class ChunkUpdate(BaseModel):
    content: Optional[str] = None
    title: Optional[str] = None
    source: Optional[str] = None
    tags: Optional[str] = None


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

@app.get("/status", summary="Service health and stats")
def route_status():
    return get_status()


# ---------------------------------------------------------------------------
# Chunks CRUD
# ---------------------------------------------------------------------------

@app.get("/chunks", summary="List all chunks (metadata only)")
def route_list_chunks(limit: int = 100, offset: int = 0):
    return list_chunks(limit=limit, offset=offset)


@app.get("/chunks/{chunk_id}", summary="Get a single chunk with full content")
def route_get_chunk(chunk_id: int):
    chunk = get_chunk(chunk_id, include_content=True)
    if chunk is None:
        raise HTTPException(status_code=404, detail="Chunk not found")
    return chunk


@app.post("/chunks", status_code=201, summary="Add a new chunk")
def route_add_chunk(data: ChunkCreate):
    return add_chunk(
        content=data.content,
        title=data.title,
        source=data.source,
        tags=data.tags,
    )


@app.patch("/chunks/{chunk_id}", summary="Update chunk fields")
def route_update_chunk(chunk_id: int, data: ChunkUpdate):
    if get_chunk(chunk_id, include_content=False) is None:
        raise HTTPException(status_code=404, detail="Chunk not found")
    updated = update_chunk(chunk_id, data.model_dump(exclude_none=True))
    return updated


@app.delete("/chunks/{chunk_id}", summary="Delete a chunk")
def route_delete_chunk(chunk_id: int):
    if not delete_chunk(chunk_id):
        raise HTTPException(status_code=404, detail="Chunk not found")
    return {"deleted": chunk_id}


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

@app.get("/search", summary="Full-text search across chunks")
def route_search(
    q: str,
    limit: int = 20,
    source: Optional[str] = None,
    tags: Optional[str] = None,
):
    try:
        results = search_chunks(q, limit=limit, source=source, tags=tags)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return results
