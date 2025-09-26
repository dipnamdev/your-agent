import os
from typing import Optional
import asyncio
import threading
import time

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl

import main as core


class ProcessSiteRequest(BaseModel):
    url: HttpUrl
    session_id: str | None = None


class AnswerRequest(BaseModel):
    question: str
    url: Optional[HttpUrl] = None
    session_id: str | None = None


app = FastAPI(title="Project_YA API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


_SESSION_HISTORY: dict[str, list[tuple[str, str]]] = {}
_MAX_TURNS = int(os.getenv("MAX_CHAT_TURNS", "5"))

# Simple in-memory job store
_THREADPOOL = asyncio.get_running_loop if False else None  # placeholder to keep imports grouped


@app.post("/process_site")
async def process_site(payload: ProcessSiteRequest):
    try:
        loop = asyncio.get_running_loop()
        index_path = await loop.run_in_executor(None, core.process_site, str(payload.url))
        if payload.session_id:
            _SESSION_HISTORY.pop(payload.session_id, None)
        return {"status": "ok", "vector_index_path": index_path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/answer")
async def answer(payload: AnswerRequest):
    try:
        session_id = payload.session_id or "__default__"
        history = _SESSION_HISTORY.get(session_id, [])
        loop = asyncio.get_running_loop()
        answer_text = await loop.run_in_executor(
            None,
            core.answer_question,
            payload.question,
            str(payload.url) if payload.url else None,
            history,
        )
        history = history + [(payload.question, answer_text)]
        if len(history) > _MAX_TURNS:
            history = history[-_MAX_TURNS:]
        _SESSION_HISTORY[session_id] = history
        return {"status": "ok", "answer": answer_text, "turns": len(history)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Removed job endpoints to simplify API as requested


if __name__ == "__main__":
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("api:app", host=host, port=port, reload=False)


