from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.routers import admin, auth, internal_judge, operator, participant, public, storage
from app.services.errors import AppError
from app.settings import settings


app = FastAPI(title="Zerone Online Judge API", version="0.1.0")

cors_origins = [origin.strip() for origin in settings.cors_allow_origins.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(AppError)
async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": {
                "code": exc.code,
                "message": exc.message,
                "request_id": request.state.request_id,
                "details": exc.details,
            }
        },
    )


@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    request.state.request_id = request.headers.get("x-request-id", f"req_{uuid4().hex}")
    response = await call_next(request)
    response.headers["x-request-id"] = request.state.request_id
    return response


@app.get("/api/health")
async def health(request: Request):
    return {
        "data": {
            "status": "ok",
            "env": settings.app_env,
            "release_color": settings.release_color,
            "release_version": settings.release_version,
        },
        "request_id": request.state.request_id,
    }


app.include_router(public.router, prefix="/api")
app.include_router(auth.router, prefix="/api")
app.include_router(participant.router, prefix="/api")
app.include_router(admin.router, prefix="/api")
app.include_router(operator.router, prefix="/api")
app.include_router(internal_judge.router, prefix="/api")
app.include_router(storage.router, prefix="/api")
