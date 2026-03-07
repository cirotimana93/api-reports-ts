from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.config import settings
from app.api.v1.api import api_router
from app.events.events import run_events

@asynccontextmanager
async def lifespan(app: FastAPI):
    # acciones al iniciar la aplicacion
    await run_events()
    yield
    # acciones al cerrar la aplicacion
    pass

app = FastAPI(
    title=settings.APP_NAME,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

app.include_router(api_router, prefix=settings.API_V1_STR)


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"message": "Welcome to the FastAPI Multi-Scraper API Teleservicios"}

@app.api_route("/favicon.ico", include_in_schema=False, methods=["GET", "HEAD"])
async def favicon():
    return {}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
