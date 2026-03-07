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
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan
)

app.include_router(api_router, prefix=settings.API_V1_STR)


@app.get("/")
async def root():
    return {"message": "Welcome to the FastAPI Multi-Scraper API"}

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return {}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
