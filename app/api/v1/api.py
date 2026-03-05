from fastapi import APIRouter
from app.api.v1.endpoints import scraper

api_router = APIRouter()
api_router.include_router(scraper.router, prefix="/scrapers", tags=["scrapers"])
