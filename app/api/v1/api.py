from fastapi import APIRouter, Depends
from app.api.v1.endpoints import scraper
from app.api.deps import get_api_key

api_router = APIRouter()
api_router.include_router(
    scraper.router, 
    prefix="/scrapers", 
    tags=["scrapers"],
    dependencies=[Depends(get_api_key)]
)
