from fastapi import APIRouter, HTTPException
from typing import List
from app.models.scraper_data import ScraperResult, UnifiedScraperResponse
from app.scrapers.mvt import MVTScraper
from app.scrapers.vgr import VGRScraper
from app.scrapers.gr import GRScraper
from app.scrapers.first import FIRSTScraper
from app.scrapers.lottingo import LottingoScraper
from app.logic.orchestrator import execute_full_reconciliation

router = APIRouter()

@router.post("/reconcile")
async def trigger_reconciliation(start_date: str = None, end_date: str = None):
    report_file = await execute_full_reconciliation(start_date, end_date)
    if not report_file:
        raise HTTPException(status_code=500, detail="error al ejecutar la conciliacion")
    return {"message": "proceso completado", "report": report_file}

@router.get("/mvt", response_model=List[ScraperResult])
async def get_mvt_data():
    scraper = MVTScraper()
    return await scraper.scrape()

@router.get("/vgr", response_model=List[ScraperResult])
async def get_vgr_data():
    scraper = VGRScraper()
    return await scraper.scrape()

@router.get("/gr", response_model=List[ScraperResult])
async def get_gr_data():
    scraper = GRScraper()
    return await scraper.scrape()

@router.get("/first", response_model=List[ScraperResult])
async def get_first_data():
    scraper = FIRSTScraper()
    return await scraper.scrape()

@router.get("/lottingo", response_model=List[ScraperResult])
async def get_lottingo_data():
    scraper = LottingoScraper()
    return await scraper.scrape()

@router.get("/all", response_model=UnifiedScraperResponse)
async def get_all_data():
    mvt = MVTScraper()
    vgr = VGRScraper()
    gr = GRScraper()
    first = FIRSTScraper()
    lottingo = LottingoScraper()
    
    # ejecutar todos los scrapers (podria paralelizarse)
    results = []
    for s in [mvt, vgr, gr, first, lottingo]:
        try:
            res = await s.scrape()
            results.extend(res)
        except Exception as e:
            results.append(ScraperResult(source=s.name, data=None, status="error", message=str(e)))
            
    return UnifiedScraperResponse(results=results, total=len(results))
