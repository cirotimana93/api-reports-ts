import asyncio
import json
import sys
from datetime import datetime
from app.scrapers.mvt import MVTScraper
from app.scrapers.lottingo import LottingoScraper
from app.scrapers.vgr import VGRScraper
from app.scrapers.gr import GRScraper
from app.scrapers.first import FIRSTScraper
from app.logic.reconciliation import ReconciliationService

async def execute_full_reconciliation(start_date: str = None, end_date: str = None):
    print("=== INICIANDO PROCESO DE CONCILIACION COMPLETO ===")
    if start_date:
        print(f"Rango de fechas: {start_date} a {end_date or start_date}")
    
    # 1. ejecutar scrapers
    scrapers = [
        LottingoScraper(),
        VGRScraper(),
        GRScraper(),
        FIRSTScraper(),
        MVTScraper()
    ]
    
    print("\n[STEP 1] Descargando reportes de proveedores...")
    for scraper in scrapers:
        try:
            print(f"\n--- Ejecutando {scraper.name} ---")
            # pasar el rango de fechas al scraper
            result = await scraper.scrape(start_date=start_date, end_date=end_date)
            print(f"Resultado {scraper.name}: {json.dumps(result, indent=2)}")
        except Exception as e:
            print(f"Error en scraper {scraper.name}: {e}")

    # 2. ejecutar reconciliacion
    print("\n[STEP 2] Iniciando analisis de conciliacion...")
    service = ReconciliationService()
    try:
        # la conciliacion tomara los ultimos archivos descargados por los scrapers
        report_file = await service.run_reconciliation(start_date)
        if report_file:
            print(f"\n[SUCCESS] Conciliacion completada. Reporte generado: {report_file}")
            return report_file
        else:
            print("\n[ERROR] No se pudo generar el reporte de conciliacion.")
            return None
    except Exception as e:
        print(f"Error en servicio de conciliacion: {e}")
        return None

async def main():
    # capturar fechas de argumentos si existen
    start_date = sys.argv[1] if len(sys.argv) > 1 else None
    end_date = sys.argv[2] if len(sys.argv) > 2 else None
    
    await execute_full_reconciliation(start_date, end_date)

if __name__ == "__main__":
    asyncio.run(main())
