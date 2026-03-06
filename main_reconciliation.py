import asyncio
import json
from app.scrapers.mvt import MVTScraper
from app.scrapers.lottingo import LottingoScraper
from app.scrapers.vgr import VGRScraper
from app.scrapers.gr import GRScraper
from app.scrapers.first import FIRSTScraper
from app.logic.reconciliation import ReconciliationService

async def main():
    print("=== INICIANDO PROCESO DE CONCILIACION ===")
    
    # 1. ejecutar scrapers
    scrapers = [
        MVTScraper(),
        LottingoScraper(),
        VGRScraper(),
        GRScraper(),
        FIRSTScraper()
    ]
    
    print("\n[STEP 1] Descargando reportes de proveedores...")
    for scraper in scrapers:
        try:
            print(f"\n--- Ejecutando {scraper.name} ---")
            result = await scraper.scrape()
            print(f"Resultado {scraper.name}: {json.dumps(result, indent=2)}")
        except Exception as e:
            print(f"Error en scraper {scraper.name}: {e}")

    # 2. ejecutar reconciliacion
    print("\n[STEP 2] Iniciando analisis de conciliacion...")
    service = ReconciliationService()
    try:
        # usamos la fecha de hoy por defecto
        report_file = await service.run_reconciliation(None)
        if report_file:
            print(f"\n[SUCCESS] Conciliacion completada. Reporte generado: {report_file}")
            print(f"Los reportes originales han sido movidos a la carpeta /processed en S3.")
        else:
            print("\n[ERROR] No se pudo generar el reporte de conciliacion.")
    except Exception as e:
        print(f"Error en servicio de conciliacion: {e}")

if __name__ == "__main__":
    asyncio.run(main())
