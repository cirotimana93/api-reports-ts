import asyncio
import json
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from app.scrapers.mvt import MVTScraper
from app.scrapers.lottingo import LottingoScraper
from app.scrapers.vgr import VGRScraper
from app.scrapers.gr import GRScraper
from app.scrapers.first import FIRSTScraper
from app.logic.reconciliation import ReconciliationService

MAX_PROCESS_ATTEMPTS = 3
MAX_SCRAPER_ATTEMPTS = 3

def extractS3Key(pathOrUrl: str) -> str:
    if not pathOrUrl:
        return ""
    if pathOrUrl.startswith("s3://"):
        # devCt: extraer path relativo omitiendo el bucket
        parts = pathOrUrl.replace("s3://", "").split("/", 1)
        return parts[1] if len(parts) > 1 else ""
    return pathOrUrl

def extractUploadedKeys(scraperResult: Any) -> List[str]:
    keys = []
    if isinstance(scraperResult, list):
        for item in scraperResult:
            if isinstance(item, dict):
                if item.get("s3_key"):
                    keys.append(item["s3_key"])
                if item.get("s3_xlsx"):
                    keys.append(item["s3_xlsx"])
    elif isinstance(scraperResult, dict):
        if scraperResult.get("s3_key"):
            keys.append(scraperResult["s3_key"])
        if scraperResult.get("s3_xlsx"):
            keys.append(scraperResult["s3_xlsx"])
    return keys

def cleanS3Files(fileKeys: List[str]):
    # devCt: eliminar archivos creados en s3 para evitar conciliaciones parciales
    from app.common.s3_utils import delete_file_from_s3
    for key in set(fileKeys):
        cleanKey = extractS3Key(key)
        if cleanKey and cleanKey.startswith("tls/reports/") and "processed/" not in cleanKey:
            try:
                delete_file_from_s3(cleanKey)
                print(f"[ok] archivo parcial eliminado de s3: {cleanKey}")
            except Exception as err:
                print(f"[alerta] no se pudo eliminar {cleanKey} de s3: {err}")

def isScraperSuccessful(scraperName: str, results: Any) -> Tuple[bool, str]:
    if not results:
        return False, "sin respuesta del scraper"
    if not isinstance(results, list):
        return False, f"respuesta inesperada: {type(results)}"
    
    # devCt: first genera dos subreportes obligatorios para la conciliacion
    if scraperName == "FIRST":
        hasOpenbets = any(r.get("report") == "openbets" and r.get("status") == "success" for r in results)
        hasBethistory = any(r.get("report") == "bethistory" and r.get("status") == "success" for r in results)
        if not (hasOpenbets and hasBethistory):
            errorMsgs = [r.get("message", "fallo reporte") for r in results if r.get("status") == "error"]
            detail = "; ".join(errorMsgs) if errorMsgs else "falta openbets o bethistory"
            return False, detail
        return True, ""
        
    for item in results:
        if item.get("status") == "error":
            return False, item.get("message", "error reportado por el scraper")
        if item.get("status") != "success":
            return False, f"estado no exitoso: {item.get('status')}"
            
    return True, ""

async def runScraperWithRetries(scraper, startDate: str = None, endDate: str = None, maxAttempts: int = MAX_SCRAPER_ATTEMPTS):
    lastDetail = "error desconocido"
    for attempt in range(1, maxAttempts + 1):
        print(f"\n--- Ejecutando {scraper.name} (intento {attempt}/{maxAttempts}) ---")
        try:
            result = await scraper.scrape(start_date=startDate, end_date=endDate)
            isOk, detail = isScraperSuccessful(scraper.name, result)
            if isOk:
                print(f"Resultado {scraper.name}: exitoso")
                return True, result, ""
            lastDetail = detail
            print(f"[{scraper.name}] intento {attempt} fallo: {detail}")
        except Exception as err:
            lastDetail = str(err)
            print(f"[{scraper.name}] excepcion en intento {attempt}: {err}")

        if attempt < maxAttempts:
            # devCt: pausa breve antes de reintentar el scraper
            await asyncio.sleep(3)

    return False, None, lastDetail

async def sendFailureAlert(failedReports: Dict[str, str]):
    try:
        from app.common.mail import sendMailOffice365
        from app.logic.mail_formatter import format_error_email
        from app.core.config import settings

        emailRecipients = [e.strip() for e in settings.GRAPH_EMAIL_TO.split(",") if e.strip()]
        if not emailRecipients:
            print("[alerta] no hay destinatarios configurados en GRAPH_EMAIL_TO")
            return False

        subject = f"Alerta: Error en proceso de conciliacion - {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        emailContent = format_error_email(failedReports)

        sent = await sendMailOffice365(
            subject=subject,
            content=emailContent,
            to_recipients=emailRecipients
        )
        return sent
    except Exception as err:
        print(f"[alerta] no se pudo enviar correo de notificacion de error: {err}")
        return False

async def execute_full_reconciliation(start_date: str = None, end_date: str = None, custom_scrapers: Optional[List[Any]] = None):
    print("=== INICIANDO PROCESO DE CONCILIACION COMPLETO ===")
    if start_date:
        print(f"Rango de fechas: {start_date} a {end_date or start_date}")

    lastFailedReports = {}

    for processAttempt in range(1, MAX_PROCESS_ATTEMPTS + 1):
        print(f"\n>>> CICLO DE CONCILIACION {processAttempt}/{MAX_PROCESS_ATTEMPTS} <<<")

        # devCt: permitir inyeccion de scrapers para pruebas controladas
        if custom_scrapers is not None:
            scrapers = custom_scrapers
        else:
            scrapers = [
                LottingoScraper(),
                VGRScraper(),
                GRScraper(),
                FIRSTScraper(),
                MVTScraper()
            ]

        uploadedFiles = []
        failedReports = {}

        print("\n[STEP 1] Descargando reportes de proveedores...")
        for scraper in scrapers:
            success, result, errorDetail = await runScraperWithRetries(
                scraper, start_date, end_date, MAX_SCRAPER_ATTEMPTS
            )
            if success and result:
                uploadedFiles.extend(extractUploadedKeys(result))
            else:
                failedReports[scraper.name] = errorDetail

        # devCt: si algun scraper fallo tras sus 3 reintentos, se limpian archivos de s3 y se reintenta el ciclo completo
        if failedReports:
            print(f"\n[alerta] ciclo {processAttempt} incompleto. reportes con fallos: {list(failedReports.keys())}")
            lastFailedReports = failedReports

            print("[info] eliminando archivos subidos a s3 en este ciclo...")
            cleanS3Files(uploadedFiles)

            if processAttempt < MAX_PROCESS_ATTEMPTS:
                print(f"[info] esperando antes de reintentar ciclo completo ({processAttempt + 1}/{MAX_PROCESS_ATTEMPTS})...")
                await asyncio.sleep(5)
                continue
            else:
                print("[error] se agotaron los 3 ciclos del proceso de conciliacion")
                break

        # STEP 2: si todos los scrapers descargaron exitosamente
        print("\n[STEP 2] Iniciando analisis de conciliacion...")
        service = ReconciliationService()
        try:
            report_file = await service.run_reconciliation(start_date)
            if report_file:
                print(f"\n[SUCCESS] Conciliacion completada. Reporte generado: {report_file}")
                return report_file
            else:
                print("\n[alerta] conciliacion retorno vacio o incompleto")
                lastFailedReports = {"CONCILIACION": "reporte no generado o faltaron archivos requeridos"}
                cleanS3Files(uploadedFiles)
                if processAttempt < MAX_PROCESS_ATTEMPTS:
                    await asyncio.sleep(5)
                    continue
        except Exception as err:
            print(f"Error en servicio de conciliacion: {err}")
            lastFailedReports = {"CONCILIACION": str(err)}
            cleanS3Files(uploadedFiles)
            if processAttempt < MAX_PROCESS_ATTEMPTS:
                await asyncio.sleep(5)
                continue

    # devCt: al fallar los 3 ciclos completos, se notifica via correo y no se envia correo de conciliacion
    print("\n[alerta] enviando notificacion de error a optimizacion operativa...")
    await sendFailureAlert(lastFailedReports)
    return None

# devCt: alias camelcase segun reglas de desarrollo
executeFullReconciliation = execute_full_reconciliation

async def main():
    start_date = sys.argv[1] if len(sys.argv) > 1 else None
    end_date = sys.argv[2] if len(sys.argv) > 2 else None
    await execute_full_reconciliation(start_date, end_date)

if __name__ == "__main__":
    asyncio.run(main())

