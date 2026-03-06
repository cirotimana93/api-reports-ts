import os
import asyncio
from typing import Any, List, Optional, Dict
from playwright.async_api import async_playwright
from app.common.base_scraper import BaseScraper
from app.core.config import settings
from app.common.s3_utils import upload_file_to_s3, delete_file_from_s3, copy_file_in_s3
from app.scrapers.mvt_converter import json_to_excel_mvt
import json
import httpx
from datetime import datetime

class MVTScraper(BaseScraper):
    def __init__(self):
        super().__init__(name="MVT", base_url=settings.MVT_URL)
        self.username = settings.MVT_USER
        self.password = settings.MVT_PASS
        self.api_url = "https://gddhbny0ul.execute-api.us-east-1.amazonaws.com/mvt-report/v1/telesales/transactions"

    async def get_token(self) -> Optional[str]:
        """realiza el login y captura el token de acceso"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            token = {"value": None}

            async def handle_response(response):
                if "openid-connect/token" in response.url and response.status == 200:
                    try:
                        data = await response.json()
                        if "access_token" in data:
                            token["value"] = data["access_token"]
                            print(f"[{self.name}] token capturado desde la red")
                    except Exception:
                        pass

            page.on("response", handle_response)

            try:
                print(f"[{self.name}] iniciando login para obtener token...")
                await page.goto(self.base_url, timeout=60000)
                await page.wait_for_load_state("networkidle")
                
                try:
                    # esperar un poco por si hay redirecciones
                    await asyncio.sleep(2)
                    
                    # buscar campo username usando wait_for_selector (que soporta timeout)
                    print(f"[{self.name}] esperando campo de usuario...")
                    await page.wait_for_selector("#username", timeout=10000)
                    
                    if await page.locator("#username").is_visible():
                        print(f"[{self.name}] formulario encontrado, enviando credenciales...")
                        await page.fill("#username", self.username)
                        await page.fill("#password", self.password)
                        
                        async with page.expect_response("**/protocol/openid-connect/token", timeout=30000) as response_info:
                            await page.click('input[type="submit"]')
                        
                        response = await response_info.value
                        if response.status == 200:
                            data = await response.json()
                            token["value"] = data.get("access_token")
                except Exception as e:
                    print(f"[{self.name}] nota: formulario no detectado o error menor: {str(e)}")

                if not token["value"]:
                    token["value"] = await page.evaluate("() => localStorage.getItem('access_token')")

                return token["value"]
            finally:
                await browser.close()

    async def _fetch_api_data(self, token: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> Any:
        """consulta el api de transacciones extrayendo todas las paginas para un rango de fechas"""
        today = datetime.now().strftime("%Y-%m-%d")
        
        # logica de fechas: si falta una, ambas son hoy
        s_date = start_date if start_date else today
        e_date = end_date if end_date else today
        
        if not start_date or not end_date:
            s_date = today
            e_date = today

        # validacion: start_date <= end_date
        try:
            if datetime.strptime(s_date, "%Y-%m-%d") > datetime.strptime(e_date, "%Y-%m-%d"):
                print(f"[{self.name}] alerta: la fecha de inicio ({s_date}) no puede ser mayor a la fecha fin ({e_date})")
                return {"data": [], "count": 0, "error": "fecha invalida"}
        except ValueError:
            print(f"[{self.name}] alerta: formato de fecha invalido. debe ser YYYY-MM-DD")
            return {"data": [], "count": 0, "error": "formato invalido"}

        all_data = []
        page = 1
        limit = 10000 
        total_count = None
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://mvt.aterax.at",
            "Referer": "https://mvt.aterax.at/"
        }
        
        async with httpx.AsyncClient() as client:
            try:
                while True:
                    params = {
                        "busqueda_fecha_inicio": s_date,
                        "busqueda_fecha_fin": e_date,
                        "cargo_id": "19",
                        "usuario_id": "4595",
                        "tipo_saldo": "1",
                        "page": str(page),
                        "limit": str(limit)
                    }
                    
                    print(f"[{self.name}] extrayendo datos desde {s_date} hasta {e_date}, pagina {page}...")
                    response = await client.get(self.api_url, params=params, headers=headers, timeout=60.0)
                    
                    if response.status_code != 200:
                        print(f"[{self.name}] error api en pagina {page}: {response.status_code}")
                        break
                        
                    result = response.json()
                    page_data = result.get("data", [])
                    total_count = result.get("count", 0)
                    
                    all_data.extend(page_data)
                    print(f"[{self.name}] progreso: {len(all_data)} / {total_count}")
                    
                    if len(all_data) >= total_count or not page_data:
                        break
                        
                    page += 1
                    await asyncio.sleep(0.5)
                
                return {
                    "data": all_data,
                    "count": total_count
                }
            except Exception as e:
                print(f"[{self.name}] error consulta api: {str(e)}")
                return {"data": all_data, "count": total_count or len(all_data)}



    async def scrape(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Any]:
        """flujo principal: login -> fetch -> save con rango de fechas"""
        token = await self.get_token()
        if not token:
            return [{"source": self.name, "status": "error", "message": "no se obtuvo token"}]

        # normalizar fechas para el guardado y logica
        today = datetime.now().strftime("%Y-%m-%d")
        s_date = start_date if start_date and end_date else today
        e_date = end_date if start_date and end_date else today

        report_data = await self._fetch_api_data(token, start_date=start_date, end_date=end_date)
        
        if "error" in report_data:
            return [{"source": self.name, "status": "error", "message": report_data["error"]}]

        if not report_data.get("data"):
            return [{"source": self.name, "status": "error", "message": "no se obtuvieron datos para el rango"}]

        # serializar json en memoria y subir directo a s3
        timestamp = datetime.now().strftime("%H%M%S")
        s_tag = s_date.replace("-", "")
        e_tag = e_date.replace("-", "")
        json_filename = f"{self.name.lower()}_reporte_{s_tag}_{e_tag}_{timestamp}.json"
        json_bytes = json.dumps(report_data, indent=4, ensure_ascii=False).encode("utf-8")

        items = report_data.get("data", [])
        count = len(items)

        # subir json a s3/tls/reports/
        s3_json_key = f"tls/reports/{json_filename}"
        upload_file_to_s3(json_bytes, s3_json_key)
        print(f"[{self.name}] json subido: {s3_json_key}")

        # convertir json -> xlsx y subir a s3/tls/reports/
        try:
            xlsx_bytes = json_to_excel_mvt(items)
            xlsx_filename = json_filename.replace(".json", ".xlsx")
            s3_xlsx_key = f"tls/reports/{xlsx_filename}"
            upload_file_to_s3(xlsx_bytes, s3_xlsx_key)
            print(f"[{self.name}] xlsx subido: {s3_xlsx_key}")
        except Exception as exc:
            print(f"[{self.name}] error generando xlsx: {exc}")
            s3_xlsx_key = ""

        # mover json a s3/tls/reports/processed/
        s3_processed_key = f"tls/reports/processed/{json_filename}"
        try:
            copy_file_in_s3(s3_json_key, s3_processed_key)
            delete_file_from_s3(s3_json_key)
            print(f"[{self.name}] json movido a processed/")
        except Exception as exc:
            print(f"[{self.name}] aviso al mover json: {exc}")

        return [{
            "source": self.name,
            "status": "success",
            "count": count,
            "total": report_data.get("count", 0),
            "s3_json": f"s3://prvfr-dev-s3bucket-ue01-001/{s3_processed_key}",
            "s3_xlsx": f"s3://prvfr-dev-s3bucket-ue01-001/{s3_xlsx_key}" if s3_xlsx_key else "",
        }]

if __name__ == "__main__":
    import asyncio
    # ejemplo de uso con rango manual
    # asyncio.run(MVTScraper().scrape(start_date="2026-03-03", end_date="2026-03-03"))
    asyncio.run(MVTScraper().scrape())

