import os
import asyncio
import json
import httpx
from datetime import datetime, timedelta
from typing import Any, List, Optional, Dict
from playwright.async_api import async_playwright
from app.common.base_scraper import BaseScraper
from app.common.s3_utils import upload_file_to_s3, delete_file_from_s3, copy_file_in_s3
from app.core.config import settings
from app.scrapers.vgr_converter import json_to_excel_vgr

class VGRScraper(BaseScraper):
    def __init__(self):
        super().__init__(name="VGR", base_url=settings.VGR_URL)
        self.domain = settings.VGR_DOMINIO
        self.username = settings.VGR_USER
        self.password = settings.VGR_PASS
        self.api_url = "https://america-manager.virtustec.com/manager/manager-api-ws/api/manager/v0.1/ticket/find"
        self.entity_id = "1776922"

    async def get_auth_info(self) -> Optional[Dict]:
        """login en vgr y captura el bearer token desde los headers de las peticiones al api"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            session_data = {"token": None}
            token_event = asyncio.Event()

            async def handle_request(request):
                # el bearer token valido aparece en las peticiones al api de tickets
                if "/manager-api-ws/api/" in request.url and not token_event.is_set():
                    auth_header = request.headers.get("authorization", "")
                    if auth_header.startswith("Bearer "):
                        session_data["token"] = auth_header[len("Bearer "):]
                        print(f"[{self.name}] token capturado desde headers del api")
                        token_event.set()

            page.on("request", handle_request)

            try:
                print(f"[{self.name}] navegando a {self.base_url}")
                await page.goto(self.base_url, timeout=60000, wait_until="networkidle")

                # llenar formulario de login
                print(f"[{self.name}] esperando formulario de login")
                await page.wait_for_selector('input[formcontrolname="domain"]', timeout=15000)

                await page.click('input[formcontrolname="domain"]')
                await page.type('input[formcontrolname="domain"]', self.domain, delay=50)

                await page.click('input[formcontrolname="username"]')
                await page.type('input[formcontrolname="username"]', self.username, delay=50)

                await page.click('input[type="password"]')
                await page.type('input[type="password"]', self.password, delay=50)

                print(f"[{self.name}] enviando formulario de login")
                await page.click('button[type="submit"]')
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(3)

                # si el token aun no fue capturado, navegar a la seccion de tickets para disparar el api
                if not token_event.is_set():
                    print(f"[{self.name}] navegando a reportes para capturar token...")
                    try:
                        await page.goto(
                            f"{self.base_url}reports/tickets",
                            timeout=20000, wait_until="networkidle"
                        )
                        await asyncio.sleep(3)
                    except Exception:
                        pass

                # esperar hasta 15s al token
                try:
                    await asyncio.wait_for(token_event.wait(), timeout=15)
                except asyncio.TimeoutError:
                    print(f"[{self.name}] timeout esperando token")

                if not session_data["token"]:
                    print(f"[{self.name}] error: no se capturo el token")
                    return None

                print(f"[{self.name}] sesion lista")
                return session_data

            except Exception as e:
                print(f"[{self.name}] error en get_auth_info: {e}")
                return None
            finally:
                await browser.close()

    async def _fetch_api_data(self, auth_info: Dict, start_date: str, end_date: str) -> Any:
        """extrae datos del api de tickets con paginacion por offset"""
        # formatear fechas
        from_iso = f"{start_date}T05:00:00.000Z"
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        to_iso = f"{end_dt.strftime('%Y-%m-%d')}T04:59:59.999Z"

        print(f"fechas recibidas: {start_date} - {end_date}")
        print(f"fechas formateadas: {from_iso} - {to_iso}")

        all_data = []
        offset = 0
        page_size = 1000
        total_records = None

        headers = {
            "Authorization": f"Bearer {auth_info['token']}",
            "accept": "application/json",
            "content-type": "application/json",
            "origin": "https://america-admin.virtustec.com",
            "referer": "https://america-admin.virtustec.com/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

        async with httpx.AsyncClient() as client:
            while True:
                params = {
                    "entityId": self.entity_id,
                    "startTime": from_iso,
                    "endTime": to_iso,
                    "first": offset,
                    "n": page_size,
                    "levelDetails": "1",
                    "orderBy": "DESC",
                    "status": "",
                    "withChildren": "true"
                }

                print(f"[{self.name}] extrayendo offset {offset}...")
                response = await client.get(self.api_url, params=params, headers=headers, timeout=60.0)

                if response.status_code != 200:
                    print(f"[{self.name}] error en api (offset {offset}): {response.status_code}")
                    break

                page_data = response.json()

                if not isinstance(page_data, list) or not page_data:
                    break

                all_data.extend(page_data)

                # obtener total del header x-total-count si existe, si no inferir por el tamanio de la pagina
                if total_records is None:
                    total_header = response.headers.get("x-total-count") or response.headers.get("X-Total-Count")
                    if total_header:
                        total_records = int(total_header)

                count_label = f"/ {total_records}" if total_records else ""
                print(f"[{self.name}] progreso: {len(all_data)} {count_label}")

                # si la pagina devolvio menos registros de los solicitados, ya termino
                if len(page_data) < page_size:
                    break

                offset += page_size
                await asyncio.sleep(0.5)

        return {
            "data": all_data,
            "total": total_records or len(all_data)
        }

    async def scrape(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Any]:
        """flujo principal: login, extraccion y guardado de datos"""
        today = datetime.now().strftime("%Y-%m-%d")
        s_date = start_date if start_date else today
        e_date = end_date if end_date else today

        print(f"fechas enviadas: {s_date} - {e_date}")

        # validacion de fechas
        try:
            if datetime.strptime(s_date, "%Y-%m-%d") > datetime.strptime(e_date, "%Y-%m-%d"):
                print(f"[{self.name}] fecha inicio mayor a fecha fin")
                return [{"source": self.name, "status": "error", "message": "fecha invalida"}]
        except ValueError:
            print(f"[{self.name}] formato de fecha invalido")
            return [{"source": self.name, "status": "error", "message": "formato invalido"}]

        auth_info = await self.get_auth_info()
        if not auth_info:
            return [{"source": self.name, "status": "error", "message": "error de autenticacion"}]

        report_data = await self._fetch_api_data(auth_info, s_date, e_date)

        if not report_data["data"]:
            print(f"[{self.name}] no se encontraron registros")
            return [{"source": self.name, "status": "success", "message": "sin datos", "count": 0}]

        items = report_data.get("data", [])
        count = len(items)

        # serializar json en memoria y subir directo a s3
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"{self.name.lower()}_reporte_{s_date.replace('-','')}_{e_date.replace('-','')}_{timestamp}.json"
        json_bytes = json.dumps(report_data, indent=4, ensure_ascii=False).encode("utf-8")

        s3_json_key = f"tls/reports/{json_filename}"
        upload_file_to_s3(json_bytes, s3_json_key)
        print(f"[{self.name}] json subido: {s3_json_key}")

        # convertir json -> xlsx y subir a s3/tls/reports/
        try:
            xlsx_bytes = json_to_excel_vgr(items)
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
            "total": report_data["total"],
            "s3_json": f"s3://prvfr-dev-s3bucket-ue01-001/{s3_processed_key}",
            "s3_xlsx": f"s3://prvfr-dev-s3bucket-ue01-001/{s3_xlsx_key}" if s3_xlsx_key else "",
        }]

if __name__ == "__main__":
    async def test():
        scraper = VGRScraper()
        result = await scraper.scrape()
        print(json.dumps(result, indent=2))

    asyncio.run(test())
