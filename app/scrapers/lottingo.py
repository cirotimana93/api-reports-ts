import os
import asyncio
import json
import httpx
from datetime import datetime, timedelta
from typing import Any, List, Optional, Dict
from playwright.async_api import async_playwright
from app.common.base_scraper import BaseScraper
from app.common.s3_utils import upload_file_to_s3
from app.core.config import settings

class LottingoScraper(BaseScraper):
    def __init__(self):
        super().__init__(name="LOTTINGO", base_url=settings.LOT_URL)
        self.username = settings.LOT_USER
        self.password = settings.LOT_PASS
        self.report_url = "https://gestion.apuestatotal.com/fastreport/bingos/"
    async def get_auth_info(self) -> Optional[Dict]:
        """login en lottingo y captura las cookies de sesion"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            try:
                print(f"[{self.name}] navegando a {self.base_url}")
                await page.goto(self.base_url, timeout=60000, wait_until="networkidle")

                # llenar formulario de login
                print(f"[{self.name}] esperando formulario de login")
                await page.wait_for_selector('input[name="fr_username"]', timeout=15000)

                await page.fill('input[name="fr_username"]', self.username)
                await page.fill('input[name="fr_password"]', self.password)

                print(f"[{self.name}] enviando formulario de login")
                await page.click('button[name="fr_login"]')
                await page.wait_for_load_state("networkidle")

                # verificar login exitoso
                form_present = await page.locator('form:has(input[name="fecha_inicio"])').count()
                if form_present > 0:
                    print(f"[{self.name}] login exitoso")
                else:
                    print(f"[{self.name}] error: no se detecto el formulario post-login")
                    return None

                # capturar cookies de sesion
                raw_cookies = await context.cookies()
                cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in raw_cookies])
                print(f"[{self.name}] sesion lista. cookies={len(raw_cookies)}")
                return {"cookies": cookie_str}

            except Exception as e:
                print(f"[{self.name}] error en get_auth_info: {e}")
                return None
            finally:
                await browser.close()

    async def _download_excel(self, auth_info: Dict, start_date: str, end_date: str) -> Any:
        """descarga el excel via get con redireccion y lo sube directamente a s3/tls/reports/"""
        # el servidor espera fecha_fin como el dia siguiente
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        fecha_fin_param = end_dt.strftime("%Y-%m-%d")

        params = {
            "fecha_inicio": start_date,
            "fecha_fin": fecha_fin_param,
            "zona_id": ""
        }

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "referer": self.report_url,
            "cookie": auth_info["cookies"],
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        }

        print(f"fechas recibidas: {start_date} - {end_date}")
        print(f"parametros enviados: fecha_inicio={start_date}, fecha_fin={fecha_fin_param}")

        # httpx sigue el redirect automaticamente y descarga el excel
        async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
            print(f"[{self.name}] solicitando reporte...")
            response = await client.get(self.report_url, params=params, headers=headers)

            if response.status_code != 200:
                print(f"[{self.name}] error al descargar: {response.status_code}")
                return None

            content_type = response.headers.get("content-type", "")
            content_length = len(response.content)
            print(f"[{self.name}] descarga exitosa. content-type: {content_type}, bytes: {content_length}")

            # construir nombre del archivo para s3
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.name.lower()}_reporte_{start_date.replace('-','')}_{end_date.replace('-','')}_{timestamp}.xls"

            # subir a s3
            s3_key = f"tls/reports/{filename}"
            upload_file_to_s3(response.content, s3_key)

            return {"s3_key": s3_key, "size_bytes": content_length}

    async def scrape(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Any]:
        # flujo principal con reintentos para manejar fallos de red
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

        max_retries = 3
        last_error = ""

        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    wait_time = attempt * 5
                    print(f"[{self.name}] reintento {attempt}/{max_retries} en {wait_time}s...")
                    await asyncio.sleep(wait_time)

                auth_info = await self.get_auth_info()
                if not auth_info:
                    last_error = "error de autenticacion"
                    continue

                result = await self._download_excel(auth_info, s_date, e_date)
                if not result:
                    last_error = "error al descargar el excel"
                    continue

                return [{
                    "source": self.name,
                    "status": "success",
                    "s3_key": result["s3_key"],
                    "size_bytes": result["size_bytes"]
                }]

            except Exception as e:
                last_error = str(e)
                print(f"[{self.name}] error en intento {attempt}: {e}")
                if "connection" in last_error.lower() or "timeout" in last_error.lower() or "failed" in last_error.lower():
                    continue
                else:
                    break

        return [{"source": self.name, "status": "error", "message": f"fallo tras {max_retries} intentos. ultimo error: {last_error}"}]

if __name__ == "__main__":
    async def test():
        scraper = LottingoScraper()
        result = await scraper.scrape()
        print(json.dumps(result, indent=2))

    asyncio.run(test())
