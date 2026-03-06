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
from app.scrapers.first_converter import convert_first_report

class FIRSTScraper(BaseScraper):
    def __init__(self):
        super().__init__(name="FIRST", base_url=settings.FIRST_URL)
        self.username = settings.FIRST_USER
        self.password = settings.FIRST_PASS
        self.api_url = "https://bo.firstsports.tech/api/auth/reports/openbets"
        self.bethistory_url = "https://bo.firstsports.tech/api/auth/reports/bethistory"
        self.declinedbets_url = "https://bo.firstsports.tech/api/auth/reports/declinedbets"

    async def get_auth_info(self) -> Optional[Dict]:
        """login completo: auth0 -> seleccion de corporativo -> captura token hs256"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
            )
            page = await context.new_page()

            # el token hs256 aparece en los headers de las peticiones al api tras el login
            session_data = {"token": None, "corp": "280"}
            token_event = asyncio.Event()

            async def handle_request(request):
                # interceptar token hs256 del engine (distinto al rs256 de auth0)
                if "/api/auth/" in request.url and not token_event.is_set():
                    h = request.headers
                    if "x-access-token" in h:
                        token = h["x-access-token"]
                        if "eyJhbGciOiJIUzI1NiI" in token:
                            session_data["token"] = token
                            session_data["corp"] = h.get("x-corp", "280")
                            print(f"[{self.name}] token hs256 capturado")
                            token_event.set()

            page.on("request", handle_request)

            try:
                print(f"[{self.name}] navegando a {self.base_url}")
                await page.goto(self.base_url, timeout=60000, wait_until="load")
                await asyncio.sleep(3)

                # clic en log in inicial si aparece antes del modal de auth0
                try:
                    btn = page.locator('button:has-text("Log In")')
                    if await btn.count() > 0:
                        await btn.first.click()
                        await asyncio.sleep(4)
                except Exception:
                    pass

                # login en auth0
                print(f"[{self.name}] esperando formulario de auth0")
                await page.wait_for_selector('input[name="email"]', timeout=30000)
                await page.fill('input[name="email"]', self.username)
                await page.fill('input[name="password"]', self.password)
                await page.click('button.auth0-lock-submit')

                # esperar redireccion de auth0 hacia /choose-corporate
                print(f"[{self.name}] esperando redireccion de auth0...")
                try:
                    await page.wait_for_url(
                        lambda url: "choose-corporate" in url or ("/signIn" not in url and "auth0" not in url),
                        timeout=30000
                    )
                except Exception:
                    pass
                await asyncio.sleep(2)
                print(f"[{self.name}] url post-login: {page.url}")

                # seleccion de corporativo en /choose-corporate
                print(f"[{self.name}] revisando seleccion de corporativo")
                try:
                    current_url = page.url

                    if "choose-corporate" in current_url or "corporate" in current_url.lower():
                        print(f"[{self.name}] en pagina de seleccion de corporativo")

                        # abrir el dropdown
                        dropdown_btn = page.locator('.SelDropdownButton')
                        if await dropdown_btn.count() > 0:
                            await dropdown_btn.first.click()
                            await asyncio.sleep(1)

                        # seleccionar apuestatotal por data-value o por texto
                        at_item = page.locator('li[data-value="280"]')
                        if await at_item.count() > 0:
                            print(f"[{self.name}] seleccionando apuestatotal")
                            await at_item.first.click(force=True)
                            await asyncio.sleep(2)
                        else:
                            at_text = page.locator('li:has-text("ApuestaTotal")')
                            if await at_text.count() > 0:
                                await at_text.first.click(force=True)
                                await asyncio.sleep(2)

                        # clic en log in para confirmar la seleccion
                        btn_login = page.locator('.MuiDialogActions-root button')
                        if await btn_login.count() == 0:
                            btn_login = page.locator('button:has-text("Log In")')
                        if await btn_login.count() > 0:
                            print(f"[{self.name}] confirmando seleccion con log in")
                            await btn_login.first.click(force=True)
                            try:
                                await page.wait_for_url(
                                    lambda url: "choose-corporate" not in url,
                                    timeout=20000
                                )
                            except Exception:
                                pass
                            await asyncio.sleep(3)

                    elif await page.locator('li[data-value="280"]').count() > 0:
                        await page.locator('li[data-value="280"]').first.click(force=True)
                        await asyncio.sleep(2)
                        btn_login = page.locator('.MuiDialogActions-root button')
                        if await btn_login.count() > 0:
                            await btn_login.first.click(force=True)
                            await asyncio.sleep(5)

                except Exception as exc:
                    print(f"[{self.name}] aviso seleccion corporativo: {exc}")

                print(f"[{self.name}] url post-corporate: {page.url}")

                # ir a reportes para que el dashboard dispare el api y genere el token hs256
                print(f"[{self.name}] navegando a reportes...")
                try:
                    await page.goto(
                        "https://bo.firstsports.tech/reports/open-bets",
                        timeout=30000, wait_until="load"
                    )
                except Exception:
                    pass

                # esperar hasta 20s a que el token hs256 sea capturado
                print(f"[{self.name}] esperando token hs256...")
                try:
                    await asyncio.wait_for(token_event.wait(), timeout=20)
                except asyncio.TimeoutError:
                    print(f"[{self.name}] timeout esperando token hs256")

                if not session_data["token"]:
                    print(f"[{self.name}] error: no se capturo el token hs256")
                    return None

                # capturar cookies de sesion
                cookies_list = await context.cookies()
                session_data["cookies"] = "; ".join(
                    [f"{c['name']}={c['value']}" for c in cookies_list]
                )
                print(f"[{self.name}] sesion lista. corp={session_data['corp']}, cookies={len(cookies_list)}")
                return session_data

            except Exception as e:
                print(f"[{self.name}] error en get_auth_info: {e}")
                return None
            finally:
                await browser.close()

    async def _fetch_report(self, auth_info: Dict, url: str, base_payload: Dict, label: str) -> Any:
        """metodo generico de extraccion con paginacion para cualquier endpoint de first"""
        all_data = []
        page = 1
        limit = 500
        total_records = 0

        headers = {
            "x-access-token": auth_info["token"],
            "x-corp": auth_info["corp"],
            "x-language": "en",
            "content-type": "application/json",
            "accept": "application/json, text/plain, */*",
            "origin": "https://bo.firstsports.tech",
            "referer": "https://bo.firstsports.tech",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "cookie": auth_info.get("cookies", "")
        }

        async with httpx.AsyncClient() as client:
            while True:
                payload = {**base_payload, "pageNumber": page, "pageSize": limit}

                print(f"[{self.name}][{label}] extrayendo pagina {page}...")
                response = await client.post(url, json=payload, headers=headers, timeout=60.0)

                if response.status_code != 200:
                    print(f"[{self.name}][{label}] error en api (pagina {page}): {response.status_code}")
                    break

                result = response.json()
                data_obj = result.get("data", {})
                if not data_obj:
                    break

                page_data = data_obj.get("list", [])
                total_records = data_obj.get("total", 0)

                all_data.extend(page_data)
                print(f"[{self.name}][{label}] progreso: {len(all_data)} / {total_records}")

                if len(all_data) >= total_records or not page_data:
                    break

                page += 1
                await asyncio.sleep(0.5)

        return {"data": all_data, "total": total_records}

    async def _fetch_openbets_data(self, auth_info: Dict, start_date: str, end_date: str) -> Any:
        """payload para openbets"""
        from_iso = f"{start_date}T05:00:00.000000Z"
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        to_iso = f"{end_dt.strftime('%Y-%m-%d')}T04:59:59.999999Z"
        payload = {
            "orderBy": "CreationDate", "orderDirection": "DESC",
            "from": from_iso, "to": to_iso,
            "displayCurrency": [40, 21, 6, 27, 13, 14, 8, 2, 1, 3],
            "freeBetSource": [0, 1, 2, 3],
            "gamificationId": "", "isTotal": False,
            "customerID": "", "username": "", "merchantCustomerCode": "",
            "betSlipCode": "", "amountCondition": "", "amountCurrency": "PlayerCurrency",
            "amountType": -1, "betSlip": -1, "boostedOdds": -1, "comboBonus": -1,
            "contributionType": -2, "crossBetting": -1, "depositBonus": -1,
            "featuredSelections": -1, "liveOrPrematch": -1, "quickBet": -1,
            "semiManaged": -1, "testAccount": -1,
            "additionalTicketsType": [], "betTypes": [], "bettingView": [],
            "brands": [], "clientTypes": [], "corporates": [], "countries": [],
            "currencies": [], "customerLevels": [], "customerTags": [],
            "eventTypes": [], "events": [], "leagueGroups": [], "leagues": [],
            "operators": [], "platform": [], "promotionId": [], "sports": []
        }
        return await self._fetch_report(auth_info, self.api_url, payload, "openbets")

    async def _fetch_bethistory_data(self, auth_info: Dict, start_date: str, end_date: str) -> Any:
        """payload para bethistory"""
        from_iso = f"{start_date}T05:00:00.000000Z"
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        to_iso = f"{end_dt.strftime('%Y-%m-%d')}T04:59:59.999999Z"
        payload = {
            "orderBy": "CreationDate", "orderDirection": "DESC",
            "betPlacementDateFrom": from_iso, "betPlacementDateTo": to_iso,
            "betSettledDateFrom": None, "betSettledDateTo": None,
            "displayCurrency": [40, 21, 6, 27, 13, 14, 8, 2, 1, 3],
            "freeBetSource": [0, 1, 2, 3], "freeBetIds": "",
            "gamificationId": "", "isTotal": False,
            "customerID": "", "username": "", "merchantCustomerCode": "",
            "betSlipCode": "", "amountCondition": "", "amountCurrency": "PlayerCurrency",
            "amountType": -1, "betSlip": -1, "boostedOdds": -1, "comboBonus": -1,
            "contributionType": -2, "crossBetting": -1, "depositBonus": -1,
            "earlyPayout": -1, "featuredSelections": -1, "liveOrPrematch": -1,
            "quickBet": -1, "resettled": -1, "semiManaged": -1, "testAccount": -1,
            "additionalTicketsType": [], "betStatus": [], "betTypes": [], "bettingView": [],
            "brands": [], "clientTypes": [], "corporates": [], "countries": [],
            "currencies": [], "customerLevels": [], "customerTags": [],
            "eventTypes": [], "events": [], "leagueGroups": [], "leagues": [],
            "operators": [], "platform": [], "promotionId": [], "selection": [], "sports": []
        }
        return await self._fetch_report(auth_info, self.bethistory_url, payload, "bethistory")

    async def _fetch_declinedbets_data(self, auth_info: Dict, start_date: str, end_date: str) -> Any:
        """payload para declinedbets"""
        from_iso = f"{start_date}T05:00:00.000000Z"
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        to_iso = f"{end_dt.strftime('%Y-%m-%d')}T04:59:59.999999Z"
        payload = {
            "orderBy": "CreationDate", "orderDirection": "DESC",
            "from": from_iso, "to": to_iso,
            "customerID": "", "username": "", "fullName": "", "merchantCustomerCode": "",
            "declinedDetails": "", "testAccount": -1,
            "brands": [], "corporates": [], "currencies": [],
            "declinedDetailID": [], "declinedReasonID": [], "declinedTypes": [],
            "eventTypes": [], "events": [], "leagues": [], "operators": [], "sports": []
        }
        return await self._fetch_report(auth_info, self.declinedbets_url, payload, "declinedbets")


    async def scrape(self, start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[Any]:
        """flujo principal: login, extraccion de 3 reportes y guardado en s3"""
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

        print(f"fechas recibidas: {s_date} - {e_date}")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        results = []

        # definir los 3 reportes a extraer
        reports = [
            ("openbets",     self._fetch_openbets_data),
            ("bethistory",   self._fetch_bethistory_data),
            ("declinedbets", self._fetch_declinedbets_data),
        ]

        for report_name, fetch_fn in reports:
            print(f"\n[{self.name}] iniciando reporte: {report_name}")
            report_data = await fetch_fn(auth_info, s_date, e_date)
            count = len(report_data["data"])

            if not count:
                print(f"[{self.name}][{report_name}] sin datos")
                results.append({
                    "source": self.name,
                    "report": report_name,
                    "status": "success",
                    "message": "sin datos",
                    "count": 0
                })
                continue

            # serializar json en memoria y subir directo a s3
            json_filename = f"{self.name.lower()}_{report_name}_{s_date.replace('-','')}_{e_date.replace('-','')}_{timestamp}.json"
            json_bytes = json.dumps(report_data, indent=4, ensure_ascii=False).encode("utf-8")

            s3_json_key = f"tls/reports/{json_filename}"
            upload_file_to_s3(json_bytes, s3_json_key)
            print(f"[{self.name}][{report_name}] json subido: {s3_json_key}")

            # convertir json -> xlsx y subir a s3/tls/reports/
            try:
                xlsx_bytes = convert_first_report(report_name, report_data["data"])
                xlsx_filename = json_filename.replace(".json", ".xlsx")
                s3_xlsx_key = f"tls/reports/{xlsx_filename}"
                upload_file_to_s3(xlsx_bytes, s3_xlsx_key)
                print(f"[{self.name}][{report_name}] xlsx subido: {s3_xlsx_key}")
            except Exception as exc:
                print(f"[{self.name}][{report_name}] error generando xlsx: {exc}")
                s3_xlsx_key = ""

            # mover json a s3/tls/reports/processed/
            s3_processed_key = f"tls/reports/processed/{json_filename}"
            try:
                copy_file_in_s3(s3_json_key, s3_processed_key)
                delete_file_from_s3(s3_json_key)
                print(f"[{self.name}][{report_name}] json movido a processed/")
            except Exception as exc:
                print(f"[{self.name}][{report_name}] aviso al mover json: {exc}")

            results.append({
                "source": self.name,
                "report": report_name,
                "status": "success",
                "count": count,
                "total": report_data["total"],
                "s3_json": f"s3://prvfr-dev-s3bucket-ue01-001/{s3_processed_key}",
                "s3_xlsx": f"s3://prvfr-dev-s3bucket-ue01-001/{s3_xlsx_key}" if s3_xlsx_key else "",
            })

        return results

if __name__ == "__main__":
    async def test():
        scraper = FIRSTScraper()
        result = await scraper.scrape()
        print(json.dumps(result, indent=2))
    
    asyncio.run(test())
