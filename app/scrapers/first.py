import os
import asyncio
import json
import httpx
from datetime import datetime, timedelta
from typing import Any, List, Optional, Dict
from playwright.async_api import async_playwright
from app.common.base_scraper import BaseScraper
from app.core.config import settings

class FIRSTScraper(BaseScraper):
    def __init__(self):
        super().__init__(name="FIRST", base_url=settings.FIRST_URL)
        self.username = settings.FIRST_USER
        self.password = settings.FIRST_PASS
        self.api_url = "https://bo.firstsports.tech/api/auth/reports/openbets"
        self.data_dir = "data"
        
        # crear carpeta de datos si no existe
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)

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

    async def _fetch_api_data(self, auth_info: Dict, start_date: str, end_date: str) -> Any:
        """consulta el api de openbets con paginacion"""
        from_iso = f"{start_date}T05:00:00.000000Z"
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)
        to_iso = f"{end_dt.strftime('%Y-%m-%d')}T04:59:59.999999Z"

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
            "referer": "https://bo.firstsports.tech/reports/open-bets",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "cookie": auth_info.get("cookies", "")
        }

        print(f"fechas recibidas: {start_date} - {end_date}")
        print(f"fechas formateadas: {from_iso} - {to_iso}")

        async with httpx.AsyncClient() as client:
            while True:
                payload = {
                    "pageNumber": page,
                    "pageSize": limit,
                    "orderBy": "CreationDate",
                    "orderDirection": "DESC",
                    "from": from_iso,
                    "to": to_iso,
                    "displayCurrency": [40, 21, 6, 27, 13, 14, 8, 2, 1, 3],
                    "freeBetSource": [0, 1, 2, 3],
                    "gamificationId": "",
                    "isTotal": False,
                    "customerID": "",
                    "username": "",
                    "merchantCustomerCode": "",
                    "betSlipCode": "",
                    "amountCondition": "",
                    "amountCurrency": "PlayerCurrency",
                    "amountType": -1,
                    "betSlip": -1,
                    "boostedOdds": -1,
                    "comboBonus": -1,
                    "contributionType": -2,
                    "crossBetting": -1,
                    "depositBonus": -1,
                    "featuredSelections": -1,
                    "liveOrPrematch": -1,
                    "quickBet": -1,
                    "semiManaged": -1,
                    "testAccount": -1,
                    "additionalTicketsType": [],
                    "betTypes": [],
                    "bettingView": [],
                    "brands": [],
                    "clientTypes": [],
                    "corporates": [],
                    "countries": [],
                    "currencies": [],
                    "customerLevels": [],
                    "customerTags": [],
                    "eventTypes": [],
                    "events": [],
                    "leagueGroups": [],
                    "leagues": [],
                    "operators": [],
                    "platform": [],
                    "promotionId": [],
                    "sports": []
                }

                print(f"[{self.name}] extrayendo datos pagina {page}...")
                response = await client.post(self.api_url, json=payload, headers=headers, timeout=60.0)
                
                if response.status_code != 200:
                    print(f"[{self.name}] error en api (pagina {page}): {response.status_code}")
                    break

                result = response.json()
                data_obj = result.get("data", {})
                if not data_obj:
                    break

                page_data = data_obj.get("list", [])
                total_records = data_obj.get("total", 0)
                
                all_data.extend(page_data)
                print(f"[{self.name}] progreso: {len(all_data)} / {total_records}")

                if len(all_data) >= total_records or not page_data:
                    break
                
                page += 1
                await asyncio.sleep(0.5)

        return {
            "data": all_data,
            "total": total_records
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

        # guardar json en disco
        timestamp = datetime.now().strftime("%Y%md_%H%M%S")
        filename = f"{self.name.lower()}_reporte_{s_date.replace('-','')}_{e_date.replace('-','')}_{timestamp}.json"
        filepath = os.path.join(self.data_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=4, ensure_ascii=False)

        print(f"[{self.name}] datos guardados en: {filepath}")

        # resumen en consola
        items = report_data["data"]
        print(f"\n[{self.name}] resumen de los primeros 5 registros:")
        for i, item in enumerate(items[:5]):
            pid = item.get("purchaseID", "n/a")
            date = item.get("creationDate", "n/a")
            amt = item.get("stakeDecimal", "0")
            cur = item.get("currencyCode", "")
            usr = item.get("customer", {}).get("loginName", "n/a")
            print(f"{i+1}. ID: {pid} | Fecha: {date} | Monto: {amt} {cur} | Usuario: {usr}")

        return [{
            "source": self.name,
            "status": "success",
            "file": filepath,
            "count": len(items),
            "total": report_data["total"]
        }]

if __name__ == "__main__":
    async def test():
        scraper = FIRSTScraper()
        result = await scraper.scrape()
        print(json.dumps(result, indent=2))
    
    asyncio.run(test())
