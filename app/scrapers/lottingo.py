from typing import Any, List, Optional
from playwright.async_api import async_playwright
from app.common.base_scraper import BaseScraper
from app.core.config import settings
import json
import asyncio

class LottingoScraper(BaseScraper):
    def __init__(self):
        super().__init__(name="LOTTINGO", base_url=settings.LOT_URL)
        self.username = settings.LOT_USER
        self.password = settings.LOT_PASS

    async def scrape(self) -> List[Any]:
        """realiza el login y luego extrae los datos"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            try:
                # 1. ir a la pagina de inicio
                print(f"[{self.name}] navegando a {self.base_url}")
                await page.goto(self.base_url, timeout=60000, wait_until="networkidle")
                
                print(f"[{self.name}] url actual: {page.url}")

                # 2. realizar login
                try:
                    print(f"[{self.name}] esperando formulario de login")
                    await page.wait_for_selector('input[name="fr_username"]', timeout=15000)
                    
                    print(f"[{self.name}] llenando credenciales")
                    await page.fill('input[name="fr_username"]', self.username)
                    await page.fill('input[name="fr_password"]', self.password)
                    
                    # hacer clic en ingresar
                    print(f"[{self.name}] haciendo clic en ingresar")
                    await page.click('button[name="fr_login"]')
                    
                    # esperar navegacion
                    await page.wait_for_load_state("networkidle")
                    print(f"[{self.name}] url despues del login: {page.url}")
                    
                    # verificar si el logeo fue exitoso buscando el formulario de consulta
                    form_present = await page.locator('form:has(input[name="fecha_inicio"])').count()
                    if form_present > 0:
                        print(f"[{self.name}] login exitoso: formulario de consulta detectado")
                    else:
                        print(f"[{self.name}] advertencia: no se detecto el formulario de consulta")

                except Exception as e:
                    print(f"[{self.name}] error durante el login: {str(e)}")

                cookies = await context.cookies()
                
                return [{
                    "source": self.name, 
                    "status": "success",
                    "url": page.url,
                    "cookies_count": len(cookies)
                }]

            except Exception as e:
                print(f"[{self.name}] error durante el scraping: {str(e)}")
                return [{
                    "source": self.name,
                    "status": "error",
                    "message": str(e)
                }]
            finally:
                await browser.close()

if __name__ == "__main__":
    import asyncio
    async def test():
        scraper = LottingoScraper()
        result = await scraper.scrape()
        print(json.dumps(result, indent=2))
    
    asyncio.run(test())
