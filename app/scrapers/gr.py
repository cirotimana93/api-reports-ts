from typing import Any, List, Optional
from playwright.async_api import async_playwright
from app.common.base_scraper import BaseScraper
from app.core.config import settings
import json
import asyncio

class GRScraper(BaseScraper):
    def __init__(self):
        super().__init__(name="GR", base_url=settings.GR_URL)
        self.domain = settings.GR_DOMINIO
        self.username = settings.GR_USER
        self.password = settings.GR_PASS

    async def scrape(self) -> List[Any]:
        """realiza el login y luego extrae los datos"""
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()

            # objeto para guardar el token capturado
            session_data = {"token": None}

            async def handle_response(response):
                if "/session/login" in response.url:
                    try:
                        status = response.status
                        print(f"[{self.name}] respuesta de login detectada: {status}")
                        if status == 200:
                            data = await response.json()
                            if "token" in data:
                                session_data["token"] = data["token"]
                                print(f"[{self.name}] token de acceso capturado desde la red")
                            else:
                                print(f"[{self.name}] respuesta 200 pero no se encontro 'token' en el json: {data.keys()}")
                        else:
                            body = await response.text()
                            print(f"[{self.name}] error en login, status {status}: {body}")
                    except Exception as e:
                        print(f"[{self.name}] error procesando respuesta: {str(e)}")

            page.on("response", handle_response)

            try:
                # 1. ir a la pagina de inicio
                print(f"[{self.name}] navegando a {self.base_url}")
                await page.goto(self.base_url, timeout=60000, wait_until="networkidle")
                
                print(f"[{self.name}] url actual: {page.url}")

                # 2. realizar login
                try:
                    print(f"[{self.name}] esperando formulario de login")
                    await page.wait_for_selector('input[formcontrolname="domain"]', timeout=15000)
                    
                    print(f"[{self.name}] llenando credenciales (usando type para disparar eventos)")
                    await page.click('input[formcontrolname="domain"]')
                    await page.type('input[formcontrolname="domain"]', self.domain, delay=50)
                    
                    await page.click('input[formcontrolname="username"]')
                    await page.type('input[formcontrolname="username"]', self.username, delay=50)
                    
                    await page.click('input[type="password"]')
                    await page.type('input[type="password"]', self.password, delay=50)
                    
                    is_disabled = await page.eval_on_selector('button[type="submit"]', "el => el.disabled")
                    print(f"[{self.name}] estado del boton submit (disabled): {is_disabled}")

                    # hacer clic en ingresar
                    print(f"[{self.name}] haciendo clic en sign in")
                    await page.click('button[type="submit"]')
                    
                    # esperamos un poco a que las peticiones se procesen y angular haga lo suyo
                    await page.wait_for_load_state("networkidle")
                    await asyncio.sleep(2) # espera extra para asegurar captura de token
                    print(f"[{self.name}] url despues del login: {page.url}")
                except Exception as e:
                    print(f"[{self.name}] error durante el login o espera de respuesta: {str(e)}")

                cookies = await context.cookies()
                
                return [{
                    "source": self.name, 
                    "status": "success",
                    "url": page.url,
                    "token": session_data["token"],
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
        scraper = GRScraper()
        result = await scraper.scrape()
        print(json.dumps(result, indent=2))
    
    asyncio.run(test())
