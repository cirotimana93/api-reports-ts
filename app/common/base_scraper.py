from abc import ABC, abstractmethod
from typing import Any, List
import httpx
from bs4 import BeautifulSoup

class BaseScraper(ABC):
    def __init__(self, name: str, base_url: str):
        self.name = name
        self.base_url = base_url

    async def fetch_page(self, url: str) -> str:
        """obtiene el contenido de una pagina"""
        async with httpx.AsyncClient() as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.text

    def parse_html(self, html: str) -> BeautifulSoup:
        """parsea el contenido html usando beautifulsoup"""
        return BeautifulSoup(html, "html.parser")

    @abstractmethod
    async def scrape(self) -> List[Any]:
        """metodo principal que debe ser implementado por cada scraper"""
        pass
