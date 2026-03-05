from pydantic import BaseModel
from typing import List, Any, Optional

class ScraperResult(BaseModel):
    source: str
    data: Any
    status: str = "success"
    message: Optional[str] = None

class UnifiedScraperResponse(BaseModel):
    results: List[ScraperResult]
    total: int
