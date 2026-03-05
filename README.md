# FastAPI Multi-Web Scraper

A modular FastAPI application for scraping data from multiple sources (MVT, VGR, GR, FIRST).

## Architecture

- `app/api/`: API endpoints and routing.
- `app/common/`: Shared utilities and base classes.
- `app/core/`: Configuration and settings.
- `app/models/`: Pydantic schemas for data validation.
- `app/scrapers/`: Individual scraper implementations.

## Getting Started

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Configure environment:**
    Edit the `.env` file with your target URLs.

3.  **Run the application:**
    ```bash
    python main.py
    ```
    Or using uvicorn:
    ```bash
    uvicorn main:app --reload
    ```

4.  **Access Documentation:**
    Go to `http://localhost:8000/docs` to see the interactive Swagger UI.

## Adding New Scrapers

1.  Create a new file in `app/scrapers/` inheriting from `BaseScraper`.
2.  Implement the `scrape` method.
3.  Add the new scraper to the API endpoints in `app/api/v1/endpoints/scraper.py`.
