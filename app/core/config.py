from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    APP_NAME: str = "FastAPI Multi-Scraper"
    DEBUG: bool = True
    API_V1_STR: str = "/api/v1"

    # mvt
    MVT_URL: str = ""
    MVT_USER: str = ""
    MVT_PASS: str = ""

    # vgr
    VGR_URL: str = ""
    VGR_DOMINIO: str = ""
    VGR_USER: str = ""
    VGR_PASS: str = ""

    # gr
    GR_URL: str = ""
    GR_DOMINIO: str = ""
    GR_USER: str = ""
    GR_PASS: str = ""

    # first
    FIRST_URL: str = ""
    FIRST_USER: str = ""
    FIRST_PASS: str = ""

    # lottingo
    LOT_URL: str = ""
    LOT_USER: str = ""
    LOT_PASS: str = ""

    # s3
    AWS_BUCKET_NAME: str = ""
    AWS_REGION: str = ""
    AWS_ROLE_ARN: str = ""
    AWS_ACCESS_KEY: str = ""
    AWS_SECRET_KEY: str = ""

    # graph api
    GRAPH_CLIENT_ID: str = ""
    GRAPH_CLIENT_SECRET: str = ""
    GRAPH_TENANT_ID: str = ""

    # correos
    GRAPH_EMAIL_FROM: str = ""
    GRAPH_EMAIL_TO: str = ""

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=True)

settings = Settings()
