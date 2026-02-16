from pydantic import BaseModel, Field
import os
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Optional

_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
if _ENV_PATH.exists():
    load_dotenv(dotenv_path=_ENV_PATH, override=False)
else:
    load_dotenv()

class Settings(BaseModel):
    telegram_bot_token: str = Field(validation_alias="TELEGRAM_BOT_TOKEN")
    orders_bot_token: Optional[str] = Field(default=None, validation_alias="ORDERS_BOT_TOKEN")
    design_bot_token: Optional[str] = Field(default=None, validation_alias="DESIGN_BOT_TOKEN")
    database_url: str = Field(validation_alias="DATABASE_URL")
    base_url: str = Field(validation_alias="BASE_URL")  # public HTTPS url for webhook
    webhook_secret_path: str = Field(validation_alias="WEBHOOK_SECRET_PATH", default="/telegram/webhook-secret")
    orders_webhook_path: str = Field(default="/telegram/orders-webhook", validation_alias="ORDERS_WEBHOOK_PATH")
    design_webhook_path: str = Field(default="/telegram/design-webhook", validation_alias="DESIGN_WEBHOOK_PATH")
    admins: List[int] = Field(default_factory=list, validation_alias="ADMINS")
    port: int = Field(default=8080, validation_alias="PORT")
    postback_token: str = Field(default="", validation_alias="POSTBACK_TOKEN")
    keitaro_api_key: str = Field(default="", validation_alias="KEITARO_API_KEY")
    keitaro_base_url: str = Field(default="", validation_alias="KEITARO_BASE_URL")
    youtube_cookies_path: Optional[str] = Field(default=None, validation_alias="YTDLP_COOKIES_PATH")
    youtube_cookies_raw: Optional[str] = Field(default=None, validation_alias="YTDLP_COOKIES")
    youtube_cookies_base64: Optional[str] = Field(default=None, validation_alias="YTDLP_COOKIES_B64")
    youtube_identity_token: Optional[str] = Field(default=None, validation_alias="YTDLP_IDENTITY_TOKEN")
    youtube_auth_user: Optional[str] = Field(default=None, validation_alias="YTDLP_AUTH_USER")
    underdog_base_url: str = Field(default="https://dashboard.underdog.click", validation_alias="UNDERDOG_BASE_URL")
    underdog_email: str = Field(default="", validation_alias="UNDERDOG_EMAIL")
    underdog_password: str = Field(default="", validation_alias="UNDERDOG_PASSWORD")
    underdog_token_ttl: int = Field(default=3600, validation_alias="UNDERDOG_TOKEN_TTL")

    @classmethod
    def load(cls) -> "Settings":
        admins = []
        admins_env = os.getenv("ADMINS", "").strip()
        if admins_env:
            for part in admins_env.split(","):
                part = part.strip()
                if part:
                    try:
                        admins.append(int(part))
                    except ValueError:
                        pass
        raw = {
            "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN", ""),
            "ORDERS_BOT_TOKEN": os.getenv("ORDERS_BOT_TOKEN"),
            "DESIGN_BOT_TOKEN": os.getenv("DESIGN_BOT_TOKEN"),
            "DATABASE_URL": os.getenv("DATABASE_URL", ""),
            "BASE_URL": os.getenv("BASE_URL", ""),
            "WEBHOOK_SECRET_PATH": os.getenv("WEBHOOK_SECRET_PATH", "/telegram/webhook"),
            "ORDERS_WEBHOOK_PATH": os.getenv("ORDERS_WEBHOOK_PATH", "/telegram/orders-webhook"),
            "DESIGN_WEBHOOK_PATH": os.getenv("DESIGN_WEBHOOK_PATH", "/telegram/design-webhook"),
            "ADMINS": admins,
            "PORT": int(os.getenv("PORT", "8080")),
            "POSTBACK_TOKEN": os.getenv("POSTBACK_TOKEN", ""),
            "KEITARO_API_KEY": os.getenv("KEITARO_API_KEY", ""),
            "KEITARO_BASE_URL": os.getenv("KEITARO_BASE_URL", ""),
            "YTDLP_COOKIES_PATH": os.getenv("YTDLP_COOKIES_PATH"),
            "YTDLP_COOKIES": os.getenv("YTDLP_COOKIES"),
            "YTDLP_COOKIES_B64": os.getenv("YTDLP_COOKIES_B64"),
            "YTDLP_IDENTITY_TOKEN": os.getenv("YTDLP_IDENTITY_TOKEN"),
            "YTDLP_AUTH_USER": os.getenv("YTDLP_AUTH_USER"),
            "UNDERDOG_BASE_URL": os.getenv("UNDERDOG_BASE_URL", "https://dashboard.underdog.click"),
            "UNDERDOG_EMAIL": os.getenv("UNDERDOG_EMAIL", ""),
            "UNDERDOG_PASSWORD": os.getenv("UNDERDOG_PASSWORD", ""),
            "UNDERDOG_TOKEN_TTL": int(os.getenv("UNDERDOG_TOKEN_TTL", "3600")),
        }
        return cls.model_validate(raw)

settings = Settings.load()
