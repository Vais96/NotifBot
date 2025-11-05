from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
from typing import List, Optional

load_dotenv()

class Settings(BaseModel):
    telegram_bot_token: str = Field(validation_alias="TELEGRAM_BOT_TOKEN")
    database_url: str = Field(validation_alias="DATABASE_URL")
    base_url: str = Field(validation_alias="BASE_URL")  # public HTTPS url for webhook
    webhook_secret_path: str = Field(validation_alias="WEBHOOK_SECRET_PATH", default="/telegram/webhook-secret")
    admins: List[int] = Field(default_factory=list, validation_alias="ADMINS")
    port: int = Field(default=8080, validation_alias="PORT")
    postback_token: str = Field(default="", validation_alias="POSTBACK_TOKEN")
    keitaro_api_key: str = Field(default="", validation_alias="KEITARO_API_KEY")
    keitaro_base_url: str = Field(default="", validation_alias="KEITARO_BASE_URL")
    youtube_cookies_path: Optional[str] = Field(default=None, validation_alias="YTDLP_COOKIES_PATH")
    youtube_cookies_raw: Optional[str] = Field(default=None, validation_alias="YTDLP_COOKIES")
    youtube_cookies_base64: Optional[str] = Field(default=None, validation_alias="YTDLP_COOKIES_B64")

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
            "DATABASE_URL": os.getenv("DATABASE_URL", ""),
            "BASE_URL": os.getenv("BASE_URL", ""),
            "WEBHOOK_SECRET_PATH": os.getenv("WEBHOOK_SECRET_PATH", "/telegram/webhook"),
            "ADMINS": admins,
            "PORT": int(os.getenv("PORT", "8080")),
            "POSTBACK_TOKEN": os.getenv("POSTBACK_TOKEN", ""),
            "KEITARO_API_KEY": os.getenv("KEITARO_API_KEY", ""),
            "KEITARO_BASE_URL": os.getenv("KEITARO_BASE_URL", ""),
            "YTDLP_COOKIES_PATH": os.getenv("YTDLP_COOKIES_PATH"),
            "YTDLP_COOKIES": os.getenv("YTDLP_COOKIES"),
            "YTDLP_COOKIES_B64": os.getenv("YTDLP_COOKIES_B64"),
        }
        return cls.model_validate(raw)

settings = Settings.load()
