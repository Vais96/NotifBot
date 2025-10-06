from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
from typing import List

load_dotenv()

class Settings(BaseModel):
    telegram_bot_token: str = Field(validation_alias="TELEGRAM_BOT_TOKEN")
    database_url: str = Field(validation_alias="DATABASE_URL")
    base_url: str = Field(validation_alias="BASE_URL")  # public HTTPS url for webhook
    webhook_secret_path: str = Field(validation_alias="WEBHOOK_SECRET_PATH", default="/telegram/webhook-secret")
    admins: List[int] = Field(default_factory=list, validation_alias="ADMINS")
    port: int = Field(default=8080, validation_alias="PORT")
    postback_token: str = Field(default="", validation_alias="POSTBACK_TOKEN")

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
        }
        return cls.model_validate(raw)

settings = Settings.load()
