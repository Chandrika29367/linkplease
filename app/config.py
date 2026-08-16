import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    PSEUDOGRAM_BASE_URL: str = "https://pseudogram-api.onrender.com"
    PSEUDOGRAM_API_KEY: str
    DATABASE_URL: str = "sqlite:///./linkplease.db"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

# Instantiate settings
settings = Settings()
