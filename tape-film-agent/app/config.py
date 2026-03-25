from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    app_name: str = "Tape Film Agent"
    app_version: str = "0.1.0"
    app_env: str = "development"

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
