from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str

    @property
    def async_database_url(self) -> str:
        """Convert database URL to async format for SQLAlchemy."""
        url = self.database_url
        # Handle postgres:// -> postgresql+asyncpg://
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        elif url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url

    theseus_api_key: str
    theseus_base_url: str = "https://mail.hackclub.com/api/v1"

    slack_bot_token: str
    slack_app_token: str  # xapp-* token for Socket Mode
    slack_signing_secret: str  # For HTTP webhook verification
    slack_notification_channel: str
    slack_canvas_id: str
    slack_jenin_user_id: str = ""

    airtable_api_key: str = ""

    # HCB V4 API for disbursements (OAuth2)
    hcb_client_id: str = ""  # OAuth2 UID
    hcb_client_secret: str = ""  # OAuth2 Secret
    hcb_base_url: str = "https://hcb.hackclub.com/api/v4"
    hcb_fulfillment_org_slug: str = "hermes-fulfillment"  # Destination org for billing

    admin_api_key: str

    api_host: str = "0.0.0.0"  # nosec B104 - Intentional for container deployment
    api_port: int = 8000
    debug: bool = False

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
