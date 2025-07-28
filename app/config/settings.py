import os
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Snowflake Configuration
    snowflake_account: str
    snowflake_warehouse: str
    snowflake_database: str
    snowflake_schema: str
    snowflake_role: Optional[str] = None
    
    # Authentication
    auth_method: str = "sso"  # "sso" or "keypair"
    
    # SSO Configuration
    sso_username: Optional[str] = None
    sso_password: Optional[str] = None
    
    # Key Pair Configuration
    keypair_username: Optional[str] = None
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None
    
    # API Configuration
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    debug: bool = False
    log_level: str = "INFO"
    
    class Config:
        env_file = ".env"

settings = Settings()