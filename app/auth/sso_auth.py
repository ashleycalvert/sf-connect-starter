import httpx
import base64
from typing import Dict, Optional
import json

class SnowflakeSSO:
    def __init__(self, account: str, username: str, password: str):
        self.account = account
        self.username = username
        self.password = password
        self.base_url = f"https://{account}.snowflakecomputing.com"
        self.session_token: Optional[str] = None
        self.master_token: Optional[str] = None
        
    async def authenticate(self) -> bool:
        """Authenticate using SSO credentials"""
        auth_url = f"{self.base_url}/session/authenticator-request"
        
        # Step 1: Initial authentication request
        auth_data = {
            "data": {
                "ACCOUNT_NAME": self.account,
                "LOGIN_NAME": self.username,
                "PASSWORD": self.password,
                "CLIENT_APP_ID": "FastAPI-Snowflake-Client",
                "CLIENT_APP_VERSION": "1.0.0"
            }
        }
        
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(auth_url, json=auth_data)
                response.raise_for_status()
                
                auth_response = response.json()
                
                if auth_response.get("success"):
                    data = auth_response.get("data", {})
                    self.session_token = data.get("token")
                    self.master_token = data.get("masterToken")
                    return True
                else:
                    print(f"Authentication failed: {auth_response.get('message')}")
                    return False
                    
            except Exception as e:
                print(f"SSO Authentication error: {e}")
                return False
    
    def get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers for API requests"""
        if not self.session_token:
            raise Exception("Not authenticated. Call authenticate() first.")
            
        return {
            "Authorization": f"Snowflake Token=\"{self.session_token}\"",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "FastAPI-Snowflake-Client/1.0.0"
        }
