import httpx
import json
import os
from typing import Dict, List, Any, Optional
from config.settings import settings
from auth.keypair_auth import SnowflakeKeyPair
from config.logging_config import logger

class SnowflakeService:
    def __init__(self):
        self.account = settings.snowflake_account
        self.warehouse = settings.snowflake_warehouse
        self.database = settings.snowflake_database
        self.schema = settings.snowflake_schema
        self.role = settings.snowflake_role
        
        self.base_url = f"https://{self.account}.snowflakecomputing.com"
        self.auth_client = None
        
        # Initialize authentication client based on method
        logger.debug(f"Initializing SnowflakeService using auth method: {settings.auth_method}")
        self.auth_client = SnowflakeKeyPair(
            account=self.account,
            username=settings.keypair_username,
            private_key_path=settings.private_key_path,
            passphrase=settings.private_key_passphrase
        )
    
    async def authenticate(self) -> bool:
        return self.auth_client.test_key_decryption()
    
    async def execute_sql(self, sql_query: str, parameters: Dict = None) -> Dict[str, Any]:
        """Execute SQL query using Snowflake SQL API"""
        sql_api_url = f"{self.base_url}/api/v2/statements"

        logger.debug(f"Executing SQL: {sql_query}")
        if parameters:
            logger.debug(f"With parameters: {parameters}")

        # Prepare request payload
        request_data = {
            "statement": sql_query,
            "timeout": 60,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "role": self.role,
        }

        if parameters:
            request_data["bindings"] = self._format_bindings(parameters)

        headers = self.auth_client.get_auth_headers()

        async with httpx.AsyncClient(timeout=120.0) as client:
            try:
                response = await client.post(
                    sql_api_url,
                    json=request_data,
                    headers=headers,
                )
                response.raise_for_status()

                result = response.json()
                return self._process_result(result)

            except httpx.HTTPStatusError as e:
                error_detail = e.response.text if e.response else str(e)
                raise Exception(
                    f"Snowflake API error: {e.response.status_code} - {error_detail}"
                )
            except Exception as e:
                raise Exception(f"Query execution error: {e}")

    def _format_bindings(self, parameters: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """Convert simple parameter dict into Snowflake bindings format."""
        bindings: Dict[str, Dict[str, str]] = {}
        for index, value in enumerate(parameters.values(), start=1):
            bindings[str(index)] = self._determine_binding(value)
        return bindings

    @staticmethod
    def _determine_binding(value: Any) -> Dict[str, str]:
        """Infer Snowflake binding type for a value."""
        binding_type = "TEXT"
        binding_value: Any = value

        if isinstance(value, bool):
            binding_type = "BOOLEAN"
            binding_value = str(value).lower()
        else:
            try:
                int_val = int(value)
                if str(int_val) == str(value):
                    binding_type = "FIXED"
                    binding_value = str(int_val)
                else:
                    raise ValueError
            except (ValueError, TypeError):
                try:
                    float_val = float(value)
                    binding_type = "REAL"
                    binding_value = str(float_val)
                except (ValueError, TypeError):
                    binding_value = str(value)

        return {"type": binding_type, "value": binding_value}
    
    def _process_result(self, result: Dict) -> Dict[str, Any]:
        """Process Snowflake API response"""
        if result.get("code") != "090001":  # Success code
            raise Exception(f"Query failed: {result.get('message')}")

        # Extract result data
        rows = result.get("data", [])
        meta = result.get("resultSetMetaData", {})

        logger.debug(f"Query returned {len(rows)} rows")

        return {
            "success": True,
            "data": rows,
            "columns": [col["name"] for col in meta.get("rowType", [])],
            "row_count": meta.get("numRows", 0),
            "query_id": result.get("statementHandle")
        }
    
    def load_sql_file(self, filename: str) -> str:
        """Load SQL query from file"""
        # Use the absolute path relative to this file's directory
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sql_path = os.path.join(base_dir, "..", "sql", filename)
        sql_path = os.path.normpath(sql_path)
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"SQL file not found: {sql_path}")
        logger.debug(f"Loading SQL file: {sql_path}")
        with open(sql_path, 'r') as file:
            return file.read().strip()
