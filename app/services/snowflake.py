import asyncio
import httpx
import json
import os
import time
from typing import Dict, List, Any, Optional

from config.settings import settings
from auth.keypair_auth import SnowflakeKeyPair
from config.logging_config import logger


class SnowflakeService:
    def __init__(self, client: Optional[httpx.AsyncClient] = None):
        self.account = settings.snowflake_account
        self.warehouse = settings.snowflake_warehouse
        self.database = settings.snowflake_database
        self.schema = settings.snowflake_schema
        self.role = settings.snowflake_role

        self.base_url = f"https://{self.account}.snowflakecomputing.com"
        self.auth_client = None
        self.client = client or httpx.AsyncClient(timeout=120.0)

        # Initialize authentication client based on method
        logger.debug(
            f"Initializing SnowflakeService using auth method: {settings.auth_method}"
        )
        self.auth_client = SnowflakeKeyPair(
            account=self.account,
            username=settings.keypair_username,
            private_key_path=settings.private_key_path,
            passphrase=settings.private_key_passphrase,
        )

    async def authenticate(self) -> bool:
        return self.auth_client.test_key_decryption()

    async def close(self) -> None:
        await self.client.aclose()

    async def _validate_sql(
        self, sql_query: str, parameters: Dict = None
    ) -> None:
        """Validate the SQL statement using Snowflake's EXPLAIN.

        This checks that the statement is syntactically correct and can be
        processed by Snowflake with the supplied bind parameters.  The query is
        not executed; only its plan is retrieved.
        """

        sql_api_url = f"{self.base_url}/api/v2/statements"

        explain_query = f"EXPLAIN {sql_query}"
        request_data = {
            "statement": explain_query,
            "timeout": 60,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
            "role": self.role,
        }

        if parameters:
            request_data["bindings"] = self._format_bindings(parameters)

        headers = self.auth_client.get_auth_headers()

        try:
            response = await self.client.post(
                sql_api_url, json=request_data, headers=headers
            )
            response.raise_for_status()
            result = response.json()
            if result.get("code") != "090001":
                raise Exception(result.get("message", "SQL validation failed"))
        except httpx.HTTPStatusError as e:
            error_detail = e.response.text if e.response else str(e)
            raise Exception(
                f"Snowflake API error during validation: {e.response.status_code} - {error_detail}"
            )
        except Exception as e:
            raise Exception(f"SQL validation error: {e}")

    async def execute_sql(
        self,
        sql_query: str,
        parameters: Dict = None,
        poll_interval: float = 1.0,
        timeout: float = 60.0,
    ) -> Dict[str, Any]:
        """Execute SQL query using Snowflake SQL API with polling and pagination.

        If the query does not complete within the Snowflake API timeout window
        the statement handle is used to poll for completion.  The result set is
        automatically paginated and a cancellation request is issued if the
        total execution time exceeds ``timeout`` seconds.
        """

        sql_api_url = f"{self.base_url}/api/v2/statements"

        logger.debug(f"Executing SQL: {sql_query}")
        if parameters:
            logger.debug(f"With parameters: {parameters}")

        # Validate the SQL before execution
        await self._validate_sql(sql_query, parameters)

        # Prepare request payload.  The ``timeout`` parameter here tells
        # Snowflake how long to wait before returning a statement handle
        # for asynchronous polling.
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

        try:
            response = await self.client.post(
                sql_api_url,
                json=request_data,
                headers=headers,
            )
            response.raise_for_status()

            result = response.json()
            handle = result.get("statementHandle")

            # If the query is still running, poll for completion
            if response.status_code == 202 or result.get("code") == "333333":
                result = await self._poll_for_result(handle, headers, poll_interval, timeout)

            # Fetch additional pages if present
            result = await self._fetch_all_pages(result, handle, headers)
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

    async def _poll_for_result(
        self,
        handle: str,
        headers: Dict[str, str],
        poll_interval: float,
        timeout: float,
    ) -> Dict[str, Any]:
        """Poll Snowflake for completion of an asynchronous statement."""
        status_url = f"{self.base_url}/api/v2/statements/{handle}"
        start_time = time.monotonic()

        while True:
            if time.monotonic() - start_time > timeout:
                await self._cancel_query(handle, headers)
                raise TimeoutError("Snowflake query timed out and was cancelled")

            poll_resp = await self.client.get(status_url, headers=headers)
            poll_resp.raise_for_status()
            result = poll_resp.json()

            status = result.get("status")
            if isinstance(status, dict):
                status = status.get("status")

            if status in ("SUCCESS", "SUCCEEDED"):
                return result
            if status in ("FAILED", "ABORTED", "FAILED_WITH_ERROR"):
                raise Exception(f"Query failed: {result.get('message')}")

            await asyncio.sleep(poll_interval)

    async def _fetch_all_pages(
        self,
        result: Dict[str, Any],
        handle: Optional[str],
        headers: Dict[str, str],
    ) -> Dict[str, Any]:
        """Retrieve all pages of a result set using nextPageToken."""
        rows = result.get("data", [])
        meta = result.get("resultSetMetaData", {})
        next_token = result.get("nextPageToken")

        while handle and next_token:
            page_url = f"{self.base_url}/api/v2/statements/{handle}?page={next_token}"
            page_resp = await self.client.get(page_url, headers=headers)
            page_resp.raise_for_status()
            page = page_resp.json()
            rows.extend(page.get("data", []))
            next_token = page.get("nextPageToken")

        result["data"] = rows
        result["resultSetMetaData"] = meta
        return result

    async def _cancel_query(self, handle: str, headers: Dict[str, str]) -> None:
        """Attempt to cancel a running statement."""
        cancel_url = f"{self.base_url}/api/v2/statements/{handle}/cancel"
        try:
            await self.client.post(cancel_url, headers=headers)
        except Exception as e:
            logger.error(f"Failed to cancel statement {handle}: {e}")
    
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
