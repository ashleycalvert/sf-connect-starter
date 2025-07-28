from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional

class QueryRequest(BaseModel):
    query_name: str = Field(..., description="Name of the SQL query to execute")
    parameters: Optional[Dict[str, Any]] = Field(default=None, description="Query parameters")

class QueryResponse(BaseModel):
    success: bool
    query_name: str
    columns: Optional[List[str]] = None
    data: Optional[List[List[Any]]] = None
    row_count: Optional[int] = None
    affected_rows: Optional[int] = None
    execution_time: float
    message: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    auth_method: str
    available_queries: List[str]

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None

class KeyPairInfoResponse(BaseModel):
    username: str
    public_key_fingerprint: str
    public_key_pem: str
    sql_command: str
