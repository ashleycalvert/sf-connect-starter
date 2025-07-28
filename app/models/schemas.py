from pydantic import BaseModel
from typing import Dict, Any, List, Optional

class QueryRequest(BaseModel):
    sql_file: str
    parameters: Optional[Dict[str, Any]] = None

class QueryResponse(BaseModel):
    success: bool
    data: List[List[Any]]
    columns: List[str]
    row_count: int
    query_id: Optional[str] = None
    error: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    auth_method: str
    account: str

class KeyPairInfoResponse(BaseModel):
    username: str
    public_key_fingerprint: str
    public_key_pem: str
    sql_command: str
