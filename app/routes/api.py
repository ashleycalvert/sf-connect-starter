from fastapi import APIRouter, HTTPException, Depends
from services.snowflake import SnowflakeService
from models.schemas import QueryRequest, QueryResponse, HealthResponse, KeyPairInfoResponse
from config.settings import settings
from auth.keypair_auth import SnowflakeKeyPair
from config.logging_config import logger

router = APIRouter()

# Dependency to get Snowflake service
async def get_snowflake_service():
    service = SnowflakeService()
    if not await service.authenticate():
        logger.error("Authentication failed")
        raise HTTPException(status_code=401, detail="Authentication failed")
    return service

@router.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    return HealthResponse(
        status="healthy",
        auth_method=settings.auth_method,
        account=settings.snowflake_account
    )

@router.get("/keypair-info", response_model=KeyPairInfoResponse)
async def get_keypair_info():
    """Get public key information for Snowflake configuration (key-pair auth only)"""
    if settings.auth_method != "keypair":
        raise HTTPException(status_code=400, detail="This endpoint is only available for key-pair authentication")
    
    try:
        logger.debug("Generating key pair information")
        keypair_client = SnowflakeKeyPair(
            account=settings.snowflake_account,
            username=settings.keypair_username,
            private_key_path=settings.private_key_path,
            passphrase=settings.private_key_passphrase
        )
        
        # Test key decryption
        if not keypair_client.test_key_decryption():
            raise HTTPException(status_code=500, detail="Failed to decrypt private key")
        
        public_key_pem = keypair_client.get_public_key_pem()
        result = KeyPairInfoResponse(
            username=settings.keypair_username,
            public_key_fingerprint=keypair_client.public_key_fp,
            public_key_pem=public_key_pem,
            sql_command=f"ALTER USER {settings.keypair_username} SET RSA_PUBLIC_KEY='{public_key_pem}';"
        )
        logger.debug("Key pair information generated successfully")
        return result
        
    except Exception as e:
        logger.error(f"Error processing key pair: {e}")
        raise HTTPException(status_code=500, detail=f"Error processing key pair: {str(e)}")

@router.post("/test-auth")
async def test_authentication():
    """Test authentication without executing any queries"""
    try:
        service = SnowflakeService()
        auth_success = await service.authenticate()
        
        if auth_success:
            return {
                "success": True,
                "message": "Authentication successful",
                "auth_method": settings.auth_method,
                "account": settings.snowflake_account
            }
        else:
            logger.error("Authentication failed")
            raise HTTPException(status_code=401, detail="Authentication failed")
            
    except Exception as e:
        logger.error(f"Authentication error: {e}")
        raise HTTPException(status_code=500, detail=f"Authentication error: {str(e)}")

@router.post("/query", response_model=QueryResponse)
async def execute_query(
    request: QueryRequest,
    snowflake_service: SnowflakeService = Depends(get_snowflake_service)
):
    """Execute SQL query from file"""
    try:
        # Load SQL from file
        logger.debug(f"Executing query from file: {request.sql_file}")
        sql_query = snowflake_service.load_sql_file(request.sql_file)
        
        # Execute query
        result = await snowflake_service.execute_sql(sql_query, request.parameters)
        
        return QueryResponse(**result)
        
    except FileNotFoundError as e:
        logger.error(str(e))
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/query/{sql_filename}")
async def execute_query_get(
    sql_filename: str,
    snowflake_service: SnowflakeService = Depends(get_snowflake_service)
):
    """Execute SQL query from file via GET request"""
    try:
        # Add .sql extension if not present
        if not sql_filename.endswith('.sql'):
            sql_filename += '.sql'
        logger.debug(f"Executing GET query from file: {sql_filename}")
        sql_query = snowflake_service.load_sql_file(sql_filename)
        result = await snowflake_service.execute_sql(sql_query)
        
        return QueryResponse(**result)
        
    except FileNotFoundError as e:
        logger.error(str(e))
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Query execution failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
