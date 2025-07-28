from fastapi import FastAPI
from routes.api import router
from config.settings import settings
from services.snowflake import SnowflakeService
import logging

# Configure logging
logging.basicConfig(level=getattr(logging, settings.log_level))
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Snowflake SQL API Backend",
    description="FastAPI backend for executing Snowflake queries via SQL API",
    version="1.0.0"
)

# Include API routes
app.include_router(router, prefix="/api/v1")

@app.on_event("startup")
async def startup_event():
    """Test Snowflake connection and authentication on startup"""
    try:
        logger.info("Testing Snowflake connection and authentication...")
        snowflake_service = SnowflakeService()
        
        # Test authentication
        auth_success = await snowflake_service.authenticate()
        if not auth_success:
            raise Exception("Snowflake authentication failed")
        
        # Test a simple query to verify connection
        test_query = "SELECT 1 as test_column"
        result = await snowflake_service.execute_sql(test_query)
        
        if result.get("success") and result.get("data"):
            logger.info("✅ Snowflake connection and authentication successful")
            logger.info(f"Test query executed successfully. Query ID: {result.get('query_id')}")
        else:
            raise Exception("Test query failed")
            
    except Exception as e:
        logger.error(f"❌ Snowflake connection test failed: {str(e)}")
        # You can choose to raise the exception to prevent app startup
        # raise e
        # Or just log the error and continue (current behavior)

@app.get("/")
async def root():
    return {
        "message": "Snowflake SQL API Backend",
        "auth_method": settings.auth_method,
        "docs": "/docs"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug
    )