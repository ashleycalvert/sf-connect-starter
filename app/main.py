from contextlib import asynccontextmanager
import httpx
from fastapi import FastAPI
from routes.api import router
from config.settings import settings
from services.snowflake import SnowflakeService
from config.logging_config import logger


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = httpx.AsyncClient(timeout=120.0)
    snowflake_service = SnowflakeService(client=client)
    try:
        logger.info("Testing Snowflake connection and authentication...")
        auth_success = await snowflake_service.authenticate()
        if not auth_success:
            raise Exception("Snowflake authentication failed")

        test_query = "SELECT 1 as test_column"
        result = await snowflake_service.execute_sql(test_query)

        if result.get("success") and result.get("data"):
            logger.info("✅ Snowflake connection and authentication successful")
            logger.info(
                f"Test query executed successfully. Query ID: {result.get('query_id')}"
            )
        else:
            raise Exception("Test query failed")

    except Exception as e:
        logger.error(f"❌ Snowflake connection test failed: {str(e)}")

    app.state.snowflake_service = snowflake_service
    try:
        yield
    finally:
        await snowflake_service.close()


app = FastAPI(
    title="Snowflake SQL API Backend",
    description="FastAPI backend for executing Snowflake queries via SQL API",
    version="1.0.0",
    lifespan=lifespan,
)

# Include API routes
app.include_router(router, prefix="/api/v1")

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
