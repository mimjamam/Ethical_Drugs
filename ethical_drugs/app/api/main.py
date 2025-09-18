from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import JSONResponse
import time
import logging
from app.api.endpoints import (
    login, profile_data, get_customer, product_list, discounts, sales_order, segment,locations,orderList
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    description="Ethical Drugs API",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    contact={
        "name": "Md Bikasuzzaman",
        "email": "bikas.zaman@sysnova.com",
        "url": "https://sysnova.com"  
    }
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # <- must be a list
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Trusted Host middleware
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=["*"]  # In production, replace with actual allowed hosts
)

# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    logger.info(f"Request: {request.method} {request.url} - Processed in {process_time:.4f} seconds")
    return response

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal server error occurred"}
    )
# Include routers
# Health check endpoint
@app.get("/health")
async def health_check():
    return {"status": "healthy"}

app.include_router(login.router, prefix="/ethical", tags=["Login"])
app.include_router(profile_data.router, prefix="/ethical", tags=["Profile Data"])
app.include_router(get_customer.router, prefix="/ethical", tags=["Customer Info"])
app.include_router(product_list.router, prefix="/ethical", tags=["Product List"])
app.include_router(discounts.router, prefix="/ethical", tags=["Discounts List"])
app.include_router(sales_order.router, prefix="/ethical", tags=["Sales Order"])
app.include_router(segment.router, prefix="/ethical", tags=["Segment"])
app.include_router(locations.router, prefix="/ethical", tags=["Locations"])
app.include_router(orderList.router, prefix="/ethical", tags=["Order List"])


