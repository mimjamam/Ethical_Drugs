from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pydantic import BaseModel
from typing import List
from ..models.base import get_db

router = APIRouter()

class LocationResponse(BaseModel):
    """Response model for location data"""
    C_ElementValue_ID: int
    loc: str

@router.get("/locations")
def get_locations(ad_client_id: int, db: Session = Depends(get_db)):
    """
    Get list of active locations based on warehouse assignments.
    
    Parameters:
    - ad_client_id: The AD_Client_ID to filter locations
    
    Returns:
    - JSON with Status, Message, and Data keys containing location info
    """
    try:
        query = text("""
            SELECT 
                ce.C_ElementValue_ID, 
                ce.name AS loc
            FROM 
                C_ElementValue ce
            WHERE 
                ce.isActive = 'Y'
                AND ce.AD_Client_ID = :ad_client_id
                AND EXISTS (
                    SELECT 1
                    FROM T_WH_SRAssignment cst
                    JOIN m_warehouse mw 
                        ON mw.m_warehouse_id = cst.m_warehouse_id
                    WHERE (cst.datefinish IS NULL OR cst.datefinish >= NOW())
                    AND mw.name ILIKE '%' || ce.name || '%'
                )
            ORDER BY ce.name
        """)
        
        result = db.execute(query, {"ad_client_id": ad_client_id}).fetchall()
        
        if not result:
            return {
                "Status": True,
                "Message": "No locations found",
                "Data": []
            }
        
        locations = [
            {"C_ElementValue_ID": row.c_elementvalue_id, "loc": row.loc} 
            for row in result
        ]
        
        return {
            "Status": True,
            "Message": "Successful",
            "Data": locations
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred while fetching locations: {str(e)}"
            }
        )
