from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from ..models.base import get_db

router = APIRouter()

@router.get("/segments")
def get_segments(db: Session = Depends(get_db)):
    """
    Fetch only 'Human' and 'Veterinary' segments from c_activity table
    and return in the same format as depots (C_Activity_ID, loc).
    """
    try:
        query = text("""
            SELECT c_activity_id AS c_activity_id, name AS segment
            FROM c_activity
            WHERE isactive = 'Y' 
              AND name IN ('Human', 'Veterinary')
            ORDER BY name;
        """)

        result = db.execute(query).fetchall()

        if not result:
            return {
                "Status": False,
                "Message": "No segments found",
                "Data": []
            }

        segments = [
            {"C_Activity_ID": row.c_activity_id, "loc": row.segment} 
            for row in result
        ]

        return {
            "Status": True,
            "Message": "Successful",
            "Data": segments
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
            }
        )
