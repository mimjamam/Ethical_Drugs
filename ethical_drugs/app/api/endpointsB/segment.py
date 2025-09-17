from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from ..models.base import get_db

router = APIRouter()

@router.get("/segments")
def get_categories(db: Session = Depends(get_db)):
    """
    Fetch only 'Human' and 'Veterinary' segments from c_activity table.
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
            return [{
                "Status": False,
                "Message": "No categories found",
                "Data": []
            }]

        categories = [
            {"segment": row.segment, "C_Activity_ID": row.c_activity_id} 
            for row in result
        ]

        return [{
            "Status": True,
            "Message": "Successful",
            "Data": categories
        }]

    except Exception as e:
        raise [HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
            }
        )]
