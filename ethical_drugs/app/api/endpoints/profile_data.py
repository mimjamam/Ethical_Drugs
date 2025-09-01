from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from sqlalchemy.sql import text
from datetime import datetime
from ..models.base import get_db

router = APIRouter()

class ProfileData(BaseModel):
    cPartnerId: int


@router.post("/profile_data")
def login(request: ProfileData, db: Session = Depends(get_db)):
    """ User Profile Data """
    try:
        if not request.cPartnerId:
            return [
                {
                    "Status": 400,
                    "Message": "You didn't provide all info",
                    "Data": {}
                }
            ]
        
        current_time = db.execute(text("SELECT NOW() AT TIME ZONE 'Asia/Dhaka';")).fetchone()[0]

        user_info = db.execute(text("""
                SELECT 
                    cbp.value AS "cbPartnerValue",
                    cbp.name AS "cbParnerName",
                    hrj.name AS "hr_job_name",
                    cs.name AS "cSalesname",
                    cb.joiningdate AS "joining_date"
                FROM c_bpartner AS cbp
                LEFT JOIN hr_job AS hrj ON cbp.hr_job_id = hrj.hr_job_id
                LEFT JOIN t_supervisorassignment AS tsa ON cbp.c_bpartner_id = tsa.c_bpartner_id
                    AND (tsa.datefinish IS NULL 
                        AND NOW() AT TIME ZONE 'Asia/Dhaka' >= tsa.datestart)
                LEFT JOIN c_salesregion AS cs ON tsa.c_salesregion_id = cs.c_salesregion_id
                LEFT JOIN c_bpartner cb ON tsa.c_bpartner_id = cb.c_bpartner_id 
                WHERE cbp.c_bpartner_id = :cPartnerId
            """), {"cPartnerId": request.cPartnerId}).fetchall()

        if not user_info:
            return [
                {
                    "Status": False,
                    "Message": "Data doesn't found",
                    "Data": {}
                }
            ]
        
        def format_date(date_val):
            if date_val:
                # Convert to datetime if not already, then format
                if isinstance(date_val, str):
                    date_val = datetime.fromisoformat(date_val)
                return date_val.strftime("%d %b,%Y")
            return None

        specialities_info = [
            {
                "cbPartnerValue": row.cbPartnerValue,
                "cbParnerName": row.cbParnerName, 
                "hrJobName": row.hr_job_name,
                "territoryName": row.cSalesname,
                "joining_date": format_date(row.joining_date)
            } for row in user_info
        ]

        return [
            {
                "Status": True,
                "Message": "Successful",
                "Data": specialities_info
            }
        ]
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
            }
        )
