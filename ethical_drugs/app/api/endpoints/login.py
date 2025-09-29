from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from sqlalchemy.sql import text
from ..models.base import get_db

router = APIRouter()

class UserLogin(BaseModel):
    user_id: str
    password: str

@router.post("/login")
def login(request: UserLogin, db: Session = Depends(get_db)):
    """User login endpoint with warehouse and segment"""

    if not request.user_id or not request.password:
        return [{
            "Status": 400,
            "Message": "You didn't provide all info",
            "Data": {
                "cBpartnerId": None,
                "adUserId": None,
                "adClientId": None,
                "adOrgId": None,
                "warehouseId": None,
                "warehouseName": None,
                "segment": None
            }
        }]

    try:
        # Fetch user
        user = db.execute(
            text("""
                SELECT isactive, c_bpartner_id, ad_user_id, ad_client_id, ad_org_id
                FROM ad_user
                WHERE name = :name AND password = :password
            """),
            {"name": request.user_id, "password": request.password}
        ).fetchone()

        if not user:
            return [{
                "Status": False,
                "Message": "Invalid user_id or password",
                "Data": {
                    "cBpartnerId": None,
                    "adUserId": None,
                    "adClientId": None,
                    "adOrgId": None,
                    "warehouseId": None,
                    "warehouseName": None,
                    "segment": None
                }
            }]

        if user.isactive != "Y":
            return [{
                "Status": False,
                "Message": "User isn't active",
                "Data": {
                    "cBpartnerId": None,
                    "adUserId": None,
                    "adClientId": None,
                    "adOrgId": None,
                    "warehouseId": None,
                    "warehouseName": None,
                    "segment": None
                }
            }]

        # Fetch warehouse and segment from profile SQL
        profile = db.execute(
            text("""
                SELECT 
                    w.m_warehouse_id,
                    w.name AS warehouse_name,
                    COALESCE(act.name, 'N/A') AS segment
                FROM c_bpartner cbp
                LEFT JOIN LATERAL (
                    SELECT mw.m_warehouse_id, mw.name
                    FROM t_wh_srassignment cst
                    JOIN m_warehouse mw ON mw.m_warehouse_id = cst.m_warehouse_id
                    WHERE cst.c_salesregion_id = (
                        SELECT COALESCE(tsa.c_salesregion_id, cs.territory_id)
                        FROM t_supervisorassignment tsa
                        LEFT JOIN t_customerassignment cs 
                               ON cs.c_bpartner_id = tsa.c_bpartner_id
                        WHERE tsa.c_bpartner_id = cbp.c_bpartner_id
                        LIMIT 1
                    )
                    AND cst.datestart <= NOW()
                    AND (cst.datefinish IS NULL OR cst.datefinish >= NOW())
                    ORDER BY cst.datestart DESC
                    LIMIT 1
                ) w ON TRUE
                LEFT JOIN c_activity act
                       ON act.c_activity_id = cbp.c_activity_id
                      AND act.isactive = 'Y'
                      AND act.name IN ('Human', 'Veterinary')
                WHERE cbp.c_bpartner_id = :cPartnerId
            """),
            {"cPartnerId": user.c_bpartner_id}
        ).fetchone()

        user_info = {
            "cBpartnerId": user.c_bpartner_id,
            "adUserId": user.ad_user_id,
            "adClientId": user.ad_client_id,
            "adOrgId": user.ad_org_id,
            "warehouseId": profile.m_warehouse_id if profile else None,
            "warehouseName": profile.warehouse_name if profile else None,
            "segment": profile.segment if profile else None
        }

        return [{
            "Status": True,
            "Message": "Logged in Successfully",
            "Data": user_info
        }]

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
                "Data": {
                    "cBpartnerId": None,
                    "adUserId": None,
                    "adClientId": None,
                    "adOrgId": None,
                    "warehouseId": None,
                    "warehouseName": None,
                    "segment": None
                }
            }
        )
