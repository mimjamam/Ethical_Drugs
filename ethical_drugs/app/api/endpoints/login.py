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
    """User login endpoint"""
    try:
        # Check required fields
        if not request.user_id or not request.password:
            return [{
                "Status": 400,
                "Message": "You didn't provide all info",
                "Data": {
                    "cBpartnerId": None,
                    "adUserId": None,
                    "adClientId": None,
                    "adOrgId": None
                }
            }]

        # Execute raw SQL query
        user = db.execute(
            text("""
                SELECT isactive, c_bpartner_id, ad_user_id, ad_client_id, ad_org_id
                FROM ad_user
                WHERE name = :name AND password = :password
            """),
            {"name": request.user_id, "password": request.password}
        ).fetchone()

        # If user not found
        if not user:
            return [{
                "Status": False,
                "Message": "Invalid user_id or password",
                "Data": {
                    "cBpartnerId": None,
                    "adUserId": None,
                    "adClientId": None,
                    "adOrgId": None
                }
            }]

        # If user is inactive
        if user.isactive != "Y":
            return [{
                "Status": False,
                "Message": "User isn't active",
                "Data": {
                    "cBpartnerId": None,
                    "adUserId": None,
                    "adClientId": None,
                    "adOrgId": None
                }
            }]

        # Prepare response for active user
        user_info = {
            "cBpartnerId": user.c_bpartner_id,
            "adUserId": user.ad_user_id,
            "adClientId": user.ad_client_id,
            "adOrgId": user.ad_org_id
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
                    "adOrgId": None
                }
            }
        )
