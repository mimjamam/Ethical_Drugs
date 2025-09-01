from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from sqlalchemy.sql import text
from ..models.base import get_db

router = APIRouter()

class User_login(BaseModel):
    user_id: str
    password: str


@router.post("/login")
def login(request: User_login, db: Session = Depends(get_db)):
    """ User login generation """
    try:
        # Check if all required fields are provided
        if not request.user_id or not request.password:
            return [
                {
                    "Status": 400,
                    "Message": "You didn't provide all info",
                    "Data": {"cPartnerId": None}
                }
            ]

        # Fetch user info by user_id and password
        user = db.execute(text("""
            SELECT isactive, c_bpartner_id
            FROM ad_user
            WHERE name = :name AND password = :password
        """), {"name": request.user_id, "password": request.password}).fetchone()

        if not user:
            return [
                {
                    "Status": False,
                    "Message": "Invalid user_id or password",
                    "Data": {"cPartnerId": None}
                }
            ]
        
        if user.isactive != "Y":
            return [
                {
                    "Status": False,
                    "Message": "User isn't active",
                    "Data": {"cPartnerId": None}
                }
            ]

        return [
            {
                "Status": True,
                "Message": "Logged in Successfully",
                "Data": {"cPartnerId": user.c_bpartner_id}
            }
        ]
    except Exception as e:
        raise [HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
                "Data": {"cPartnerId": None}
            }
        )]

