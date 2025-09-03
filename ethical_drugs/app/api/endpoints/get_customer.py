from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from sqlalchemy.sql import text
from ..models.base import get_db

router = APIRouter()

class SupervisorRequest(BaseModel):
    supervisorId: int  

@router.post("/get_customer")
def get_customers_under_supervisor(request: SupervisorRequest, db: Session = Depends(get_db)):
    """ Get all customers under a supervisor """
    try:
        if not request.supervisorId:
            return [{
                "Status": 400,
                "Message": "You didn't provide supervisorId",
                "Data": []
            }]

        query = text("""
            SELECT DISTINCT cbp.c_bpartner_id AS customer_id,
                   cbp.value AS customer_code,
                   cbp.name AS customer_name,
                   cbl.phone AS phone,
                   cl.city AS city,
                   cl.address1 AS address1,
                   cc.name AS country_name
            FROM t_customerassignment cs
            JOIN c_bpartner cbp ON cbp.c_bpartner_id = cs.c_bpartner_id
            LEFT JOIN c_bpartner_location cbl ON cbp.c_bpartner_id = cbl.c_bpartner_id
            LEFT JOIN c_location cl ON cbl.c_location_id = cl.c_location_id
            LEFT JOIN c_country cc ON cl.c_country_id = cc.c_country_id
            WHERE (cs.datefinish IS NULL OR cs.datefinish >= NOW() AT TIME ZONE 'Asia/Dhaka')
              AND cs.datestart <= NOW() AT TIME ZONE 'Asia/Dhaka'
              AND cs.datestart = (
                  SELECT MAX(datestart)
                  FROM t_customerassignment
                  WHERE c_bpartner_id = cs.c_bpartner_id
                  AND datestart <= NOW() AT TIME ZONE 'Asia/Dhaka'
              )
              AND cs.territory_id IN (
                  SELECT DISTINCT rg.c_salesregion_id
                  FROM c_salesregion rg
                  LEFT JOIN t_salesregionmapping srgm
                      ON srgm.c_salesregion_id = rg.c_salesregion_id
                     AND srgm.datestart = (
                         SELECT MAX(datestart)
                         FROM t_salesregionmapping
                         WHERE c_salesregion_id = rg.c_salesregion_id
                           AND datestart <= NOW() AT TIME ZONE 'Asia/Dhaka'
                     )
                  LEFT JOIN c_salesregion prg
                      ON prg.c_salesregion_id = srgm.c_salesregion_parent_id
                  LEFT JOIN t_salesregionmapping srgm1
                      ON srgm1.c_salesregion_id = prg.c_salesregion_id
                     AND srgm1.datestart = (
                         SELECT MAX(datestart)
                         FROM t_salesregionmapping
                         WHERE c_salesregion_id = prg.c_salesregion_id
                           AND datestart <= NOW() AT TIME ZONE 'Asia/Dhaka'
                     )
                  WHERE rg.levelno = 6
                    AND rg.ad_org_id = rg.ad_org_id
                    AND srgm.c_salesregion_parent_id = :supervisorId
              )
        """)

        result = db.execute(query, {"supervisorId": request.supervisorId}).fetchall()

        if not result:
            return [{
                "Status": False,
                "Message": "No customers found under this supervisor",
                "Data": []
            }]

        customers = [
            {
                "customerId": row.customer_id,
                "customerCode": row.customer_code,
                "customerName": row.customer_name,
                "phone": row.phone,
                "city": row.city,
                "address1": row.address1,
                "countryName": row.country_name
            }
            for row in result
        ]

        return [{
            "Status": True,
            "Message": "Successful",
            "Data": customers
        }]

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
            }
        ) 