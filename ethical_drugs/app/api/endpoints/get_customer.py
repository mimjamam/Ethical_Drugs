from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pydantic import BaseModel
from ..models.base import get_db

router = APIRouter()

class SupervisorRequest(BaseModel):
    cbPartnerId: int  

@router.post("/getCustomers")
def get_customers_under_supervisor(request: SupervisorRequest, db: Session = Depends(get_db)):
    """ Get all customers under a supervisor """
    try:
        if not request.cbPartnerId:
            return {
                "Status": 400,
                "Message": "You didn't provide cbPartnerId",
                "Data": []
            }

        query = text(f"""
        SELECT DISTINCT cs.C_BPartner_ID AS customer_id, 
                        REPLACE(bp.name, '_', ' ') AS customer,
                        bp.value AS code,
                        bp.customersubgroup,
                        (SELECT name FROM ad_user au WHERE au.c_bpartner_id = bp.c_bpartner_id LIMIT 1) AS contact_person,
                        CAST(bp.totalopenbalance AS decimal(10,2)) AS due,
                        bp.c_bp_group_id,
                        (SELECT ((COALESCE(cl.address1, '') || ',' || COALESCE(cl.address2, '') || ',' || COALESCE(cl.address3, '') || ',' || COALESCE(cl.address4, '') || ',' || COALESCE(cl.city, ''))) 
                         FROM c_bpartner sbp
                         JOIN c_bpartner_location cbpl 
                             ON cbpl.c_bpartner_id = sbp.c_bpartner_id 
                            AND cbpl.c_bpartner_location_id = (SELECT MAX(c_bpartner_location_id) 
                                                               FROM c_bpartner_location 
                                                               WHERE c_bpartner_id = sbp.c_bpartner_id)
                         JOIN c_location cl ON cl.c_location_id = cbpl.c_location_id
                         WHERE sbp.c_bpartner_id = bp.c_bpartner_id
                           AND COALESCE((COALESCE(cl.address1, '') || ',' || COALESCE(cl.address2, '') || ',' || COALESCE(cl.address3, '') || ',' || COALESCE(cl.address4, '') || ',' || COALESCE(cl.city, '')), '') != '') AS customer_address,
                        (SELECT cbpl.phone
                         FROM c_bpartner sbp
                         JOIN c_bpartner_location cbpl 
                             ON cbpl.c_bpartner_id = sbp.c_bpartner_id 
                            AND cbpl.c_bpartner_location_id = (SELECT MAX(c_bpartner_location_id) 
                                                               FROM c_bpartner_location 
                                                               WHERE c_bpartner_id = sbp.c_bpartner_id)
                         WHERE sbp.c_bpartner_id = bp.c_bpartner_id 
                           AND COALESCE(cbpl.phone, '') != '') AS customer_phone
        FROM c_bpartner bp
        JOIN T_CustomerAssignment cs ON cs.c_bpartner_id = bp.c_bpartner_id
        WHERE (cs.datefinish IS NULL OR cs.datefinish >= NOW())  
          AND cs.datestart <= NOW()
          AND cs.datestart = (SELECT MAX(datestart) 
                              FROM T_CustomerAssignment  
                              WHERE C_Bpartner_ID = cs.C_BPartner_ID 
                                AND datestart <= NOW())
          AND territory_id IN (
              SELECT DISTINCT rg.C_SalesRegion_ID 
              FROM C_SalesRegion rg
              LEFT JOIN T_SupervisorAssignment cs 
                     ON rg.C_SalesRegion_ID = cs.C_SalesRegion_ID
                    AND cs.datestart = (SELECT MAX(datestart) 
                                        FROM T_SupervisorAssignment  
                                        WHERE C_SalesRegion_ID = rg.C_SalesRegion_ID 
                                          AND datestart <= NOW())
                    AND (cs.datefinish IS NULL OR cs.datefinish >= NOW())
              LEFT JOIN C_BPartner bp ON bp.C_BPartner_ID = cs.C_BPartner_ID
              LEFT JOIN T_SalesRegionMapping srgm ON srgm.C_SalesRegion_ID = rg.C_SalesRegion_ID
                                                AND srgm.datestart = (SELECT MAX(datestart) 
                                                                      FROM T_SalesRegionMapping 
                                                                      WHERE C_SalesRegion_ID = rg.C_SalesRegion_ID 
                                                                        AND datestart <= NOW())
              LEFT JOIN C_SalesRegion prg ON prg.C_SalesRegion_ID = srgm.C_SalesRegion_Parent_ID
              LEFT JOIN T_SalesRegionMapping srgm1 ON srgm1.C_SalesRegion_ID = prg.C_SalesRegion_ID
                                                  AND srgm1.datestart = (SELECT MAX(datestart) 
                                                                         FROM T_SalesRegionMapping 
                                                                         WHERE C_SalesRegion_ID = prg.C_SalesRegion_ID 
                                                                           AND datestart <= NOW())
              WHERE rg.levelno = 6 
                AND rg.AD_Org_ID = 1000000
                AND srgm.C_SalesRegion_Parent_ID IN (
                    SELECT cs.C_SalesRegion_ID 
                    FROM T_SupervisorAssignment cs 
                    JOIN c_salesregion sr ON cs.C_SalesRegion_ID = sr.C_SalesRegion_ID
                    WHERE cs.datestart = (SELECT MAX(datestart) 
                                          FROM T_SupervisorAssignment 
                                          WHERE C_SalesRegion_ID = cs.C_SalesRegion_ID 
                                            AND datestart <= NOW() 
                                            AND c_bpartner_id = :cbPartnerId)
                      AND (cs.datefinish IS NULL OR cs.datefinish >= NOW())
                      AND cs.c_bpartner_id = :cbPartnerId
                )
          )
        ORDER BY customer
        """)

        result = db.execute(query, {"cbPartnerId": request.cbPartnerId}).fetchall()

        if not result:
            return {
                "Status": False,
                "Message": "No customers found for this supervisor",
                "Data": []
            }

        customers = [
            {
                "customerId": row.customer_id,
                "customer": row.customer,
                "code": row.code,
                "customerSubGroup": row.customersubgroup,
                "contactPerson": row.contact_person,
                "due": float(row.due),
                "c_bp_group_id": row.c_bp_group_id,
                "customerAddress": row.customer_address,
                "customerPhone": row.customer_phone
            }
            for row in result
        ]

        return {
            "Status": True,
            "Message": "Successful",
            "Data": customers
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
            }
        )