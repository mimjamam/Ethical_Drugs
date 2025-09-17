from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pydantic import BaseModel
from typing import List, Optional
from ..models.base import get_db
import json
import datetime

router = APIRouter()

# -------------------------------
# Pydantic Models
# -------------------------------
class ProductItem(BaseModel):
    product_id: int
    quantity: float
    price: float

class SalesOrderRequest(BaseModel):
    latitude: float
    longitude: float
    isodapproved: bool
    AD_Client_ID: int
    AD_Org_ID: int
    description: Optional[str]
    dateordered: datetime.date
    delivarydate: datetime.date
    documentno: str
    M_Pricelist_ID: int
    grandtotal: float
    M_Warehouse_ID: int
    salestype: str
    AD_user_ID: int
    C_BPartner_ID: int
    deliverydate: Optional[str]
    preffereddate: Optional[str]
    Paymenttype: str
    C_SalesRegion_ID: int
    Segment_ID: int
    Segment_Name: str
    Function_ID: int
    Function_Name: str
    Location_ID: int
    Location_Name: str
    ProductArray: List[ProductItem]

# -------------------------------
# Endpoint
# -------------------------------
@router.post("/create_sales_order")
def create_sales_order(request: SalesOrderRequest, db: Session = Depends(get_db)):
    """ Create Sales Order with order lines and calculate grand total """
    try:
        if not request.ProductArray:
            return [{
                "Status": 400,
                "Message": "ProductArray cannot be empty",
                "Data": []
            }]

        sql = text("""
        WITH new_order AS (
            INSERT INTO t_salesorder (
                ad_client_id, ad_org_id, created, createdby, updated, updatedby, isactive,
                documentno, erp_docno, dateordered, deliverydate, iscash, m_pricelist_id,
                m_warehouse_id, grandtotal, description, c_bpartner_id, c_salesregion_id,
                segment_id, function_id, location_id, latitude, longitude, isodapproved,
                paymenttype, preffereddate
            )
            VALUES (
                :AD_Client_ID, :AD_Org_ID, NOW(), :AD_user_ID, NOW(), :AD_user_ID, 'Y',
                :documentno, 'ERP-' || extract(epoch from now()), :dateordered, :delivarydate,
                'Y', :M_Pricelist_ID, :M_Warehouse_ID, 0, :description, :C_BPartner_ID,
                :C_SalesRegion_ID, :Segment_ID, :Function_ID, :Location_ID,
                :latitude, :longitude, :isodapproved, :Paymenttype, :preffereddate
            )
            RETURNING t_salesorder_id
        ),
        insert_lines AS (
            INSERT INTO t_salesorderline (
                ad_client_id, ad_org_id, created, createdby, updated, updatedby, isactive,
                t_salesorder_id, m_product_id, orderedqty, unitprice
            )
            SELECT
                :AD_Client_ID, :AD_Org_ID, NOW(), :AD_user_ID, NOW(), :AD_user_ID, 'Y',
                no.t_salesorder_id, p.product_id, p.quantity::varchar, p.price::varchar
            FROM new_order no
            CROSS JOIN jsonb_to_recordset(:ProductArray::jsonb)
                AS p(product_id int, quantity numeric, price numeric)
            RETURNING t_salesorder_id
        )
        UPDATE t_salesorder so
        SET grandtotal = (
            SELECT COALESCE(SUM(ol.orderedqty::numeric * ol.unitprice::numeric), 0)
            FROM t_salesorderline ol
            WHERE ol.t_salesorder_id = so.t_salesorder_id
        )
        WHERE so.t_salesorder_id = (SELECT t_salesorder_id FROM new_order)
        RETURNING *;
        """)

        product_array_json = json.dumps([item.dict() for item in request.ProductArray])
        params = request.dict()
        params["ProductArray"] = product_array_json

        result = db.execute(sql, params).fetchone()
        db.commit()

        if not result:
            return [{
                "Status": False,
                "Message": "Sales order creation failed",
                "Data": []
            }]

        sales_order = dict(result)

        return [{
            "Status": True,
            "Message": "Sales order created successfully",
            "Data": sales_order
        }]

    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}"
            }
        )
