from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pydantic import BaseModel
from typing import List, Optional
from ..models.base import get_db

router = APIRouter()


class SalesOrderLineRequest(BaseModel):
    orderedqty: float
    m_product_id: int
    unitprice: float
    pdiscount: Optional[float] = 0
    pbonus: Optional[float] = 0
    rangepercent: Optional[float] = 0
    rangediscount: Optional[float] = 0
    description: Optional[str] = ""


class SalesOrderRequest(BaseModel):
    createdby: int
    updatedby: int
    description: str
    dateordered: str
    documentno: str
    grandtotal: float
    m_warehouse_id: int
    ad_user_id: int
    customerid: int
    paymenttype: str
    segment: str
    selectedfunction: str
    selectedlocation: str
    deliverydate: str
    lat_val: str
    long_val: str
    lines: List[SalesOrderLineRequest]


@router.post("/create_salesorder")
def create_salesorder(request: SalesOrderRequest, db: Session = Depends(get_db)):
    try:
        # Turn off auto-commit to control transaction
        db.autocommit = False

        # --- Insert Sales Order Header ---
        try:
            sql_order = text("""
                INSERT INTO adempiere.t_salesorder (
                    ad_client_id, ad_org_id, created, createdby, description, isactive,
                    updated, updatedby, billinglocation, dateordered, documentno,
                    erp_docno, grandtotal, isapproved, m_warehouse_id, salestype,
                    ad_user_id, approverid, customerid, forwardprocessed, msg,
                    forwardto_id, paymenttype, segment, selectedfunction, selectedlocation,
                    m_pricelist_id, segment_id, function_id, location_id, c_salesregion_id,
                    deliverydate, lat_val, long_val, c_order_id
                ) VALUES (
                    1000000, 1000000, NOW(), :createdby, :description, 'Y',
                    NOW(), :updatedby, '', :dateordered, :documentno, '',
                    :grandtotal, 'N', :m_warehouse_id, :paymenttype, :ad_user_id, 0, :customerid, 'N', '',
                    0, :paymenttype, :segment, :selectedfunction, :selectedlocation,
                    1000002, 1000001, 1000427, 1000001, 1000001,
                    :deliverydate, :lat_val, :long_val, 0
                )
                RETURNING t_salesorder_id
            """)
            result = db.execute(sql_order, request.dict(exclude={"lines"}))
            salesorder_id = result.scalar()
            if not salesorder_id:
                raise HTTPException(status_code=500, detail="Failed to create Sales Order header")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Sales Order Header Insert Error: {str(e)}")

        # --- Insert Sales Order Lines ---
        try:
            sql_line = text("""
                INSERT INTO adempiere.t_salesorderline (
                    ad_client_id, ad_org_id, created, createdby, description, isactive,
                    updated, updatedby, orderedqty, m_product_id, t_salesorder_id,
                    documentno, unitprice, document_no, pdiscount, pbonus,
                    rangepercent, rangediscount
                ) VALUES (
                    1000000, 1000000, NOW(), :createdby, :description, 'Y',
                    NOW(), :updatedby, :orderedqty, :m_product_id, :t_salesorder_id,
                    :documentno, :unitprice, :documentno, :pdiscount, :pbonus,
                    :rangepercent, :rangediscount
                )
            """)
            for line in request.lines:
                line_data = {
                    **line.dict(),
                    "createdby": request.createdby,
                    "updatedby": request.updatedby,
                    "t_salesorder_id": salesorder_id,
                    "documentno": request.documentno
                }
                db.execute(sql_line, line_data)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Sales Order Line Insert Error: {str(e)}")

        # --- Commit Transaction ---
        try:
            db.commit()
        except Exception as e:
            db.rollback()
            raise HTTPException(status_code=500, detail=f"Transaction Commit Error: {str(e)}")

        return {"Status": 200, "Message": "Sales Order created successfully", "SalesOrderID": salesorder_id}

    except HTTPException as he:
        raise he
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Unexpected Error: {str(e)}")
