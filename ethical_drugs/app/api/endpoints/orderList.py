from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from datetime import datetime
from typing import List, Optional
from pydantic import BaseModel, Field
from ..models.base import get_db

router = APIRouter()

class SalesOrderLineResponse(BaseModel):
    tSalesorderlineId: int = Field(..., alias="t_salesorderline_id")
    tSalesorderId: int = Field(..., alias="t_salesorder_id")
    mProductId: int = Field(..., alias="m_product_id")
    orderedQty: float = Field(..., alias="orderedqty")
    unitPrice: float = Field(..., alias="unitprice")
    pDiscount: Optional[float] = Field(None, alias="pdiscount")
    pBonus: Optional[float] = Field(None, alias="pbonus")
    rangePercent: Optional[float] = Field(None, alias="rangepercent")
    rangeDiscount: Optional[float] = Field(None, alias="rangediscount")
    description: Optional[str] = None

class SalesOrderResponse(BaseModel):
    tSalesorderId: int = Field(..., alias="t_salesorder_id")
    documentNo: str = Field(..., alias="documentno")
    dateOrdered: datetime = Field(..., alias="dateordered")
    grandTotal: float = Field(..., alias="grandtotal")
    description: Optional[str] = None
    mWarehouseId: int = Field(..., alias="m_warehouse_id")
    adUserId: int = Field(..., alias="ad_user_id")
    customerId: int = Field(..., alias="customerid")
    paymentType: str = Field(..., alias="paymenttype")
    segment: Optional[str] = None
    selectedFunction: Optional[str] = Field(None, alias="selectedfunction")
    selectedLocation: Optional[str] = Field(None, alias="selectedlocation")
    deliveryDate: Optional[datetime] = Field(None, alias="deliverydate")
    latVal: Optional[str] = Field(None, alias="lat_val")
    longVal: Optional[str] = Field(None, alias="long_val")
    created: datetime
    lines: List[SalesOrderLineResponse] = []

@router.get("/orderList", response_model=List[SalesOrderResponse])
def get_order_list(
    ad_user_id: int = Query(...),
    created_date: Optional[str] = Query(None),
    db: Session = Depends(get_db)
):
    try:
        db.autocommit = False
    except Exception:
        pass

    try:
        try:
            sql_order = """
                SELECT 
                    so.t_salesorder_id, so.documentno, so.dateordered, so.grandtotal,
                    so.description, so.m_warehouse_id, so.ad_user_id, so.customerid,
                    so.paymenttype, so.segment, so.selectedfunction, so.selectedlocation,
                    so.deliverydate, so.lat_val, so.long_val, so.created
                FROM adempiere.t_salesorder so
                WHERE so.ad_user_id::text = :ad_user_id
            """
            params = {"ad_user_id": str(ad_user_id)}

            if created_date:
                try:
                    parsed_date = datetime.strptime(created_date, "%Y-%m-%d").date()
                    sql_order += " AND DATE(so.created) = :created_date"
                    params["created_date"] = parsed_date
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Invalid date format: {str(e)}")

            try:
                result = db.execute(text(sql_order), params)
                sales_orders = result.mappings().all()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error fetching sales orders: {str(e)}")

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Order query error: {str(e)}")

        if not sales_orders:
            return []

        try:
            order_ids = [str(order["t_salesorder_id"]) for order in sales_orders]
            placeholders = ", ".join([":id_" + str(i) for i in range(len(order_ids))])
            sql_line = f"""
                SELECT 
                    sol.t_salesorderline_id, sol.t_salesorder_id, sol.m_product_id,
                    sol.orderedqty::numeric as orderedqty,
                    sol.unitprice::numeric as unitprice,
                    sol.pdiscount::numeric as pdiscount,
                    sol.pbonus::numeric as pbonus,
                    sol.rangepercent::numeric as rangepercent,
                    sol.rangediscount::numeric as rangediscount,
                    sol.description
                FROM adempiere.t_salesorderline sol
                WHERE sol.t_salesorder_id IN ({placeholders})
            """
            line_params = {f"id_{i}": int(order_id) for i, order_id in enumerate(order_ids)}
            try:
                lines_result = db.execute(text(sql_line), line_params)
                lines = lines_result.mappings().all()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error fetching order lines: {str(e)}")

        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Order line query error: {str(e)}")

        try:
            lines_by_order = {}
            for line in lines:
                order_id = line["t_salesorder_id"]
                if order_id not in lines_by_order:
                    lines_by_order[order_id] = []
                lines_by_order[order_id].append(line)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error grouping order lines: {str(e)}")

        try:
            response = []
            for order in sales_orders:
                order_id = order["t_salesorder_id"]
                order_dict = dict(order)
                order_dict["lines"] = lines_by_order.get(order_id, [])
                response.append(order_dict)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error combining orders and lines: {str(e)}")

        return response

    except Exception as e:
        try:
            db.rollback()
        except:
            pass
        raise HTTPException(status_code=500, detail=f"Unexpected server error: {str(e)}")
