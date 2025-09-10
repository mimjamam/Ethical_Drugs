from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from sqlalchemy.sql import text
from typing import Optional
from ..models.base import get_db
import datetime

router = APIRouter()

class Discounts(BaseModel):
    c_bpartnerID: int
    date: str             # format YYYY-MM-DD
    time: str             # format HH:MM:SS
    inCash: str = "Y"     # "Y" or "N", default is "Y"
    inCredit: str = "Y"   # "Y" or "N", default is "Y"
    c_bp_group_id: Optional[int] = None  # optional group ID

@router.post("/discounts")
def get_product_list(request: Discounts, db: Session = Depends(get_db)):
    """ Get full product list with discount schema info """
    try:
        if not request.c_bpartnerID:
            return [{
                "Status": 400,
                "Message": "You didn't provide c_bpartnerID",
                "Data": []
            }]

        # Combine date and time into full datetime
        order_datetime_str = f"{request.date} {request.time}"
        order_datetime = datetime.datetime.strptime(order_datetime_str, "%Y-%m-%d %H:%M:%S")

        # SQL query using UNION ALL for partner and group discounts
        query = text("""
            SELECT ds.discount_category,
                   da.m_discountschema_id,
                   CASE 
                       WHEN ds.discount_category = 'PB' THEN 'Campaign on Product Order Quantity'
                       ELSE 'Campaign on Product Order Amount'
                   END AS discountschema_type,
                   CASE 
                       WHEN ds.discount_category = 'CD' THEN REPLACE(ds.name, ',', '')
                       WHEN ds.discount_category = 'FD' THEN 'Flat Discount'
                       ELSE (COALESCE(mp.name, '') || ' ' || md.breakvalue || ':' || COALESCE(tp.freeqty, 0) || ' Bonus Offer')
                   END AS discountschema_name,
                   da.validfrom,
                   da.validto,
                   ds.discounttype,
                   ds.isquantitybased,
                   ds.ismixed,
                   ds.maxvalue,
                   ds.minvalue,
                   md.m_discountschemabreak_id,
                   COALESCE(md.m_product_id, 0) AS m_product_id,
                   md.breakvalue,
                   md.breakdiscount,
                   COALESCE(tp.freeqty, 0) AS freeqty,
                   md.seqno
            FROM c_bp_discounts da
            JOIN m_discountschema ds ON da.m_discountschema_id = ds.m_discountschema_id
            JOIN m_discountschemabreak md ON md.m_discountschema_id = ds.m_discountschema_id
            LEFT JOIN t_promoreward tp ON tp.m_discountschemabreak_id = md.m_discountschemabreak_id
            LEFT JOIN m_product mp ON mp.m_product_id = md.m_product_id
            WHERE da.c_bpartner_id = :c_bpartner_id
              AND :order_datetime BETWEEN da.validfrom::date AND da.validto::date
              AND da.validfrom IS NOT NULL
              AND da.validto IS NOT NULL
              AND ds.isactive = 'Y'
              AND (:iscash = 'Y' OR :iscredit = 'Y')

            UNION ALL

            SELECT ds.discount_category,
                   da.m_discountschema_id,
                   CASE 
                       WHEN ds.discount_category = 'PB' THEN 'Campaign on Product Order Quantity'
                       ELSE 'Campaign on Product Order Amount'
                   END AS discountschema_type,
                   CASE 
                       WHEN ds.discount_category = 'CD' THEN REPLACE(ds.name, ',', '')
                       WHEN ds.discount_category = 'FD' THEN 'Flat Discount'
                       ELSE (COALESCE(mp.name, '') || ' ' || md.breakvalue || ':' || COALESCE(tp.freeqty, 0) || ' Bonus Offer')
                   END AS discountschema_name,
                   da.validfrom,
                   da.validto,
                   ds.discounttype,
                   ds.isquantitybased,
                   ds.ismixed,
                   ds.maxvalue,
                   ds.minvalue,
                   md.m_discountschemabreak_id,
                   COALESCE(md.m_product_id, 0) AS m_product_id,
                   md.breakvalue,
                   md.breakdiscount,
                   COALESCE(tp.freeqty, 0) AS freeqty,
                   md.seqno
            FROM m_bp_gr_discounts da
            JOIN m_discountschema ds ON da.m_discountschema_id = ds.m_discountschema_id
            JOIN m_discountschemabreak md ON md.m_discountschema_id = ds.m_discountschema_id
            LEFT JOIN t_promoreward tp ON tp.m_discountschemabreak_id = md.m_discountschemabreak_id
            LEFT JOIN m_product mp ON mp.m_product_id = md.m_product_id
            WHERE (:c_bp_group_id IS NULL OR da.c_bp_group_id = :c_bp_group_id)
              AND :order_datetime BETWEEN da.validfrom::date AND da.validto::date
              AND da.validfrom IS NOT NULL
              AND da.validto IS NOT NULL
              AND ds.isactive = 'Y'
              AND (:iscash = 'Y' OR :iscredit = 'Y')
            ORDER BY discountschema_name, seqno;
        """)

        result = db.execute(query, {
            "c_bpartner_id": request.c_bpartnerID,
            "order_datetime": order_datetime,
            "c_bp_group_id": request.c_bp_group_id,
            "iscash": request.inCash,
            "iscredit": request.inCredit
        }).fetchall()

        if not result:
            return [{
                "Status": False,
                "Message": "No discounts found",
                "Data": []
            }]

        products = [
            {
                "discountCategory": row.discount_category,
                "discountSchemaId": row.m_discountschema_id,
                "discountSchemaType": row.discountschema_type,
                "discountSchemaName": row.discountschema_name,
                "validFrom": row.validfrom.strftime("%Y-%m-%d %H:%M:%S"),
                "validTo": row.validto.strftime("%Y-%m-%d %H:%M:%S"),
                "discountType": row.discounttype,
                "isQuantityBased": row.isquantitybased,
                "isMixed": row.ismixed,
                "maxValue": row.maxvalue,
                "minValue": row.minvalue,
                "discountSchemaBreakId": row.m_discountschemabreak_id,
                "productId": row.m_product_id,
                "breakValue": row.breakvalue,
                "breakDiscount": row.breakdiscount,
                "freeQty": row.freeqty,
                "sequenceNo": row.seqno
            }
            for row in result
        ]

        return [{
            "Status": True,
            "Message": "Successful",
            "Data": products
        }]

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
            }
        )
