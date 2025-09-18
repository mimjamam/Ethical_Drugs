from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pydantic import BaseModel
from typing import Optional
from ..models.base import get_db

router = APIRouter()

class DiscountRequest(BaseModel):
    customerId: Optional[int] = None
    group_id: Optional[int] = None
    iscash: str 

@router.post("/discounts")
def get_discounts(request: DiscountRequest, db: Session = Depends(get_db)):
    """ Get discounts for a customer and/or group """
    try:
        if not request.customerId and not request.group_id:
            return {
                "Status": 400,
                "Message": "You must provide at least customerId or group_id",
                "Data": []
            }

        query = text("""
        SELECT ds.discount_category, da.m_discountschema_id,
               CASE WHEN ds.discount_category = 'PB' THEN 'Campaign on Product Order Quantity'
                    ELSE 'Campaign on Product Order Amount'
               END AS discountschema_type,
               CASE WHEN ds.discount_category = 'CD' THEN REPLACE(ds.name, ',', '')
                    WHEN ds.discount_category = 'FD' THEN 'Flat Discount'
                    ELSE (SELECT mp.name FROM m_product mp WHERE mp.m_product_id = md.m_product_id)
                         || ' ' || md.breakvalue || ':' || (tp.freeqty) || ' Bonus Offer'
               END AS discountschema_name,
               da.validfrom, da.validto, ds.discounttype, ds.isquantitybased, ds.ismixed,
               ds.maxvalue, ds.minvalue, md.m_discountschemabreak_id,
               COALESCE(md.m_product_id,0) AS m_product_id, md.breakvalue, md.breakdiscount,
               tp.freeqty AS freeqty, md.seqno
        FROM c_bp_discounts da
        JOIN m_discountschema ds ON da.m_discountschema_id = ds.m_discountschema_id
        JOIN m_discountschemabreak md ON md.m_discountschema_id = ds.m_discountschema_id
        LEFT JOIN t_promoreward tp ON tp.m_discountschemabreak_id = md.m_discountschemabreak_id
        WHERE (:customerId IS NULL OR da.c_bpartner_id = :customerId)
          AND '2025-09-17' BETWEEN da.ValidFrom AND da.ValidTo
          AND da.ValidFrom IS NOT NULL AND da.ValidTo IS NOT NULL
          AND ds.isactive = 'Y'
          AND ds.cash_discount = :iscash

        UNION ALL

        SELECT ds.discount_category, da.m_discountschema_id,
               CASE WHEN ds.discount_category = 'PB' THEN 'Campaign on Product Order Quantity'
                    ELSE 'Campaign on Product Order Amount'
               END AS discountschema_type,
               CASE WHEN ds.discount_category = 'CD' THEN REPLACE(ds.name, ',', '')
                    WHEN ds.discount_category = 'FD' THEN 'Flat Discount'
                    ELSE (SELECT mp.name FROM m_product mp WHERE mp.m_product_id = md.m_product_id)
                         || ' ' || md.breakvalue || ':' || (tp.freeqty) || ' Bonus Offer'
               END AS discountschema_name,
               da.validfrom, da.validto, ds.discounttype, ds.isquantitybased, ds.ismixed,
               ds.maxvalue, ds.minvalue, md.m_discountschemabreak_id,
               COALESCE(md.m_product_id,0) AS m_product_id, md.breakvalue, md.breakdiscount,
               tp.freeqty AS freeqty, md.seqno
        FROM M_BP_GR_discounts da
        JOIN m_discountschema ds ON da.m_discountschema_id = ds.m_discountschema_id
        JOIN m_discountschemabreak md ON md.m_discountschema_id = ds.m_discountschema_id
        LEFT JOIN t_promoreward tp ON tp.m_discountschemabreak_id = md.m_discountschemabreak_id
        WHERE (:group_id IS NULL OR da.C_BP_GROUP_ID = :group_id)
          AND '2025-09-17' BETWEEN da.ValidFrom AND da.ValidTo
          AND da.ValidFrom IS NOT NULL AND da.ValidTo IS NOT NULL
          AND ds.isactive = 'Y'
          AND ds.cash_discount = :iscash
        ORDER BY discountschema_name, seqno;
        """)

        result = db.execute(query, {
            "customerId": request.customerId,
            "group_id": request.group_id,
            "iscash": request.iscash
        }).fetchall()

        if not result:
            return {
                "Status": False,
                "Message": "No discounts found for this customer/group",
                "Data": []
            }

        discounts = [
            {
                "discountCategory": row.discount_category,
                "mDiscountschemaId": row.m_discountschema_id,
                "discountSchemaType": row.discountschema_type,
                "discountschemaName": row.discountschema_name,
                "validFrom": str(row.validfrom),
                "validTo": str(row.validto),
                "discountType": row.discounttype,
                "isQuantityBased": row.isquantitybased,
                "isMixed": row.ismixed,
                "maxValue": row.maxvalue,
                "minValue": row.minvalue,
                "mDiscountschemabreakId": row.m_discountschemabreak_id,
                "mProductId": row.m_product_id,
                "breakValue": row.breakvalue,
                "breakDiscount": row.breakdiscount,
                "freeQty": row.freeqty,
                "seqNo": row.seqno
            }
            for row in result
        ]

        return {
            "Status": True,
            "Message": "Successful",
            "Data": discounts
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
            }
        )
