from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pydantic import BaseModel
from ..models.base import get_db

router = APIRouter()

class Discounts(BaseModel):
    c_bpartnerID: int   # only input field

@router.post("/discounts")
def get_discounts(request: Discounts, db: Session = Depends(get_db)):
    """ Get discount schema info for a business partner """
    try:
        if not request.c_bpartnerID:
            return [{
                "Status": 400,
                "Message": "You didn't provide c_bpartnerID",
                "Data": []
            }]

        # SQL query: partner + group discounts
        query = text("""
            SELECT 
                CASE 
                    WHEN ds.discount_category = 'CD' THEN REPLACE(ds.name, ',', '')
                    WHEN ds.discount_category = 'FD' THEN 'Flat Discount'
                    ELSE (COALESCE(mp.name, '') || ' ' || md.breakvalue || ':' || COALESCE(tp.freeqty, 0) || ' Bonus Offer')
                END AS discountschema_name,
                ds.maxvalue,
                COALESCE(md.m_product_id, 0) AS m_product_id,
                ds.discount_category,
                ds.isquantitybased,
                md.seqno,
                da.m_discountschema_id,
                COALESCE(tp.freeqty, 0) AS freeqty,
                ds.discounttype,
                ds.ismixed,
                md.m_discountschemabreak_id,
                ds.minvalue,
                NULL::numeric AS volume,
                NULL::numeric AS qtyplan,
                md.breakdiscount,
                da.validfrom,
                CASE 
                    WHEN ds.discount_category = 'PB' THEN 'Campaign on Product Order Quantity'
                    ELSE 'Campaign on Product Order Amount'
                END AS discountschema_type,
                FALSE AS isweight,
                md.breakvalue,
                da.validto
            FROM c_bp_discounts da
            JOIN m_discountschema ds ON da.m_discountschema_id = ds.m_discountschema_id
            JOIN m_discountschemabreak md ON md.m_discountschema_id = ds.m_discountschema_id
            LEFT JOIN t_promoreward tp ON tp.m_discountschemabreak_id = md.m_discountschemabreak_id
            LEFT JOIN m_product mp ON mp.m_product_id = md.m_product_id
            WHERE da.c_bpartner_id = :c_bpartner_id
              AND ds.isactive = 'Y'

            UNION ALL

            SELECT 
                CASE 
                    WHEN ds.discount_category = 'CD' THEN REPLACE(ds.name, ',', '')
                    WHEN ds.discount_category = 'FD' THEN 'Flat Discount'
                    ELSE (COALESCE(mp.name, '') || ' ' || md.breakvalue || ':' || COALESCE(tp.freeqty, 0) || ' Bonus Offer')
                END AS discountschema_name,
                ds.maxvalue,
                COALESCE(md.m_product_id, 0) AS m_product_id,
                ds.discount_category,
                ds.isquantitybased,
                md.seqno,
                da.m_discountschema_id,
                COALESCE(tp.freeqty, 0) AS freeqty,
                ds.discounttype,
                ds.ismixed,
                md.m_discountschemabreak_id,
                ds.minvalue,
                NULL::numeric AS volume,
                NULL::numeric AS qtyplan,
                md.breakdiscount,
                da.validfrom,
                CASE 
                    WHEN ds.discount_category = 'PB' THEN 'Campaign on Product Order Quantity'
                    ELSE 'Campaign on Product Order Amount'
                END AS discountschema_type,
                FALSE AS isweight,
                md.breakvalue,
                da.validto
            FROM m_bp_gr_discounts da
            JOIN m_discountschema ds ON da.m_discountschema_id = ds.m_discountschema_id
            JOIN m_discountschemabreak md ON md.m_discountschema_id = ds.m_discountschema_id
            LEFT JOIN t_promoreward tp ON tp.m_discountschemabreak_id = md.m_discountschemabreak_id
            LEFT JOIN m_product mp ON mp.m_product_id = md.m_product_id
            WHERE EXISTS (
                SELECT 1 
                FROM c_bpartner bp 
                WHERE bp.c_bpartner_id = :c_bpartner_id 
                  AND bp.c_bp_group_id = da.c_bp_group_id
            )
              AND ds.isactive = 'Y'
            ORDER BY discountschema_name, seqno;
        """)

        result = db.execute(query, {"c_bpartner_id": request.c_bpartnerID}).fetchall()

        if not result:
            return [{
                "Status": False,
                "Message": "No discounts found",
                "Data": []
            }]

        discounts = [
            {
                "discountschema_name": row.discountschema_name,
                "maxvalue": row.maxvalue,
                "m_product_id": row.m_product_id,
                "discount_category": row.discount_category,
                "isquantitybased": row.isquantitybased,
                "seqno": row.seqno,
                "m_discountschema_id": row.m_discountschema_id,
                "freeqty": row.freeqty,
                "discounttype": row.discounttype,
                "ismixed": row.ismixed,
                "m_discountschemabreak_id": row.m_discountschemabreak_id,
                "minvalue": row.minvalue,
                "volume": row.volume,
                "qtyplan": row.qtyplan,
                "breakdiscount": row.breakdiscount,
                "validfrom": row.validfrom.strftime("%Y-%m-%d %H:%M:%S") if row.validfrom else None,
                "discountschema_type": row.discountschema_type,
                "isweight": row.isweight,
                "breakvalue": row.breakvalue,
                "validto": row.validto.strftime("%Y-%m-%d %H:%M:%S") if row.validto else None,
            }
            for row in result
        ]

        return [{
            "Status": True,
            "Message": "Successful",
            "Data": discounts
        }]

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={
                "Status": 500,
                "Message": f"An error occurred: {str(e)}",
            }
        )
