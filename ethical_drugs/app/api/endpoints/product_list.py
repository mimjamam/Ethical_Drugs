from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from sqlalchemy.sql import text
from ..models.base import get_db

router = APIRouter()

class Product(BaseModel):
    cbPartnerId: int  

@router.post("/productList")
def get_product_list(request: Product, db: Session = Depends(get_db)):
    """ Get full product list with activity name """
    try:
        if not request.cbPartnerId:
            return [{
                "Status": 400,
                "Message": "You didn't provide supervisorId",
                "Data": []
            }]

        query = text("""
            SELECT 
                mp.value || '-' || mp.name AS product,
                mp.M_Product_ID as "M_Product_ID",
                mp.value AS pro_value,
                mp.name AS pro_name,
                pp.pricestd,
                mp.M_Product_Category_ID as "M_Product_Category_ID" ,
                mp.c_uom_id,
                mp.sku,
                ct.name AS tax,
                tax.name AS taxname,
                tax.rate AS vat,
                pp.m_productprice_id,
                ca.name AS activity_name
            FROM M_Product mp
            JOIN M_ProductPrice pp 
                ON pp.M_Product_ID = mp.M_Product_ID
            JOIN M_PriceList_Version plv 
                ON plv.M_PriceList_Version_ID = pp.M_PriceList_Version_ID
            JOIN c_taxcategory ct 
                ON ct.c_taxcategory_id = mp.c_taxcategory_id
            JOIN c_tax tax 
                ON tax.C_TaxCategory_ID = mp.c_taxcategory_id
            LEFT JOIN c_activity ca
                ON mp.c_activity_id = ca.c_activity_id
            WHERE plv.M_PriceList_ID = 1000002
              AND plv.validfrom = (
                    SELECT MAX(validfrom) 
                    FROM M_PriceList_Version
                    WHERE M_PriceList_ID = 1000002
                      AND validfrom <= NOW()
                      AND isactive = 'Y'
                )
              AND mp.isActive = 'Y'
              AND mp.M_Product_Category_ID IN (
                    SELECT M_Product_Category_ID 
                    FROM M_Product_Category
                    WHERE LOWER(name) LIKE '%finished goods%' 
                       OR LOWER(name) LIKE '%bulk%'
                )
              AND mp.AD_Org_ID = 1000000
              AND mp.M_Product_ID IN (
                    SELECT m_product_id 
                    FROM M_ProductPrice 
                    WHERE m_pricelist_version_id IN (
                        SELECT m_pricelist_version_id 
                        FROM M_PriceList_Version 
                        WHERE M_PriceList_ID = 1000002
                    )
                )
            ORDER BY mp.name;
        """)

        result = db.execute(query).fetchall()

        if not result:
            return [{
                "Status": False,
                "Message": "No products found",
                "Data": []
            }]

        products = [
            {
                "taxname": row.taxname,
                "product": row.product,
                "proCode": row.pro_value,
                "mProductId": row.M_Product_ID,
                "pricestd": row.pricestd,
                "vat": row.vat,
                "cUomId": row.c_uom_id,
                "tax": row.tax,
                "mProductCategoryId": row.M_Product_Category_ID,
                "sku": row.sku,
                "proName": row.pro_name,
                "activityName": row.activity_name
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