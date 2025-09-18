from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from sqlalchemy.sql import text
from ..models.base import get_db

router = APIRouter()

class ProfileData(BaseModel):
    cPartnerId: int

@router.post("/profileData")
def profile_data(request: ProfileData, db: Session = Depends(get_db)):
    """Optimized User Profile Data API"""

    if not request.cPartnerId:
        return {"Status": 400, "Message": "cPartnerId is required", "Data": {}}

    try:
        query = text("""
        SELECT 
            cbp.value AS cb_partner_value,
            cbp.name AS cb_partner_name,
            COALESCE(hj.name, 'N/A') AS hr_job_name,
            COALESCE(TO_CHAR(cbp.joiningdate, 'DD Mon,YYYY'), 'N/A') AS joining_date,

            COALESCE(inv.total_sales, 0) AS total_sales,
            COALESCE(inv.total_vat, 0) AS sales_vat,
            COALESCE(tsa.value::numeric, 0) AS target,

            r.c_salesregion_id,
            COALESCE(cs.name, 'N/A') AS territory_name,
            COALESCE(p.period_name || ' (' || TO_CHAR(p.period_end, 'DD Mon,YYYY') || ')', 'N/A') AS period,
            w.m_warehouse_id,
            w.warehouse_name

        FROM c_bpartner cbp

        -- Job
        LEFT JOIN hr_job hj 
               ON hj.hr_job_id = cbp.hr_job_id

        -- Sales aggregation
        LEFT JOIN (
            SELECT ci.c_bpartner_id,
                   SUM(ci.grandtotal) AS total_sales,
                   SUM(cit.taxamt)    AS total_vat
            FROM c_invoice ci
            LEFT JOIN c_invoicetax cit ON cit.c_invoice_id = ci.c_invoice_id
            GROUP BY ci.c_bpartner_id
        ) inv ON inv.c_bpartner_id = cbp.c_bpartner_id

        -- Supervisor
        LEFT JOIN t_supervisorassignment tsa 
               ON tsa.c_bpartner_id = cbp.c_bpartner_id

        -- Region (LATERAL JOIN = only 1 row)
        LEFT JOIN LATERAL (
            SELECT COALESCE(tsa.c_salesregion_id, cs.territory_id) AS c_salesregion_id
            FROM t_supervisorassignment tsa
            LEFT JOIN t_customerassignment cs ON cs.c_bpartner_id = tsa.c_bpartner_id
            WHERE tsa.c_bpartner_id = cbp.c_bpartner_id
            LIMIT 1
        ) r ON TRUE

        -- Territory
        LEFT JOIN c_salesregion cs 
               ON cs.c_salesregion_id = r.c_salesregion_id

        -- Period (latest active)
        LEFT JOIN LATERAL (
            SELECT cp.name AS period_name, tpc.dateto AS period_end
            FROM t_periodclosing tpc
            JOIN c_period cp ON cp.c_period_id = tpc.c_period_id
            WHERE tpc.c_salesregion_id = r.c_salesregion_id
              AND tpc.datefrom <= NOW()
              AND tpc.dateto >= NOW()
            ORDER BY tpc.dateto DESC
            LIMIT 1
        ) p ON TRUE

        -- Warehouse (latest active)
        LEFT JOIN LATERAL (
            SELECT mw.m_warehouse_id, mw.name AS warehouse_name
            FROM t_wh_srassignment cst
            JOIN m_warehouse mw ON mw.m_warehouse_id = cst.m_warehouse_id
            WHERE cst.c_salesregion_id = r.c_salesregion_id
              AND cst.datestart <= NOW()
              AND (cst.datefinish IS NULL OR cst.datefinish >= NOW())
            ORDER BY cst.datestart DESC
            LIMIT 1
        ) w ON TRUE

        WHERE cbp.c_bpartner_id = :cPartnerId;
        """)

        row = db.execute(query, {"cPartnerId": request.cPartnerId}).fetchone()

        if not row:
            return {"Status": False, "Message": "No data found for this partner", "Data": {}}

        response_data = {
            "cbPartnerValue": row.cb_partner_value,
            "cbParnerName": row.cb_partner_name,
            "hrJobName": row.hr_job_name,
            "joiningDate": row.joining_date,
            "totalSales": float(row.total_sales),
            "salesVat": float(row.sales_vat),
            "target": float(row.target),
            "cSalesregionId": row.c_salesregion_id,
            "territoryName": row.territory_name,
            "period": row.period,
            "warehouseId": row.m_warehouse_id,
            "warehouseName": row.warehouse_name,
        }

        return {"Status": True, "Message": "Successful", "Data": response_data}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"Status": 500, "Message": f"An error occurred: {str(e)}"}
        )
