# from fastapi import APIRouter, Depends, HTTPException
# from sqlalchemy.orm import Session
# from pydantic import BaseModel
# from sqlalchemy.sql import text
# from ..models.base import get_db

# router = APIRouter()

# class ProfileData(BaseModel):
#     cPartnerId: int

# @router.post("/profileData")
# def profile_data(request: ProfileData, db: Session = Depends(get_db)):
#     """Optimized User Profile Data API with Segment and Warehouse"""

#     if not request.cPartnerId:
#         return {"Status": 400, "Message": "cPartnerId is required", "Data": {}}

#     try:
#         query = text("""
#         SELECT 
#             cbp.value AS cb_partner_value,
#             cbp.name AS cb_partner_name,
#             COALESCE(hj.name, 'N/A') AS hr_job_name,
#             COALESCE(TO_CHAR(cbp.joiningdate, 'DD Mon,YYYY'), 'N/A') AS joining_date,

#             COALESCE(inv.total_sales, 0) AS total_sales,
#             COALESCE(inv.total_vat, 0) AS sales_vat,
#             COALESCE(tsa.value::numeric, 0) AS target,

#             r.c_salesregion_id,
#             COALESCE(cs.name, 'N/A') AS territory_name,
#             COALESCE(p.period_name, 'N/A') AS period,  
#             w.m_warehouse_id,
#             w.warehouse_name,
#             p.period_end,  

#             -- Segment
#             COALESCE(act.name, 'N/A') AS segment

#         FROM c_bpartner cbp

#         -- Job
#         LEFT JOIN hr_job hj 
#                ON hj.hr_job_id = cbp.hr_job_id

#         -- Sales aggregation
#         LEFT JOIN (
#             SELECT ci.c_bpartner_id,
#                    SUM(ci.grandtotal) AS total_sales,
#                    SUM(cit.taxamt)    AS total_vat
#             FROM c_invoice ci
#             LEFT JOIN c_invoicetax cit ON cit.c_invoice_id = ci.c_invoice_id
#             GROUP BY ci.c_bpartner_id
#         ) inv ON inv.c_bpartner_id = cbp.c_bpartner_id

#         -- Supervisor
#         LEFT JOIN t_supervisorassignment tsa 
#                ON tsa.c_bpartner_id = cbp.c_bpartner_id

#         -- Region
#         LEFT JOIN LATERAL (
#             SELECT COALESCE(tsa.c_salesregion_id, cs.territory_id) AS c_salesregion_id
#             FROM t_supervisorassignment tsa
#             LEFT JOIN t_customerassignment cs ON cs.c_bpartner_id = tsa.c_bpartner_id
#             WHERE tsa.c_bpartner_id = cbp.c_bpartner_id
#             LIMIT 1
#         ) r ON TRUE

#         -- Territory
#         LEFT JOIN c_salesregion cs 
#                ON cs.c_salesregion_id = r.c_salesregion_id

#         -- Period
#         LEFT JOIN LATERAL (
#             SELECT cp.name AS period_name,
#                    TO_CHAR(tpc.dateto, 'DDth Mon''YY') AS period_end
#             FROM t_periodclosing tpc
#             JOIN c_period cp ON cp.c_period_id = tpc.c_period_id
#             WHERE tpc.c_salesregion_id = r.c_salesregion_id
#               AND tpc.datefrom <= NOW()
#               AND tpc.dateto >= NOW()
#             ORDER BY tpc.dateto DESC
#             LIMIT 1
#         ) p ON TRUE

#         -- Warehouse
#         LEFT JOIN LATERAL (
#             SELECT mw.m_warehouse_id, mw.name AS warehouse_name
#             FROM t_wh_srassignment cst
#             JOIN m_warehouse mw ON mw.m_warehouse_id = cst.m_warehouse_id
#             WHERE cst.c_salesregion_id = r.c_salesregion_id
#               AND cst.datestart <= NOW()
#               AND (cst.datefinish IS NULL OR cst.datefinish >= NOW())
#             ORDER BY cst.datestart DESC
#             LIMIT 1
#         ) w ON TRUE

#         -- Segment Join
#         LEFT JOIN c_activity act
#                ON act.c_activity_id = cbp.c_activity_id
#               AND act.isactive = 'Y'
#               AND act.name IN ('Human', 'Veterinary')

#         WHERE cbp.c_bpartner_id = :cPartnerId;
#         """)

#         row = db.execute(query, {"cPartnerId": request.cPartnerId}).fetchone()

#         if not row:
#             return {"Status": False, "Message": "No data found for this partner", "Data": {}}

#         response_data = {
#             "cbPartnerValue": row.cb_partner_value,
#             "cbParnerName": row.cb_partner_name,
#             "hrJobName": row.hr_job_name,
#             "joiningDate": row.joining_date,
#             "totalSales": float(row.total_sales),
#             "salesVat": float(row.sales_vat),
#             "target": float(row.target),
#             "cSalesregionId": row.c_salesregion_id,
#             "territoryName": row.territory_name,
#             "period": row.period, 
#             "warehouseId": row.m_warehouse_id,
#             "warehouseName": row.warehouse_name,
#             "collectible": float(row.total_sales) + float(row.sales_vat),
#             "closingDate": row.period_end,
#             "segment": row.segment
#         }

#         return {"Status": True, "Message": "Successful", "Data": response_data}

#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail={"Status": 500, "Message": f"An error occurred: {str(e)}"}
#         )



# from fastapi import APIRouter, Depends, HTTPException
# from sqlalchemy.orm import Session
# from pydantic import BaseModel
# from sqlalchemy.sql import text
# from ..models.base import get_db

# router = APIRouter()

# class ProfileData(BaseModel):
#     cPartnerId: int

# @router.post("/profileData")
# def profile_data(request: ProfileData, db: Session = Depends(get_db)):
#     """User Profile Data API with period-wise opening, collection, closing, and segment"""

#     if not request.cPartnerId:
#         return {"Status": 400, "Message": "cPartnerId is required", "Data": {}}

#     try:
#         query = text("""
#         WITH current_period AS (
#             SELECT datefrom AS period_start_raw, dateto AS period_end_raw
#             FROM t_periodclosing
#             WHERE datefrom <= NOW() AND dateto >= NOW()
#             ORDER BY dateto DESC
#             LIMIT 1
#         )
#         SELECT 
#             cbp.value AS cb_partner_value,
#             cbp.name AS cb_partner_name,
#             COALESCE(hj.name, 'N/A') AS hr_job_name,
#             COALESCE(TO_CHAR(cbp.joiningdate, 'DD Mon,YYYY'), 'N/A') AS joining_date,
#             COALESCE(tsa.value::numeric, 0) AS target,

#             r.c_salesregion_id,
#             COALESCE(cs.name, 'N/A') AS territory_name,
#             COALESCE(p.period_name, 'N/A') AS period,
#             w.m_warehouse_id,
#             w.warehouse_name,

#             -- Period dates formatted
#             TO_CHAR(cp.period_start_raw, 'DDth Mon''YY') AS period_start,
#             TO_CHAR(cp.period_end_raw, 'DDth Mon''YY') AS period_end,

#             -- Opening balance before period
#             COALESCE(opening.opening_bal, 0) AS opening_bal,
#             COALESCE(opening.opening_vat, 0) AS opening_vat,

#             -- Current period sales
#             COALESCE(sales.current_sales, 0) AS current_sales,
#             COALESCE(sales.current_vat, 0) AS current_vat,

#             -- Returns in period
#             COALESCE(returns.return_amt, 0) AS return_amt,
#             COALESCE(returns.return_vat, 0) AS return_vat,

#             -- Payments/Collection in period
#             COALESCE(payments.paid_amt, 0) + COALESCE(payments.over_under, 0) AS total_collected,

#             -- Closing/Due
#             (COALESCE(opening.opening_bal, 0)
#              + COALESCE(sales.current_sales, 0)
#              - COALESCE(returns.return_amt, 0)
#              - (COALESCE(payments.paid_amt, 0) + COALESCE(payments.over_under, 0))
#             ) AS closing_due,

#             -- Segment
#             COALESCE(act.name, 'N/A') AS segment

#         FROM c_bpartner cbp
#         CROSS JOIN current_period cp

#         LEFT JOIN hr_job hj ON hj.hr_job_id = cbp.hr_job_id
#         LEFT JOIN t_supervisorassignment tsa ON tsa.c_bpartner_id = cbp.c_bpartner_id

#         LEFT JOIN LATERAL (
#             SELECT COALESCE(tsa.c_salesregion_id, cs.territory_id) AS c_salesregion_id
#             FROM t_supervisorassignment tsa
#             LEFT JOIN t_customerassignment cs ON cs.c_bpartner_id = tsa.c_bpartner_id
#             WHERE tsa.c_bpartner_id = cbp.c_bpartner_id
#             LIMIT 1
#         ) r ON TRUE

#         LEFT JOIN c_salesregion cs ON cs.c_salesregion_id = r.c_salesregion_id

#         -- Period-wise Opening Balance + VAT
#         LEFT JOIN (
#             SELECT ci.c_bpartner_id,
#                    SUM(ci.grandtotal) AS opening_bal,
#                    SUM(cit.taxamt) AS opening_vat
#             FROM c_invoice ci
#             LEFT JOIN c_invoicetax cit ON cit.c_invoice_id = ci.c_invoice_id
#             CROSS JOIN current_period cp
#             WHERE ci.dateinvoiced < cp.period_start_raw
#             GROUP BY ci.c_bpartner_id
#         ) opening ON cbp.c_bpartner_id = opening.c_bpartner_id

#         -- Current Period Sales + VAT
#         LEFT JOIN (
#             SELECT ci.c_bpartner_id,
#                    SUM(ci.grandtotal) AS current_sales,
#                    SUM(cit.taxamt) AS current_vat
#             FROM c_invoice ci
#             LEFT JOIN c_invoicetax cit ON cit.c_invoice_id = ci.c_invoice_id
#             CROSS JOIN current_period cp
#             WHERE ci.dateinvoiced BETWEEN cp.period_start_raw AND cp.period_end_raw
#             GROUP BY ci.c_bpartner_id
#         ) sales ON cbp.c_bpartner_id = sales.c_bpartner_id

#         -- Returns + VAT
#         LEFT JOIN (
#             SELECT ci.c_bpartner_id,
#                    SUM(ci.grandtotal) AS return_amt,
#                    SUM(cit.taxamt) AS return_vat
#             FROM c_invoice ci
#             LEFT JOIN c_invoicetax cit ON cit.c_invoice_id = ci.c_invoice_id
#             CROSS JOIN current_period cp
#             WHERE ci.dateinvoiced BETWEEN cp.period_start_raw AND cp.period_end_raw
#             GROUP BY ci.c_bpartner_id
#         ) returns ON cbp.c_bpartner_id = returns.c_bpartner_id

#         -- Payments / Collection
#         LEFT JOIN (
#             SELECT cpmt.c_bpartner_id, SUM(cpmt.payamt) AS paid_amt, SUM(cpmt.overunderamt) AS over_under
#             FROM c_payment cpmt
#             CROSS JOIN current_period cp
#             WHERE cpmt.dateacct BETWEEN cp.period_start_raw AND cp.period_end_raw
#             GROUP BY cpmt.c_bpartner_id
#         ) payments ON cbp.c_bpartner_id = payments.c_bpartner_id

#         -- Period info
#         LEFT JOIN LATERAL (
#             SELECT cp2.name AS period_name
#             FROM t_periodclosing tpc
#             JOIN c_period cp2 ON cp2.c_period_id = tpc.c_period_id
#             WHERE tpc.datefrom <= NOW() AND tpc.dateto >= NOW()
#             ORDER BY tpc.dateto DESC
#             LIMIT 1
#         ) p ON TRUE

#         -- Warehouse
#         LEFT JOIN LATERAL (
#             SELECT mw.m_warehouse_id, mw.name AS warehouse_name
#             FROM t_wh_srassignment cst
#             JOIN m_warehouse mw ON mw.m_warehouse_id = cst.m_warehouse_id
#             WHERE cst.c_salesregion_id = r.c_salesregion_id
#               AND cst.datestart <= NOW()
#               AND (cst.datefinish IS NULL OR cst.datefinish >= NOW())
#             ORDER BY cst.datestart DESC
#             LIMIT 1
#         ) w ON TRUE

#         LEFT JOIN c_activity act
#             ON act.c_activity_id = cbp.c_activity_id
#            AND act.isactive = 'Y'
#            AND act.name IN ('Human', 'Veterinary')

#         WHERE cbp.c_bpartner_id = :cPartnerId;
#         """)

#         row = db.execute(query, {"cPartnerId": request.cPartnerId}).fetchone()

#         if not row:
#             return {"Status": False, "Message": "No data found for this partner", "Data": {}}

#         total_sales = float(row.opening_bal) + float(row.current_sales)
#         total_vat = float(row.opening_vat) + float(row.current_vat)
#         collectible = total_sales + total_vat

#         response_data = {
#             "cbPartnerValue": row.cb_partner_value,
#             "cbParnerName": row.cb_partner_name,
#             "hrJobName": row.hr_job_name,
#             "joiningDate": row.joining_date,
#             "totalSales": total_sales,
#             "salesVat": total_vat,
#             "collectible": collectible,
#             "target": float(row.target),
#             "cSalesregionId": row.c_salesregion_id,
#             "territoryName": row.territory_name,
#             "period": row.period,
#             "periodStart": row.period_start,
#             "periodEnd": row.period_end,
#             "warehouseId": row.m_warehouse_id,
#             "warehouseName": row.warehouse_name,
#             "openingBalance": float(row.opening_bal),
#             "currentSales": float(row.current_sales),
#             "returns": float(row.return_amt),
#             "totalCollected": float(row.total_collected),
#             "closingDue": float(row.closing_due),
#             "segment": row.segment
#         }

#         return {"Status": True, "Message": "Successful", "Data": response_data}

#     except Exception as e:
#         raise HTTPException(
#             status_code=500,
#             detail={"Status": 500, "Message": f"An error occurred: {str(e)}"}
#         )


from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pydantic import BaseModel
from ..models.base import get_db

router = APIRouter()

class ProfileData(BaseModel):
    cPartnerId: int

@router.post("/profileData")
def profile_data(request: ProfileData, db: Session = Depends(get_db)):
    if not request.cPartnerId:
        return {"Status": 400, "Message": "cPartnerId is required", "Data": {}}

    try:
        query = text("""
        WITH current_period AS (
            SELECT datefrom AS period_start, dateto AS period_end
            FROM t_periodclosing
            WHERE datefrom <= NOW() AND dateto >= NOW()
            ORDER BY dateto DESC
            LIMIT 1
        ),
        agg_invoice AS (
            SELECT 
                ci.c_bpartner_id,
                SUM(CASE WHEN ci.dateinvoiced < cp.period_start THEN ci.grandtotal ELSE 0 END) AS opening_bal,
                SUM(CASE WHEN ci.dateinvoiced < cp.period_start THEN cit.taxamt ELSE 0 END) AS opening_vat,
                SUM(CASE WHEN ci.dateinvoiced BETWEEN cp.period_start AND cp.period_end THEN ci.grandtotal ELSE 0 END) AS current_sales,
                SUM(CASE WHEN ci.dateinvoiced BETWEEN cp.period_start AND cp.period_end THEN cit.taxamt ELSE 0 END) AS current_vat,
                SUM(CASE WHEN ci.dateinvoiced BETWEEN cp.period_start AND cp.period_end THEN ci.grandtotal ELSE 0 END) AS return_amt
            FROM c_invoice ci
            LEFT JOIN c_invoicetax cit ON cit.c_invoice_id = ci.c_invoice_id
            CROSS JOIN current_period cp
            GROUP BY ci.c_bpartner_id
        ),
        agg_payment AS (
            SELECT
                cpmt.c_bpartner_id,
                SUM(cpmt.payamt) AS paid_amt,
                SUM(cpmt.overunderamt) AS over_under
            FROM c_payment cpmt
            CROSS JOIN current_period cp
            WHERE cpmt.dateacct BETWEEN cp.period_start AND cp.period_end
            GROUP BY cpmt.c_bpartner_id
        ),
        partner_region AS (
            SELECT tsa.c_bpartner_id,
                   COALESCE(tsa.c_salesregion_id, cs.territory_id) AS c_salesregion_id
            FROM t_supervisorassignment tsa
            LEFT JOIN t_customerassignment cs ON cs.c_bpartner_id = tsa.c_bpartner_id
        )
        SELECT 
            cbp.value AS cb_partner_value,
            cbp.name AS cb_partner_name,
            COALESCE(hj.name,'N/A') AS hr_job_name,
            COALESCE(TO_CHAR(cbp.joiningdate,'DD Mon,YYYY'),'N/A') AS joining_date,
            COALESCE(tsa.value::numeric,0) AS target,
            pr.c_salesregion_id,
            COALESCE(cs.name,'N/A') AS territory_name,
            COALESCE(cp2.name,'N/A') AS period,
            w.m_warehouse_id,
            w.name AS warehouse_name,
            TO_CHAR(cp.period_start,'DDth Mon''YY') AS period_start,
            TO_CHAR(cp.period_end,'DDth Mon''YY') AS period_end,
            COALESCE(ai.opening_bal,0) AS opening_bal,
            COALESCE(ai.opening_vat,0) AS opening_vat,
            COALESCE(ai.current_sales,0) AS current_sales,
            COALESCE(ai.current_vat,0) AS current_vat,
            COALESCE(ai.return_amt,0) AS return_amt,
            COALESCE(ap.paid_amt,0)+COALESCE(ap.over_under,0) AS total_collected,
            (COALESCE(ai.opening_bal,0)+COALESCE(ai.current_sales,0)
             - COALESCE(ai.return_amt,0)
             - (COALESCE(ap.paid_amt,0)+COALESCE(ap.over_under,0))) AS closing_due,
            COALESCE(act.name,'N/A') AS segment
        FROM c_bpartner cbp
        CROSS JOIN current_period cp
        LEFT JOIN hr_job hj ON hj.hr_job_id = cbp.hr_job_id
        LEFT JOIN t_supervisorassignment tsa ON tsa.c_bpartner_id = cbp.c_bpartner_id
        LEFT JOIN partner_region pr ON pr.c_bpartner_id = cbp.c_bpartner_id
        LEFT JOIN c_salesregion cs ON cs.c_salesregion_id = pr.c_salesregion_id
        LEFT JOIN c_period cp2 ON cp2.c_period_id = (SELECT c_period_id FROM t_periodclosing tpc2 WHERE tpc2.datefrom <= NOW() AND tpc2.dateto >= NOW() ORDER BY tpc2.dateto DESC LIMIT 1)
        LEFT JOIN m_warehouse w ON w.m_warehouse_id = (
            SELECT mw.m_warehouse_id
            FROM t_wh_srassignment cst
            JOIN m_warehouse mw ON mw.m_warehouse_id = cst.m_warehouse_id
            WHERE cst.c_salesregion_id = pr.c_salesregion_id
              AND cst.datestart <= NOW()
              AND (cst.datefinish IS NULL OR cst.datefinish >= NOW())
            ORDER BY cst.datestart DESC
            LIMIT 1
        )
        LEFT JOIN agg_invoice ai ON ai.c_bpartner_id = cbp.c_bpartner_id
        LEFT JOIN agg_payment ap ON ap.c_bpartner_id = cbp.c_bpartner_id
        LEFT JOIN c_activity act ON act.c_activity_id = cbp.c_activity_id
            AND act.isactive='Y'
            AND act.name IN ('Human','Veterinary')
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
            "target": float(row.target),
            "cSalesregionId": row.c_salesregion_id,
            "territoryName": row.territory_name,
            "period": row.period,
            "warehouseId": row.m_warehouse_id,
            "warehouseName": row.warehouse_name,
            "periodStart": row.period_start,
            "periodEnd": row.period_end,
            "openingBal": float(row.opening_bal),
            "openingVat": float(row.opening_vat),
            "currentSales": float(row.current_sales),
            "currentVat": float(row.current_vat),
            "returnAmt": float(row.return_amt),
            "totalCollected": float(row.total_collected),
            "closingDue": float(row.closing_due),
            "collectible": round(float(row.current_sales) + float(row.current_vat), 2),
            "segment": row.segment
        }

        return {"Status": True, "Message": "Successful", "Data": response_data}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"Status": 500, "Message": f"An error occurred: {str(e)}"}
        )

