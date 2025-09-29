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


import math
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from pydantic import BaseModel
from ..models.base import get_db

router = APIRouter()

class SalesRepRequest(BaseModel):
    adUserId: int

@router.post("/profileData")
def salesrep_data(request: SalesRepRequest, db: Session = Depends(get_db)):
    if not request.adUserId:
        return {"Status": 400, "Message": "adUserId is required", "Data": {}}

    try:
        # 1. Current period with DDth Mon''YY format
        period_query = text("""
            SELECT 
                TO_CHAR(tp.datefrom,'DDth Mon''YY') AS period_start,
                TO_CHAR(tp.dateto,'DDth Mon''YY') AS period_end,
                cp.name AS period_name
            FROM t_periodclosing tp
            JOIN c_period cp ON cp.c_period_id = tp.c_period_id
            WHERE tp.isactive = 'Y'
              AND cp.isactive = 'Y'
              AND tp.ad_client_id = 1000000
              AND CURRENT_DATE BETWEEN tp.datefrom AND tp.dateto
            LIMIT 1
        """)
        period = db.execute(period_query).fetchone()
        if not period:
            return {"Status": False, "Message": "No active period found", "Data": {}}

        period_start = period.period_start
        period_end = period.period_end
        period_name = period.period_name

        # 2. Sales data
        sales_query = text("""
            SELECT
                SUM(ci.grandtotal) AS salesintp85,
                SUM(ci.totallines) AS tot,
                SUM(ci.grandtotal) - SUM(ci.totallines) AS vat
            FROM c_invoice ci
            JOIN c_doctype dt ON dt.c_doctype_id = ci.c_doctype_id
            WHERE ci.salesrep_id = :adUserId
              AND ci.dateinvoiced::date BETWEEN (SELECT datefrom FROM t_periodclosing tp2 JOIN c_period cp2 ON cp2.c_period_id = tp2.c_period_id WHERE tp2.isactive='Y' AND cp2.isactive='Y' AND tp2.ad_client_id=1000000 AND CURRENT_DATE BETWEEN tp2.datefrom AND tp2.dateto LIMIT 1)
                                        AND (SELECT dateto FROM t_periodclosing tp2 JOIN c_period cp2 ON cp2.c_period_id = tp2.c_period_id WHERE tp2.isactive='Y' AND cp2.isactive='Y' AND tp2.ad_client_id=1000000 AND CURRENT_DATE BETWEEN tp2.datefrom AND tp2.dateto LIMIT 1)
        """)
        sales = db.execute(sales_query, {"adUserId": request.adUserId}).fetchone()

        # 3. Submission data
        submission_query = text("""
            SELECT SUM(al.amount) AS submission
            FROM c_invoice ci
            JOIN c_allocationline al ON ci.c_invoice_id = al.c_invoice_id
            WHERE ci.salesrep_id = :adUserId
              AND ci.dateinvoiced::date BETWEEN (SELECT datefrom FROM t_periodclosing tp2 JOIN c_period cp2 ON cp2.c_period_id = tp2.c_period_id WHERE tp2.isactive='Y' AND cp2.isactive='Y' AND tp2.ad_client_id=1000000 AND CURRENT_DATE BETWEEN tp2.datefrom AND tp2.dateto LIMIT 1)
                                        AND (SELECT dateto FROM t_periodclosing tp2 JOIN c_period cp2 ON cp2.c_period_id = tp2.c_period_id WHERE tp2.isactive='Y' AND cp2.isactive='Y' AND tp2.ad_client_id=1000000 AND CURRENT_DATE BETWEEN tp2.datefrom AND tp2.dateto LIMIT 1)
        """)
        submission = db.execute(submission_query, {"adUserId": request.adUserId}).fetchone()

        # 4. Sales rep info (separate query)
        rep_info_query = text("""
            SELECT
                cbp.value AS cb_partner_value,
                cbp.name AS cb_partner_name,
                COALESCE(hj.name,'N/A') AS hr_job_name,
                COALESCE(TO_CHAR(cbp.joiningdate,'DD Mon,YYYY'),'N/A') AS joining_date,
                COALESCE(tsa.value::numeric,0) AS target,
                pr.c_salesregion_id,
                COALESCE(cs.name,'N/A') AS territory_name,
                COALESCE(w.m_warehouse_id,0) AS m_warehouse_id,
                COALESCE(w.name,'N/A') AS warehouse_name
            FROM ad_user au
            JOIN c_bpartner cbp ON cbp.c_bpartner_id = au.c_bpartner_id
            LEFT JOIN hr_job hj ON hj.hr_job_id = cbp.hr_job_id
            LEFT JOIN t_supervisorassignment tsa ON tsa.c_bpartner_id = cbp.c_bpartner_id
            LEFT JOIN (
                SELECT tsa.c_bpartner_id,
                       COALESCE(tsa.c_salesregion_id, cs.territory_id) AS c_salesregion_id
                FROM t_supervisorassignment tsa
                LEFT JOIN t_customerassignment cs ON cs.c_bpartner_id = tsa.c_bpartner_id
            ) pr ON pr.c_bpartner_id = cbp.c_bpartner_id
            LEFT JOIN c_salesregion cs ON cs.c_salesregion_id = pr.c_salesregion_id
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
            WHERE au.ad_user_id = :adUserId
        """)
        rep_info = db.execute(rep_info_query, {"adUserId": request.adUserId}).fetchone()

        # Calculations with ceiling
        total_sales = math.ceil(float(sales.tot if sales.tot else 0))
        total_vat = math.ceil(float(sales.vat if sales.vat else 0))
        returnAmt = math.ceil(float(sales.salesintp85 if sales.salesintp85 else 0))
        totalCollected = math.ceil(float(submission.submission if submission.submission else 0))
        collectible = math.ceil(total_vat + returnAmt)
        closingDue = math.ceil(returnAmt - totalCollected)
        target = math.ceil(float(rep_info.target)) if rep_info.target else 0
        cSalesregionId = math.ceil(float(rep_info.c_salesregion_id)) if rep_info.c_salesregion_id else 0
        warehouseId = math.ceil(float(rep_info.m_warehouse_id)) if rep_info.m_warehouse_id else 0

        # Response in strict order
        response_data = {
            "cbPartnerValue": rep_info.cb_partner_value,
            "cbParnerName": rep_info.cb_partner_name,
            "hrJobName": rep_info.hr_job_name,
            "joiningDate": rep_info.joining_date,
            "totalSales": total_sales,
            "salesVat": total_vat,
            "returnAmt": returnAmt,
            "totalCollected": totalCollected,
            "collectible": collectible,
            "closingDue": closingDue,
            "target": target,
            "cSalesregionId": cSalesregionId,
            "territoryName": rep_info.territory_name,
            "period": period_name,
            "periodStart": period_start,
            "periodEnd": period_end,
            "warehouseId": warehouseId,
            "warehouseName": rep_info.warehouse_name
        }

        return {"Status": True, "Message": "Successful", "Data": response_data}

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail={"Status": 500, "Message": f"An error occurred: {str(e)}"}
        )
