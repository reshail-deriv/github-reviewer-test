-- Query size: 618.37 MB, Elapsed time: 6 sec, Slot time consumed: 1 min 12 sec, Expected used Date Range: 2 year, Data source: BQ, Project: deriv-bi-reporting
WITH overall AS (
  SELECT DATE(pt.created_date) AS created_date
       , pt.trace_id
       , 'premier_cashier' AS cashier_type
       , CONCAT( IF(pt.Gaming_User NOT IN ('AutoRejection','INTERNET','PayOpsIT','payout-automation-1','payout-automation-3','payout-automation-malaysia-1','payout-automation-malaysia-2','payout-automation-malaysia-3','AutoApproval','0','macroipoh','macrorwanda','macro.rw','macroparaguay','macro','macrouae','PayOps-IT','')
                  , pt.Gaming_User, 'system')
                 , ','
                 , IF(pt.Accounting_User NOT IN ('0','AutoRejection','AutoApproval','macroipoh','macrorwanda','macro.rw','macroparaguay','macro','macrouae','PayOps-IT','')
                    , pt.Accounting_User, 'system')
                 , ','
                 , IF(pt.Edited_By NOT IN ('AutoRejection','INTERNET','PayOpsIT','payout-automation-1','payout-automation-2','payout-automation-3','payout-automation-malaysia-1','payout-automation-malaysia-2','payout-automation-malaysia-3','AutoApproval','0','macroipoh','macrorwanda','macro.rw','macroparaguay','macro','macrouae','PayOps-IT','')
                    , pt.Edited_By, 'system') ) AS approved
    FROM doughflow_fivetran.payout_transactions AS pt
    JOIN doughflow_fivetran.sportsbooks AS sb ON pt.sbook_id = sb.sbook_id
   WHERE DATE(pt.created_date) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 YEAR)
     AND LOWER(sb.front_end_name) NOT LIKE '%uae%'
     AND DATE(pt.created_date) >= <Parameters.Start Date>
     AND DATE(pt.created_date) <= <Parameters.End Date>

   UNION ALL

  SELECT DATE(created_time) AS created_date
       , id AS trace_id
       , 'crypto_cashier' AS cashier_type
       , CASE WHEN authorisers IN ('{}','{system}') THEN 'system'
              ELSE TRIM(authorisers,'{}')
         END AS approved
    FROM bi.bo_cryptocurrency
   WHERE DATE(created_time) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), MONTH), INTERVAL 2 YEAR)
     AND DATE(created_time) >= <Parameters.Start Date>
     AND DATE(created_time) <= <Parameters.End Date>
     AND transaction_type = 'withdrawal'
)

SELECT * EXCEPT(approved)
  FROM overall
     , UNNEST(SPLIT(LOWER(approved))) AS approved_by
 ORDER BY 1
