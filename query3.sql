-- Query size: 521 MB , Running time: 490 msec, Slot time consumed:7 sec, Expected used Date Range: daily, Data source: BQ , Project : deriv-bi-reporting

WITH dw AS (
  SELECT email
       , account_status
    FROM dynamicworks.partner
   WHERE account_status = 'Active'
)

SELECT bc.residence_country AS residence
     , DATE(bc.date_joined) AS joined_date
     , COUNT(DISTINCT bc.binary_user_id) AS total_signup
  FROM bi.bo_client AS bc
  JOIN dw -- joining with data from dw to check two conditions : account status active and account needs to be there in DW
    ON bc.email = dw.email
 WHERE bc.account_type = 'partner'
   AND DATE(bc.date_joined) >= '2025-06-24'
   AND bc.is_internal_client IS FALSE
   AND bc.email NOT LIKE '%tstmail.link%'
   AND bc.email NOT LIKE '%deriv%'
 GROUP BY ALL
