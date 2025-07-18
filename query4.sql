-- Data source: NRS

WITH locked_users AS (
    -- For DE reference: Using full table alias as metabase report is using field filter
    -- Get a list of user whose account(s) have been locked and isn't deleted. If the lock is deleted, it will be removed from the client status table
    SELECT bo.client_vw.binary_user_id
         , bo.client_vw.is_internal_client
         , cs.client_loginid
         , cs.last_modified_date
         , cs.reason
         , LOWER(cs.reason) LIKE 'payment related%loss%' OR LOWER(cs.reason) LIKE 'other%loss%'AS is_payment_related_loss
         , ROUND(COALESCE(a.balance_usd,0),2) AS bo_balance_usd
      FROM bo.client_vw
      JOIN bo.client_status AS cs
        ON bo.client_vw.loginid = cs.client_loginid
      LEFT JOIN bo.account AS a
        ON bo.client_vw.loginid = a.client_loginid
    [[WHERE cs.last_modified_date >= {{locked_date}}]]
       AND cs.status_code = 'no_withdrawal_or_trading'
       AND bo.client_vw.is_internal_client IS FALSE
       AND {{ loginid }}
)

, bo_sibling AS (
    -- Deriv balance
    SELECT a.binary_user_id
         , a.client_loginid
         , a.currency_code
         , ROUND(a.balance_usd,2) AS bo_balance_usd
      FROM bo.account AS a
      JOIN locked_users ON a.binary_user_id = locked_users.binary_user_id
       AND a.client_loginid <> locked_users.client_loginid
     WHERE a.balance_usd <> 0
)

, bo_agg AS (
    -- Deriv total balance
    SELECT binary_user_id
         , STRING_AGG(DISTINCT CONCAT(client_loginid::TEXT,' (',bo_balance_usd,')'),', ') AS bo_loginid_n_balance_usd
         , SUM(bo_balance_usd) AS total_bo_balance_usd
      FROM bo_sibling
     GROUP BY 1
)

, mt5 AS (
    -- MT5 balance
    SELECT mt5.binary_user_id
         , mt5.login
         , mt5.balance AS mt5_balance
      FROM mt5.user AS mt5
      JOIN locked_users ON mt5.binary_user_id = locked_users.binary_user_id
    WHERE mt5.balance <> 0
    GROUP BY 1,2,3
)

, mt5_agg AS (
    -- MT5 total balance
    SELECT mt5.binary_user_id
         , STRING_AGG(DISTINCT CONCAT(mt5.login::TEXT,' (', mt5_balance::TEXT,')'),', ') AS mt5_login_n_balace
         , SUM(mt5.mt5_balance) AS total_mt5_balance
      FROM mt5
     GROUP BY 1
)

  -- Combining both Deriv and MT5 including total amount of balance for BO and Deriv respectively
SELECT lu.client_loginid
     , lu.bo_balance_usd
     , lu.reason AS locked_reason
     , lu.last_modified_date
     , ba.bo_loginid_n_balance_usd
     , ma.mt5_login_n_balace
     , ba.total_bo_balance_usd
     , ma.total_mt5_balance
     , lu.bo_balance_usd + COALESCE(ba.total_bo_balance_usd,0) + COALESCE(ma.total_mt5_balance,0) AS total_balance
     , COALESCE(lu.binary_user_id,ma.binary_user_id, ba.binary_user_id) AS binary_user_id
  FROM locked_users AS lu
  LEFT JOIN bo_agg AS ba ON lu.binary_user_id = ba.binary_user_id
  LEFT JOIN mt5_agg AS ma ON lu.binary_user_id = ma.binary_user_id
 [[WHERE is_payment_related_loss::TEXT = LOWER({{ is_payment_related_loss }})]] -- BI-9547
 GROUP BY 1,2,3,4,5,6,7,8,9,10
HAVING (lu.bo_balance_usd + COALESCE(ba.total_bo_balance_usd,0) + COALESCE(ma.total_mt5_balance,0)) != 0
 ORDER BY 10, 9 DESC
