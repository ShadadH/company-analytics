-- Creating a New Database
CREATE DATABASE Ziina;
use Ziina;

-- Setting Up Tables

CREATE TABLE accounts_table (
    account_id Int,
    channel VARCHAR(255) NOT NULL
);


create table transactions_(
	transaction_id Int,
    account_id int,
    transaction_month date,
    revenue decimal(5,2));
    
-- Loading Data and Setting Keys

LOAD DATA LOCAL INFILE '/Users/macbookair/Downloads/Tables/Transactions_Table.csv' INTO TABLE transactions_
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

LOAD DATA LOCAL INFILE '/Users/macbookair/Downloads/Tables/Accounts_Table.csv' INTO TABLE accounts_table
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


-- Joining Tables

CREATE TABLE joined_table AS
SELECT t.transaction_id, t.transaction_month, t.revenue, a.account_id, a.channel
FROM transactions_ t
JOIN accounts_table a
ON t.account_id = a.account_id;


-- Monthly Analysis

SELECT MONTH(transaction_month) AS month,
       SUM(revenue) AS total_revenue,
       COUNT(DISTINCT transaction_id) AS total_transactions
FROM joined_table
GROUP BY MONTH(transaction_month)
ORDER BY month ASC;



    
-- Adding another Column for the 3 Letter Abbrevation for the Cohort Month

ALTER TABLE joined_table
ADD COLUMN cohort date;
  
WITH cohort AS (
  SELECT
    account_id,
    MIN(transaction_month) AS first_transaction_month
  FROM
    joined_table
  GROUP BY
    account_id
)
UPDATE
  joined_table 
JOIN
  cohort c
ON
  joined_table.account_id = c.account_id
SET
  joined_table.cohort = DATE_FORMAT(c.first_transaction_month, '%Y-%m-%d');
  
ALTER TABLE joined_table
ADD COLUMN cohort_month VARCHAR(3);

UPDATE joined_table
SET cohort_month = DATE_FORMAT(cohort, '%b');
SET sql_safe_updates = 0;

-- Churn Rate Calculation

WITH monthly_activity AS (
  SELECT
    transaction_month,
    account_id,
    LEAD(transaction_month, 1) OVER(PARTITION BY account_id ORDER BY transaction_month) AS next_active_month,
    MAX(transaction_month) OVER() AS max_month -- Calculate the max transaction_month
  FROM joined_table
  GROUP BY transaction_month, account_id
),
churned_accounts AS (
  SELECT
    transaction_month,
    COUNT(*) AS churned_customers
  FROM monthly_activity
  WHERE (next_active_month IS NULL OR next_active_month > transaction_month + INTERVAL 1 MONTH)
    AND transaction_month < max_month -- Exclude the last month from churn calculation
  GROUP BY transaction_month
),
active_accounts AS (
  SELECT
    transaction_month,
    COUNT(DISTINCT account_id) AS active_customers
  FROM joined_table
  GROUP BY transaction_month
)
SELECT
  active_accounts.transaction_month,
  active_customers,
  IFNULL(churned_customers, 0) AS churned_customers,
  (IFNULL(churned_customers, 0) / active_customers) * 100 AS churn_rate
FROM active_accounts
LEFT JOIN churned_accounts USING (transaction_month)
ORDER BY transaction_month;

-- Cohort Analysis

SELECT
  cohort,
  cohort_month,
  COUNT(DISTINCT account_id) AS num_accounts,
  SUM(revenue) AS total_revenue,
  COUNT(transaction_id) AS total_transactions,
  SUM(revenue) / COUNT(DISTINCT account_id) AS revenue_per_cohort,
  COUNT(transaction_id) / COUNT(DISTINCT account_id) AS transactions_per_cohort
FROM
  joined_table
GROUP BY
  cohort, cohort_month
ORDER BY
  cohort ASC;
  
-- Channels Analysis

SELECT 
  c.channel, 
  c.total_revenue, 
  a.total_count
FROM
  (
    SELECT 
      channel, 
      SUM(revenue) AS total_revenue
    FROM 
      joined_table
    GROUP BY 
      channel
    ORDER BY 
      total_revenue DESC
  ) c
LEFT JOIN
  (
    SELECT 
      channel, 
      COUNT(channel) AS total_count
    FROM 
      accounts_table
    GROUP BY 
      channel
  ) a
ON c.channel = a.channel;

-- Top Users Analysis

SELECT account_id, 
       SUM(revenue) AS total_revenue,
       COUNT(*) AS purchases_quantity,
       channel
FROM joined_table
GROUP BY account_id, channel
order by purchases_quantity desc;


















