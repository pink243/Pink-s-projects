
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month, 
    COUNT(totals.visits) AS visits, 
    SUM(totals.pageviews) AS pageviews, 
    SUM(totals.transactions) AS transactions, 
    SUM(totals.totaltransactionrevenue)/(POWER(10,6)) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _TABLE_SUFFIX BETWEEN '0101'AND'0331'
GROUP BY month
ORDER BY month;


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL

SELECT
    trafficSource.source AS source,
    SUM(totals.visits) AS total_visits,
    SUM(totals.Bounces) AS total_no_of_bounces,
    (SUM(totals.Bounces)/SUM(totals.visits))* 100 AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY source
ORDER BY total_visits DESC;

-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL

WITH month_data AS
(SELECT 
     "Month" as time_type,
     FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS time,
     trafficSource.source AS source,
     SUM(totals.transactionRevenue) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY source, time),

week_data AS
(SELECT 
     "Week" as time_type,
     FORMAT_DATE("%Y%W", PARSE_DATE("%Y%m%d", date)) AS time,
     trafficSource.source AS source,
     SUM(totals.transactionRevenue) AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY source, time)
SELECT * FROM month_data
UNION ALL
SELECT * FROM week_data;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

WITH purchaser_data AS
(SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    (SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid)) AS avg_pageviews_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0601' AND '0731'
AND totals.transactions>=1
GROUP BY month),

non_purchaser_data AS
(SELECT
FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
SUM(totals.pageviews)/COUNT(DISTINCT fullvisitorid) AS avg_pageviews_non_purchase,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE _table_suffix BETWEEN '0601' AND '0731'
AND totals.transactions is null
GROUP BY month)

SELECT
    pd.*,
    avg_pageviews_non_purchase
FROM purchaser_data pd
LEFT JOIN non_purchaser_data USING (month)
ORDER BY pd.month;


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    SUM(totals.transactions) / COUNT(DISTINCT fullvisitorid) AS Avg_total_transactions_per_user
FROM`bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
WHERE  totals.transactions IS NOT NULL 
GROUP BY month;


-- Query 06: Average amount of money spent per session
#standardSQL

SELECT 
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    SUM(totals.totaltransactionrevenue) / SUM(totals.visits) AS avg_revenue_by_user_per_visit
  from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*` 
WHERE totals.transactions IS NOT NULL
group by month;



-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

WITH buyer_list AS 
(SELECT
    distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND product.productRevenue IS NOT NULL)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) AS product
JOIN buyer_list USING (fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue IS NOT NULL
GROUP BY other_purchased_products
ORDER BY quantity DESC;



--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

SELECT
    FORMAT_DATE('%Y%m', PARSE_DATE('%Y%m%d', date)) AS month,
    COUNT(case when hits.eCommerceAction.action_type = '2'then 1 else null end) AS num_product_view,
    COUNT(case when hits.eCommerceAction.action_type = '3'then 1 else null end) AS num_addtocard,
    COUNT(case when hits.eCommerceAction.action_type = '6'then 1 else null end) AS num_purchase,
    COUNT(case when hits.eCommerceAction.action_type = '3'then 1 else null end)/COUNT(case when hits.eCommerceAction.action_type = '2'then 1 else null end)) *100 AS ad_to_cart_rate,
    COUNT(case when hits.eCommerceAction.action_type = '6'then 1 else null end)/COUNT(case when hits.eCommerceAction.action_type = '2'then 1 else null end)) *100 AS purchase_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
  unnest (hits) hits
WHERE _table_suffix BETWEEN '0101' AND '0331'
GROUP BY month
ORDER BY month;
