-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month

#standardSQL
SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      count(fullVisitorId) as visits,
      sum(totals.pageviews) as pageviews,
      sum(totals.transactions) as transactions,
      sum(totals.transactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE _table_suffix between '20170101' and '20170331'
GROUP BY month
ORDER BY month ASC;


-- Query 02: Bounce rate per traffic source in July 2017

#standardSQL
SELECT trafficSource.source as source,
      count(fullVisitorId) as total_visits,
      sum(totals.bounces) as total_no_of_bounces,
      (sum(totals.bounces)/count(fullVisitorId))*100 as bounce_rate,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY trafficSource.source
ORDER BY total_visits DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017

#standardSQL
WITH month as (
SELECT concat("month") as time_type,
       format_date("%Y%m",parse_date("%Y%m%d",date)) as time,
       trafficSource.source as source,
       sum(totals.transactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY time, source
ORDER BY revenue DESC
),
week as (
SELECT concat("week") as time_type,
       format_date("%Y%W",parse_date("%Y%m%d",date)) as time,
       trafficSource.source as source,
       sum(totals.transactionRevenue)/1000000 as revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY time, source
ORDER BY revenue DESC
)
SELECT *
FROM month
UNION ALL 
SELECT *
FROM week
ORDER BY 4 DESC;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser


#standardSQL
WITH cte as (
      SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
             Case when totals.transactions is null then "non_purchase"
                  else "purchase"
                  end as type_user,
             count(distinct fullVisitorId) as num_of_visits,
             sum(totals.pageviews) as pageviews
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
      WHERE _table_suffix between '20170601' and '20170731'
      GROUP BY month, type_user
      ORDER BY month asc
),
cte2 as(
      SELECT month,
             pageviews/num_of_visits as avg_pageviews_purchase
      FROM cte
      WHERE type_user = 'purchase'
),
cte3 as (
      SELECT month,
             pageviews/num_of_visits as avg_pageviews_non_purchase
      FROM cte
      WHERE type_user = 'non_purchase'   
)
SELECT cte2.month,
       avg_pageviews_purchase,
       avg_pageviews_non_purchase
FROM cte2
LEFT JOIN cte3
ON cte2.month = cte3.month


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
             sum(totals.transactions)/count(distinct fullVisitorId) as Avg_total_transactions_per_user
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      where totals.totalTransactionRevenue is not null
      Group by month


#standardSQL
WITH cte as (
      SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
             Case when totals.transactions is null then "non_purchase"
                  else "purchase"
                  end as type_user,
             count(distinct fullVisitorId) as num_of_visits,
             sum(totals.transactions) as total_transaction
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      GROUP BY month, type_user
      ORDER BY month asc
)
SELECT cte.month,
       total_transaction/num_of_visits as Avg_total_transactions_per_user
FROM cte
WHERE type_user = 'purchase'

-- Query 06: Average amount of money spent per session

#standardSQL
SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
             sum(totals.totalTransactionRevenue)/count(distinct fullVisitorId) as avg_revenue_by_user_per_visit
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      where totals.totalTransactionRevenue is not null
      Group by month


#standardSQL
WITH cte as (
      SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
             Case when totals.transactions is null then "non_purchase"
                  else "purchase"
                  end as type_user,
             count(distinct fullVisitorId) as num_of_visits,
             sum(totals.totalTransactionRevenue) as revenue
      FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
      GROUP BY month, type_user
      ORDER BY month asc
)
SELECT month,
       revenue/num_of_visits as avg_revenue_by_user_per_visit
FROM cte
WHERE type_user = "purchase";

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)

#standardSQL
WITH visitor_id as (
    SELECT DISTINCT fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
       UNNEST(hits) AS hits,
       UNNEST(hits.product) as product
    WHERE v2ProductName = "YouTube Men's Vintage Henley" and product.productRevenue is not null
)
SELECT a.v2ProductName as other_purchased_products,
        sum(a.productQuantity) as quantity
FROM visitor_id 
LEFT JOIN (
          SELECT * from `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
            UNNEST(hits) AS hits,
            UNNEST(hits.product) as product ) as a 
ON visitor_id.fullVisitorId = a.fullVisitorId 
WHERE a.productRevenue is not null and a.v2ProductName <> "YouTube Men's Vintage Henley"
GROUP BY a.v2ProductName
ORDER BY quantity DESC


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH cte as (
    SELECT format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
            case when hits.eCommerceAction.action_type = '2' then "view_product"
                 when hits.eCommerceAction.action_type = '3' then "addtocart"
                 when hits.eCommerceAction.action_type = '6' then "purchase"
            END as action
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
       UNNEST(hits) AS hits,
       UNNEST(hits.product) as product
    WHERE _table_suffix between "20170101" and "20170331"
),
cte1 as (
    select cte.month,
         count(cte.action) as number_product_view
    from cte
    where cte.action = "view_product"
    group by month
),
cte2 as (
    select cte.month,
         count(action) as num_addtocart
    from cte
    where action = "addtocart"
    group by month
),
cte3 as (
    select cte.month,
         count(action) as num_purchase
    from cte
    where action = "purchase"
    group by month
)
SELECT cte1.month,
    cte1.number_product_view,
    cte2.num_addtocart,
    cte3.num_purchase,
    round((cte2.num_addtocart/cte1.number_product_view)*100,2) as add_to_cart_rate,
    round((cte3.num_purchase/cte1.number_product_view)*100,2) as purchase_rate
from cte1
left join cte2
    on cte1.month = cte2.month
left join cte3
    on cte1.month = cte3.month
order by cte1.month asc
