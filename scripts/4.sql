SELECT
    year,
    customer_name,
    ROUND(SUM(total_profit),2) AS profit
FROM pipeline.pei.year_cat_sub_cat_cust_aggregate
GROUP BY 1,2
