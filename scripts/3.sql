SELECT
    customer_name,
    SUM(total_profit) AS profit
FROM pipeline.pei.year_cat_sub_cat_cust_aggregate
GROUP BY 1
;
