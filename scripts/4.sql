SELECT
    year
    customer_name,
    SUM(total_profit) AS profit
FROM pipline.pei.year_cat_sub_cat_cust_aggregate
GROUP BY 1,2
;
