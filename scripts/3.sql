SELECT
    customer_name,
    SUM(total_profit) AS profit
FROM PEI.PEI_SCHEMA.year_cat_sub_cat_cust_aggregate
GROUP BY 1
;
