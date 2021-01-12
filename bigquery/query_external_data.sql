SELECT  
                a.date,
                a.country,
                a.city,
                approx_count_distinct(a.productSKU) as count_of_products
FROM `gcp-big-data-292014.ecommerce.cls_external_data` a   --External Table
group by a.country,a.city,a.date


SELECT  
        a.date,
        a.country,
        a.city,
        approx_count_distinct(a.productSKU) count_of_products
FROM `gcp-big-data-292014.ecommerce.all_sessions_raw` a  --Native table
group by a.country,a.city,a.date
