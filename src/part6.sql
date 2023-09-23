CREATE OR REPLACE FUNCTION get_offers_cross_sales(groups_count INT DEFAULT 1, max_churn_rate NUMERIC DEFAULT 10,
                                                max_stability_index NUMERIC DEFAULT 10, max_part_sku NUMERIC DEFAULT 100,
                                                margin_part NUMERIC DEFAULT 50)
RETURNS table(customer_id BIGINT, sku_name VARCHAR, offer_discount_depth INT)
    LANGUAGE plpgsql
AS $$ 
BEGIN
    RETURN QUERY
    WITH get_customers_and_groups AS (
        SELECT
            g.customer_id,
            g.group_id,
            (g.group_minimum_discount * 100)::int / 5 * 5 + 5 AS group_minimum_discount,
            row_number() OVER w AS rank_affinity
        FROM groups g
        WHERE g.group_churn_rate <= max_churn_rate
            AND g.group_stability_index < max_stability_index
        WINDOW w AS (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC)
    ),

    add_margin_and_sku_id AS (
        SELECT
            gc.customer_id,
            gc.group_id,
            gc.group_minimum_discount,
            s.sku_retail_price - s.sku_purchase_price AS margin,
            row_number() OVER w1 AS rank_margin,
            s.sku_id,
            c.customer_primary_store
        FROM get_customers_and_groups gc
        JOIN customers c ON gc.rank_affinity <= groups_count
            AND gc.customer_id = c.customer_id
        JOIN store s ON c.customer_primary_store = s.transaction_store_id
        JOIN product_grid s2 ON gc.group_id = s2.group_id
            AND s.sku_id = s2.sku_id
        WINDOW w1 AS (PARTITION BY gc.customer_id, gc.group_id
            ORDER BY s.sku_retail_price - s.sku_purchase_price DESC)
    ),

    add_part_sku_in_groups AS (
        SELECT
            a1.customer_id,
            a1.group_id,
            a1.sku_id,
            (SELECT COUNT(DISTINCT c2.transaction_id)
            FROM purchase_history ph
                JOIN checks c2 ON ph.customer_id = a1.customer_id
                    AND ph.group_id = a1.group_id
                    AND ph.transaction_id = c2.transaction_id
                    AND c2.sku_id = a1.sku_id)::NUMERIC
            / (SELECT p.group_purchase FROM periods p
                WHERE p.customer_id = a1.customer_id
                    AND p.group_id = a1.group_id) AS part_sku_in_groups,
            a1.group_minimum_discount,
            a1.customer_primary_store
        FROM add_margin_and_sku_id a1
        WHERE rank_margin = 1
    ),

    get_allowable_discount AS (
        SELECT
            a2.customer_id,
            a2.group_id,
            a2.sku_id,
            a2.group_minimum_discount,
            (SELECT SUM(s.sku_retail_price - s.sku_purchase_price) / SUM(s.sku_retail_price) * margin_part
            FROM store s WHERE s.transaction_store_id = a2.customer_primary_store) AS allowable_discount
        FROM add_part_sku_in_groups a2
        WHERE a2.part_sku_in_groups * 100 <= max_part_sku
    )

    SELECT
        ga.customer_id::BIGINT,
        s.sku_name,
        ga.group_minimum_discount
    FROM get_allowable_discount ga
        JOIN product_grid s ON ga.sku_id = s.sku_id
    WHERE ga.group_minimum_discount <= ga.allowable_discount;
END
$$;

--TESTS
--SELECT * FROM get_offers_cross_sales();
SELECT * FROM get_offers_cross_sales(5, 3, 0.5, 100, 30);
--SELECT * FROM get_offers_cross_sales(5, 3, 0.5, 100, 50);