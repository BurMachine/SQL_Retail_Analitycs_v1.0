CREATE OR REPLACE FUNCTION get_diff_between_date_in_days(first_date timestamp, last_date timestamp)
RETURNS numeric
    LANGUAGE plpgsql
AS $$
DECLARE
    time_interval interval := first_date - last_date;
BEGIN
    RETURN abs(date_part('day', time_interval) + date_part('hour', time_interval) / 24 + date_part('minute', time_interval) / (24 * 60)
        + date_part('second', time_interval) / ( 24 * 60 * 60));
END
$$;

CREATE OR REPLACE FUNCTION get_offers_frequency_of_visits(t_start_date timestamp DEFAULT '2022-01-01', t_end_date timestamp DEFAULT '2022-12-31',
                                                        add_transactions_count int DEFAULT 1, max_churn_rate numeric DEFAULT 100,
                                                        max_discount_share numeric DEFAULT 100, margin_part numeric DEFAULT 50)
RETURNS table (customer_id bigint, start_date timestamp, end_date timestamp, required_transactions_count int, group_name varchar, offer_discount_depth int)
    LANGUAGE plpgsql
AS $$
DECLARE
    days_count numeric;
BEGIN
    IF t_start_date > t_end_date THEN
        RAISE EXCEPTION 'ERROR: Дата начала должна быть меньше даты окончания периода';
    END IF;

    days_count := get_diff_between_date_in_days(t_end_date, t_start_date);

    RETURN QUERY
        SELECT DISTINCT
            g.customer_id::bigint,
            t_start_date,
            t_end_date,
            round(days_count / (SELECT customer_frequency FROM customers c WHERE c.customer_id = g.customer_id))::int + add_transactions_count,
            first_value(gs.group_name) OVER w,
            (first_value(g.group_minimum_discount) OVER w * 100)::int / 5 * 5 + 5
        FROM groups g
            JOIN sku_group gs ON gs.group_id = g.group_id
                AND g.group_churn_rate <= max_churn_rate
                AND g.group_discount_share * 100 < max_discount_share
                AND (g.group_minimum_discount * 100)::int / 5 * 5 + 5
                    < (SELECT sum(s2.sku_retail_price - s2.sku_purchase_price) / sum(s2.sku_retail_price)
                        FROM product_grid s
                        JOIN store s2 ON g.group_id = s.group_id
                            AND s.sku_id = s2.sku_id) * margin_part
                WINDOW w as (PARTITION BY g.customer_id ORDER BY g.group_affinity_index DESC);
END
$$;

--TESTS
--SELECT * FROM get_offers_frequency_of_visits();
SELECT * FROM get_offers_frequency_of_visits('2022-08-18 00:00:00', '2022-08-18 00:00:00', 1,3, 70, 30);
--SELECT * FROM get_offers_frequency_of_visits('2022-08-18 00:00:00', '2022-08-18 00:00:00', 1,10, 50, 50);
