CREATE OR REPLACE FUNCTION fn_get_key_date(key INTEGER) RETURNS SETOF DATE
    LANGUAGE plpgsql AS
$$
BEGIN
    IF (key = 1) THEN
        RETURN QUERY
        SELECT transaction_datetime::DATE
        FROM "Transactions"
        ORDER BY 1 LIMIT 1;
    ELSEIF (key = 2) THEN
        RETURN QUERY
        SELECT transaction_datetime::DATE
        FROM "Transactions"
        ORDER BY 1 DESC LIMIT 1;
    END IF;
END
$$;

DROP FUNCTION IF EXISTS fn_method_first(DATE, DATE, NUMERIC);
CREATE OR REPLACE FUNCTION fn_method_first(first_date DATE, last_date DATE, coefficient_of_average_check_increase NUMERIC)
RETURNS TABLE (customer_id BIGINT, required_check_measure NUMERIC)
    LANGUAGE plpgsql
AS $$
BEGIN
    IF (first_date < fn_get_key_date(1)) THEN
        first_date = fn_get_key_date(1);
    ELSEIF (last_date > fn_get_key_date(2)) THEN
        last_date = fn_get_key_date(2);
    ELSEIF (first_date >= last_date) THEN
        RAISE EXCEPTION
            'last date of the specified period must be later than the first one';
    END IF;
    RETURN QUERY
        WITH pre_query AS (
            SELECT cards.customer_id AS Customer_ID, (t.transaction_summ) AS trans_summ
            FROM cards
                JOIN transactions t ON cards.customer_card_id = t.customer_card_id
            WHERE t.transaction_datetime BETWEEN first_date AND last_date)
        SELECT DISTINCT pq.Customer_ID, (avg(trans_summ) OVER (PARTITION BY pq.Customer_ID))::NUMERIC * coefficient_of_average_check_increase AS Avg_check
        FROM pre_query pq;
END
$$;

DROP FUNCTION IF EXISTS fn_method_second(BIGINT, NUMERIC);
CREATE OR REPLACE FUNCTION fn_method_second(number_of_transactions BIGINT, coefficient_of_average_check_increase NUMERIC)
RETURNS TABLE (customer_id BIGINT, required_check_measure NUMERIC)
    LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    WITH pre_query AS (
        SELECT c.customer_id, t.customer_card_id, t.transaction_summ, rank() OVER (PARTITION BY c.customer_id ORDER BY transaction_datetime DESC) AS rank
        FROM transactions t
            JOIN cards c ON c.customer_card_id = t.customer_card_id),
        CTE1 AS (
        SELECT pre_query.customer_id, customer_card_id, transaction_summ
        FROM pre_query
        WHERE rank <= number_of_transactions)
    SELECT DISTINCT CTE1.customer_id, (avg(transaction_summ) OVER (PARTITION BY CTE1.customer_id))::NUMERIC * coefficient_of_average_check_increase AS Avg_check
    FROM CTE1;
END
$$;

DROP FUNCTION IF EXISTS fn_reward_group(customer_churn_rate NUMERIC, discount_share NUMERIC, margin NUMERIC);
CREATE OR REPLACE FUNCTION fn_reward_group(customer_churn_rate NUMERIC, discount_share NUMERIC, margin NUMERIC)
RETURNS TABLE(customer_id INTEGER, group_id BIGINT, Offer_Discount_Depth NUMERIC)
    LANGUAGE plpgsql
AS $$
DECLARE
    row RECORD;
    flag BOOL := FALSE;
    person_id INTEGER := 0;
    average_m NUMERIC;
    temp NUMERIC;
BEGIN
    FOR row IN (SELECT groups.customer_id, groups.group_id, group_affinity_index,
                group_churn_rate, group_discount_share, group_minimum_discount,
                dense_rank() OVER (PARTITION BY groups.customer_id ORDER BY group_affinity_index DESC)
                FROM groups
                WHERE group_churn_rate <= customer_churn_rate AND group_discount_share < (discount_share / 100.)
                ORDER BY customer_id, group_minimum_discount)

    LOOP
    average_m = (SELECT AVG(group_summ_paid - group_cost)
                 FROM purchase_history ph
                 WHERE ph.customer_id = row.customer_id
                 AND ph.group_id = row.group_id);
    temp = (FLOOR ((row.group_minimum_discount * 100) / 5.0) * 5)::numeric(10, 2);
    IF (person_id != row.customer_id) THEN
        IF (average_m > 0
            AND row.group_minimum_discount::numeric(10, 2) > 0
            AND average_m * margin / 100. > temp * average_m / 100.) THEN
                IF (temp = 0) THEN
                temp = 5;
                END IF;
            RETURN QUERY (SELECT g.customer_id, g.group_id,
                          temp AS Offer_Discount_Depth
                          FROM groups g
                          WHERE row.customer_id = g.customer_id AND
                                row.group_id = g.group_id);
            person_id = row.customer_id;
        END IF;
    END IF;
    END LOOP;
END
$$;

DROP FUNCTION IF EXISTS fn_part4(calculation_method_enum, DATE, DATE, BIGINT, NUMERIC, NUMERIC, NUMERIC, NUMERIC);
DROP TYPE IF EXISTS calculation_method_enum;
CREATE TYPE calculation_method_enum AS ENUM ('1', '2');
CREATE OR REPLACE FUNCTION fn_part4(calculation_method calculation_method_enum, first_date DATE, last_date DATE, number_of_transactions BIGINT,
                                    coefficient_of_average_check_increase NUMERIC, maximum_churn_index NUMERIC, maximum_share_of_transactions_with_a_discount NUMERIC,
                                    allowable_share_of_margin NUMERIC)
RETURNS TABLE(Customer_ID BIGINT, Required_Check_Measure NUMERIC, Group_Name VARCHAR, Offer_Discount_Depth NUMERIC)
    LANGUAGE plpgsql
AS $$
BEGIN
    IF (calculation_method = '1') THEN
        RETURN QUERY
            SELECT ch.customer_id, ch.required_check_measure, gs.group_name, rd.Offer_Discount_Depth
            FROM fn_method_first(first_date,
                                 last_date,
              coefficient_of_average_check_increase) AS ch
            JOIN fn_reward_group(maximum_churn_index,
                                          maximum_share_of_transactions_with_a_discount,
                                          allowable_share_of_margin) rd ON
                ch.customer_id = rd.customer_id
            JOIN sku_group gs ON gs.group_id = rd.customer_id
            ORDER BY customer_id;
    ELSEIF (calculation_method = '2') THEN
        RETURN QUERY
            SELECT ch.customer_id, ch.required_check_measure, gs.group_name, rd.Offer_Discount_Depth
            FROM fn_method_second(number_of_transactions,
                         coefficient_of_average_check_increase) AS ch
            JOIN fn_reward_group(maximum_churn_index,
                                     maximum_share_of_transactions_with_a_discount,
                                          allowable_share_of_margin) rd ON
                ch.customer_id = rd.customer_id
            JOIN sku_group gs ON gs.group_id = rd.group_id
            ORDER BY Offer_Discount_Depth, Required_Check_Measure;
    END IF;
END
$$;

--TESTS
SELECT * FROM fn_part4('2', null, null,  100, 1.15, 3, 70, 30);
