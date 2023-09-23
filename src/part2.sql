CREATE MATERIALIZED VIEW IF NOT EXISTS Customers AS (
    WITH  Id_Average_Check AS(
        SELECT pi.customer_id AS customer_id,
        (SUM(transaction_summ) / COUNT(transaction_summ))::NUMERIC AS "customer_average_check"
        FROM personal_information pi
        JOIN cards c ON pi.customer_id = c.customer_id
        JOIN transactions t ON c.customer_card_id = t.customer_card_id
        GROUP BY pi.customer_id
        ORDER BY customer_average_check DESC
    ),

    Frequency AS (
         SELECT pi.customer_id AS customer_id,
        (MAX(date(transaction_datetime ))-MIN(date(transaction_datetime )))/COUNT(transaction_id)::NUMERIC AS customer_frequency
        FROM personal_information pi
            JOIN cards c ON pi.customer_id = c.customer_id
            JOIN transactions t ON c.customer_card_id = t.customer_card_id
        GROUP BY pi.customer_id
        ORDER BY customer_frequency ASC
    ),

    Inactive_Period AS (
        SELECT pi.customer_id,
        (extract(epoch from (SELECT * FROM date_of_analysis_formation))-extract(epoch from MAX(transaction_datetime)))/86400 AS customer_inactive_period
        FROM personal_information pi
            JOIN cards c ON pi.customer_id = c.customer_id
            JOIN transactions t ON c.customer_card_id = t.customer_card_id
        GROUP BY pi.customer_id
        ORDER BY customer_inactive_period ASC
    ),

    Churn_Rate AS (
        SELECT IP.customer_id, customer_inactive_period/customer_frequency::NUMERIC AS customer_churn_rate
        FROM Inactive_Period IP
            JOIN Frequency F ON IP.customer_id = F.customer_id
        ORDER BY customer_churn_rate ASC
    ),

    Percentile_Calculation_Check AS (
        SELECT customer_id, customer_average_check,
        CUME_DIST() OVER (ORDER BY customer_average_check DESC) AS percent_check_rank
        FROM Id_Average_Check
    ),

    Percentile_Calculation_Frequency AS (
        SELECT customer_id, customer_frequency,
        CUME_DIST() OVER (ORDER BY customer_frequency ASC) AS percent_frequency_rank
        FROM Frequency
    ),

    Percentile_Calculation_Churn AS (
        SELECT customer_id, customer_frequency,
        CUME_DIST() OVER (ORDER BY customer_frequency ASC) AS percent_frequency_rank
        FROM Frequency
    ),

    Churn_Rate_Value AS (
        SELECT customer_id, customer_churn_rate
        FROM Churn_Rate
    ),

    Check_Segment AS (
        SELECT customer_id, customer_average_check,
           CASE
               WHEN percent_check_rank <= 0.1 THEN 'High'
               WHEN percent_check_rank > 0.1 AND percent_check_rank <= 0.35 THEN 'Medium'
               ELSE 'Low'
           END AS customer_average_check_segment
        FROM Percentile_Calculation_Check
    ),

    Frequency_Segment AS (
        SELECT customer_id, customer_frequency,
           CASE
               WHEN percent_frequency_rank <= 0.1 THEN 'Often'
               WHEN percent_frequency_rank > 0.1 AND percent_frequency_rank <= 0.35 THEN 'Occasionally'
               ELSE 'Rarely'
           END AS customer_frequency_segment
        FROM Percentile_Calculation_Frequency
    ),

    Churn_Segment AS (
        SELECT customer_id,
           CASE
               WHEN customer_churn_rate <= 2 THEN 'Low'
               WHEN customer_churn_rate > 2 AND customer_churn_rate <= 5 THEN 'Medium'
               ELSE 'High'
           END AS customer_churn_segment
        FROM Churn_Rate_Value
    ),

   Segment_Number AS (
        SELECT C.customer_id,
            CASE customer_average_check_segment
                WHEN 'Low' THEN 0
                WHEN 'Medium' THEN 9
                ELSE 18 END +
            CASE customer_frequency_segment
                WHEN 'Rarely' THEN 0
                WHEN 'Occasionally' THEN 3
                ELSE 6 END +
            CASE customer_churn_segment
                WHEN 'Low' THEN 1
                WHEN 'Medium' THEN 2
                ELSE 3
            END AS customer_segment
        FROM Check_Segment C
            JOIN Frequency_Segment F ON C.customer_id = F.customer_id
            JOIN Churn_Segment CS ON CS.customer_id = F.customer_id
    ),

    All_Store_And_Share AS (
        SELECT pi.customer_id, transaction_store_id, COUNT(transaction_store_id) AS share_1
        FROM personal_information pi
            JOIN cards c ON pi.customer_id = c.customer_id
            JOIN transactions t ON c.customer_card_id = t.customer_card_id
        GROUP BY pi.customer_id,transaction_store_id
        ORDER BY customer_id
    ),

    Count_Share2 AS (
        SELECT pi.customer_id, c1.transaction_store_id, share_1, COUNT(t.transaction_store_id) AS share_2
        FROM personal_information pi
            JOIN cards c ON pi.customer_id = c.customer_id
            JOIN transactions t ON c.customer_card_id = t.customer_card_id
            JOIN All_Store_And_Share c1 ON c1.customer_id = pi.customer_id
        GROUP BY pi.customer_id, c1.transaction_store_id, c1.share_1
        ORDER BY customer_id
    ),

    Calculation_in_Share AS(
     SELECT ASAS.customer_id, asas.transaction_store_id, asas.share_1, share_2, asas.share_1/share_2::NUMERIC AS calculation_share
        FROM Count_Share2 CS
            JOIN All_Store_And_Share ASAS ON  CS.customer_id = ASAS.customer_id
        GROUP BY ASAS.customer_id,asas.transaction_store_id, asas.share_1, share_2
        ORDER BY customer_id
    ),

    All_Transaction AS (
        SELECT
            Customer_ID,
            transaction_id,
            Transaction_Store_ID,
            ROW_NUMBER() OVER (PARTITION BY Customer_ID ORDER BY Transaction_DateTime DESC) AS store_number, transaction_datetime
        FROM transactions t
            JOIN cards C2 ON C2.customer_card_id = t.customer_card_id
    ),

    Three_Last_Transaction AS (
        SELECT Customer_ID,
            CASE
                WHEN store_number = 1 THEN transaction_store_id
            END AS store1,
            CASE
                WHEN store_number = 2 THEN transaction_store_id
            END AS store2,
            CASE
                WHEN store_number = 3 THEN transaction_store_id
            END AS store3
        FROM All_Transaction
        WHERE store_number <= 3
            GROUP BY Customer_ID, transaction_store_id, store_number
            ORDER BY customer_id
    ),

    Store1 AS (
        SELECT t.customer_id, store1, calculation_share AS calc_share1
        FROM Three_Last_Transaction t
            JOIN Calculation_in_Share c ON c.customer_id = t.customer_id AND transaction_store_id = store1
        WHERE store1 IS NOT NULL
            GROUP BY t.customer_id , store1, calculation_share
    ),

    Store2 AS (
        SELECT t.customer_id, store2, calculation_share AS calc_share2
          FROM Three_Last_Transaction t
         JOIN Calculation_in_Share c ON c.customer_id = t.customer_id AND transaction_store_id = store2
         WHERE store2 IS NOT NULL
         GROUP BY t.customer_id ,  store2, calculation_share
    ),

    Store3 AS (
        SELECT t.customer_id,  store3, calculation_share AS calc_share3
        FROM Three_Last_Transaction t
            JOIN Calculation_in_Share c ON c.customer_id = t.customer_id AND transaction_store_id = store3
        WHERE store3 IS NOT NULL
            GROUP BY t.customer_id ,  store3, calculation_share
    ),

    AllStore AS (
        SELECT s1.customer_id, s1.store1, calc_share1, s2.store2, calc_share2, s3.store3, calc_share3
        FROM Store1 s1
            JOIN Store2 s2 ON s1.customer_id = s2.customer_id
            JOIN Store3 s3 ON s2.customer_id = s3.customer_id
        GROUP BY s1.customer_id , s1.store1, s2.store2, s3.store3, calc_share1, calc_share2, calc_share3
    ),

    Max_Calc_Share AS (
        SELECT customer_id,
            CASE
                WHEN calc_share1 >= calc_share2 AND calc_share1 >= calc_share3 THEN store1
                WHEN calc_share2 >= calc_share1 AND calc_share2 >= calc_share3 THEN store2
                ELSE store3
            END AS primary_store,
            CASE
                WHEN calc_share1 >= calc_share2 AND calc_share1 >= calc_share3 THEN calc_share1
                WHEN calc_share2 >= calc_share1 AND calc_share2 >= calc_share3 THEN calc_share2
                ELSE calc_share3
            END AS max_calc_share
        FROM AllStore
    ),

    Last_datetime AS (
        SELECT customer_id, transaction_store_id AS primary_store2, transaction_datetime
        FROM All_Transaction a
        WHERE store_number = 1
    ),

    Primary_Store AS (
        SELECT a.customer_id,
            CASE
                WHEN store1 = store2 AND store2 = store3 THEN store1
                WHEN calc_share1 = calc_share2 AND calc_share2 = calc_share3 THEN m.primary_store
                ELSE l.primary_store2
            END AS Customer_Primary_Store
        FROM AllStore a
            JOIN Max_Calc_Share m ON m.customer_id = a.customer_id
            JOIN Last_datetime l ON a.customer_id = l.customer_id
        GROUP BY a.customer_id, store1, store2, store3, primary_store, primary_store2, calc_share3, calc_share2, calc_share1
    )

    SELECT iac.customer_id, iac.customer_average_check, cs.customer_average_check_segment, f.customer_frequency,
       fs.customer_frequency_segment, ip.customer_inactive_period, customer_churn_rate, customer_churn_segment, customer_segment, customer_primary_store
    FROM Id_Average_Check iac
        JOIN Check_Segment cs ON iac.customer_id = cs.customer_id
        JOIN Frequency f ON iac.customer_id = f.customer_id
        JOIN Frequency_Segment fs ON iac.customer_id = fs.customer_id
        JOIN Inactive_Period ip ON iac.customer_id = ip.customer_id
        JOIN Churn_Rate cr ON iac.customer_id = cr.customer_id
        JOIN Churn_Segment css ON iac.customer_id = css.customer_id
        JOIN Segment_Number sn ON iac.customer_id = sn.customer_id
        JOIN Primary_Store ps ON iac.customer_id = ps.customer_id
    ORDER BY iac.customer_id
);


CREATE MATERIALIZED VIEW IF NOT EXISTS Purchase_History AS (
    WITH Group_id AS (
            SELECT P.customer_id, T.transaction_id, T.transaction_datetime, group_id,
                SUM(sku_amount * sku_purchase_price)::NUMERIC AS Group_Cost,
                SUM(sku_summ)::NUMERIC AS Group_summ,
                SUM(sku_summ_paid)::NUMERIC AS Group_Summ_Paid
            FROM personal_information P
                JOIN cards C ON P.customer_id = C.customer_id
                JOIN transactions T ON C.customer_card_id = T.customer_card_id
                JOIN checks CH ON CH.transaction_id = T.transaction_id
                JOIN product_grid PG ON CH.sku_id = PG.sku_id
                JOIN store S ON PG.sku_id = S.sku_id AND S.transaction_store_id = T.transaction_store_id
            GROUP BY P.customer_id, T.transaction_id, T.transaction_datetime, group_id, sku_discount
            ORDER BY P.customer_id, group_id
    )

    SELECT  * FROM Group_id g
    ORDER BY customer_id, group_id
);


CREATE MATERIALIZED VIEW IF NOT EXISTS Periods AS (
    WITH Period AS (
        SELECT  P.customer_id, group_id, t.transaction_id, ((sku_discount/sku_summ)) AS Group_Min_Discount
        FROM personal_information P
            JOIN cards C ON P.customer_id = C.customer_id
            JOIN transactions T ON C.customer_card_id = T.customer_card_id
            JOIN checks CH ON CH.transaction_id = T.transaction_id
            JOIN product_grid PG ON CH.sku_id = PG.sku_id
        GROUP BY P.customer_id, PG.group_id, t.transaction_id, sku_discount/sku_summ
        ORDER BY P.customer_id
    ),

    Date_First_Last_Purchase AS (
        SELECT Ph.customer_id, ph.group_id, MIN((transaction_datetime)) AS First_Group_Purchase_Date, MAX((transaction_datetime)) AS Last_Group_Purchase_Date,
                COUNT(ph.transaction_id) AS Group_Purchase
        FROM Purchase_History PH
        GROUP BY Ph.customer_id, ph.group_id
        ORDER BY Ph.customer_id, group_id
    ),

    Frequency_Purchase AS (
        SELECT d.customer_id, d.group_id, ((extract(epoch from Last_Group_Purchase_Date -  First_Group_Purchase_Date)/86400 + 1) / Group_Purchase)::NUMERIC AS Group_Frequency
        FROM Date_First_Last_Purchase d
    )

    SELECT  D.Customer_ID, D.Group_ID, First_Group_Purchase_Date, Last_Group_Purchase_Date, Group_Purchase, Group_Frequency,
        CASE
            WHEN MAX(group_min_discount) = 0 THEN 0
            ELSE (MIN(Group_Min_Discount) FILTER ( WHERE group_min_discount > 0 ))
        END AS Group_Min_Discount
    FROM Period P
        JOIN Date_First_Last_Purchase D ON D.customer_id = P.customer_id AND p.group_id = d.group_id
        JOIN Frequency_Purchase F ON F.customer_id = D.customer_id AND f.group_id = p.group_id
    GROUP BY D.group_id, d.customer_id, First_Group_Purchase_Date, Last_Group_Purchase_Date, Group_Purchase, Group_Frequency
    ORDER BY D.customer_id, D.group_id
);


CREATE MATERIALIZED VIEW IF NOT EXISTS Groups AS (
    WITH Affenity_Index AS (
        SELECT PH.customer_id, P.group_id, group_purchase/COUNT(ph.transaction_id)::NUMERIC AS group_affinity_index
        FROM Purchase_History PH
            JOIN Periods P ON P.customer_id = PH.customer_id
        WHERE ph.transaction_datetime BETWEEN first_group_purchase_date AND last_group_purchase_date
            GROUP BY PH.customer_id, P.group_id, group_purchase
            ORDER BY customer_id
    ),

    Churn_Rate AS (
        SELECT  ph.customer_id, ph.group_id,
            (extract(epoch FROM(SELECT * FROM date_of_analysis_formation)) - extract(epoch FROM MAX((ph.transaction_datetime))))/(group_frequency)/86400::NUMERIC AS Group_Churn_Rate
        FROM transactions T
            JOIN purchase_history ph ON T.transaction_id = ph.transaction_id
            JOIN periods p ON ph.group_id = p.group_id AND p.customer_id = ph.customer_id
        GROUP BY ph.customer_id, ph.group_id, group_frequency
        ORDER BY customer_id, group_id
    ),

    Intervals AS (
        SELECT ph.customer_id, ph.transaction_id,  ph.group_id, ph.transaction_datetime,
            EXTRACT(DAY FROM (transaction_datetime - LAG(transaction_datetime)
                OVER (PARTITION BY ph.customer_id, ph.group_id ORDER BY transaction_datetime))) AS interval
        FROM purchase_history ph
            JOIN periods p ON p.customer_id = ph.customer_id AND p.group_id = ph.group_id
        GROUP BY ph.customer_id, transaction_id, ph.group_id, transaction_datetime
        ORDER BY customer_id, group_id
    ),

    Stability_Index AS (
        SELECT i.customer_id, i.group_id, AVG(
            CASE
                WHEN (i.interval - p.group_frequency) > 0::NUMERIC THEN (i.interval - p.group_frequency)
                ELSE (i.interval - p.group_frequency) * '-1'::INTEGER::NUMERIC
            END / p.group_frequency) AS group_stability_index
        FROM Intervals i
            JOIN periods p ON p.customer_id = i.customer_id AND i.group_id = p.group_id
        GROUP BY i.customer_id, i.group_id
        ORDER BY customer_id, group_id
    ),

    Margin AS (
        SELECT customer_id, group_id, SUM(group_summ_paid - group_cost)::NUMERIC AS Group_Margin
        FROM purchase_history
        GROUP BY customer_id, group_id
        ORDER BY customer_id, group_id
    ),

    Count_Discount_Share AS (
        SELECT DISTINCT p.customer_id, g.group_id,
            CASE
                WHEN MAX(sku_discount) = 0 THEN COUNT(c3.transaction_id)
                ELSE COUNT(c3.transaction_id)  FILTER ( WHERE sku_discount> 0 )
            END AS count_share
        FROM personal_information P
            JOIN cards C2 ON p.customer_id = C2.customer_id
            JOIN transactions T2 ON C2.customer_card_id = T2.customer_card_id
            JOIN checks C3 ON T2.transaction_id = C3.transaction_id
            JOIN product_grid G ON G.sku_id = C3.sku_id
        GROUP BY p.customer_id, g.group_id
        ORDER BY customer_id
    ),

    Discount_Share AS (
        SELECT DISTINCT c.customer_id, c.group_id, count_share/group_purchase::NUMERIC AS Group_Discount_Share
        FROM Count_Discount_Share c
            JOIN periods p ON c.group_id = p.group_id AND p.customer_id = c.customer_id
        GROUP BY c.customer_id, c.group_id, Group_Discount_Share
    ),

    Minimum_Discount AS (
        SELECT customer_id, group_id, MIN(group_min_discount) AS Group_Minimum_Discount
        FROM periods p
        GROUP BY customer_id, group_id
        ORDER BY customer_id, group_id
    ),

    Group_Average_Discount AS (
        SELECT  customer_id, group_id, AVG(group_summ_paid/group_summ)::NUMERIC AS Group_Average_Discount
        FROM purchase_history
            JOIN checks C4 ON purchase_history.transaction_id = C4.transaction_id
        WHERE sku_discount > 0
            GROUP BY customer_id, group_id
            ORDER BY customer_id, group_id
    )

    SELECT DISTINCT af.customer_id, af.group_id, group_affinity_index, Group_Churn_Rate,
                    COALESCE(Group_Stability_Index, 0) AS Group_Stability_Index, Group_Margin,
           Group_Discount_Share, Group_Minimum_Discount, Group_Average_Discount
    FROM Affenity_Index af
        JOIN Churn_Rate cr ON af.group_id = cr.group_id AND af.customer_id = cr.customer_id
        JOIN Stability_Index si ON si.group_id = cr.group_id AND si.customer_id = af.customer_id
        JOIN Margin gm ON gm.customer_id = af.customer_id AND gm.group_id = af.group_id
        JOIN Discount_Share ds ON ds.group_id = cr.group_id AND ds.customer_id = cr.customer_id
        JOIN Minimum_Discount md ON md.group_id = af.group_id AND md.customer_id = af.customer_id
        JOIN Group_Average_Discount gad ON gad.group_id = md.group_id AND gad.customer_id = ds.customer_id
    GROUP BY af.customer_id, af.group_id, group_affinity_index, Group_Churn_Rate, Group_Discount_Share, Group_Minimum_Discount, Group_Average_Discount, Group_Stability_Index, Group_Margin
    ORDER BY af.customer_id, af.group_id
);
