CREATE TABLE IF NOT EXISTS Personal_information (
    Customer_ID SERIAL PRIMARY KEY,
    Customer_Name VARCHAR CHECK (Customer_Name ~ '(^[A-Z]([a-z]|-|\s)*|^[А-Я]([а-я]|-|\s)*)'),
    Customer_Surname VARCHAR CHECK (Customer_Surname ~ '(^[A-Z]([a-z]|-|\s)*|^[А-Я]([а-я]|-|\s)*)'),
    Customer_Primary_Email VARCHAR CHECK (Customer_Primary_Email ~ '^[-\w\.]+@([\w]+\.)+[\w]{2,4}$'),
    Customer_Primary_Phone VARCHAR CHECK (Customer_Primary_Phone ~ '[+]?[0-9]+')
);

CREATE TABLE IF NOT EXISTS Cards (
    Customer_Card_ID SERIAL PRIMARY KEY,
    Customer_ID BIGINT NOT NULL REFERENCES Personal_information(Customer_ID)
);

CREATE TABLE IF NOT EXISTS Transactions (
    Transaction_ID SERIAL PRIMARY KEY UNIQUE,
    Customer_Card_ID BIGINT REFERENCES Cards(Customer_Card_ID),
    Transaction_Summ NUMERIC,
    Transaction_DateTime TIMESTAMP WITHOUT TIME ZONE,
    Transaction_Store_ID BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS SKU_Group (
    Group_ID SERIAL PRIMARY KEY,
    Group_Name VARCHAR CHECK (Group_Name ~ '^[A-zА-я0-9_\/-]+$')
);

CREATE TABLE IF NOT EXISTS Product_grid (
    SKU_ID SERIAL PRIMARY KEY,
    SKU_Name VARCHAR,
    Group_ID BIGINT REFERENCES SKU_Group(Group_ID)
);

CREATE TABLE IF NOT EXISTS Store (
    Transaction_Store_ID BIGINT NOT NULL,
    SKU_ID BIGINT NOT NULL REFERENCES Product_grid(SKU_ID),
    SKU_Purchase_Price NUMERIC CHECK (SKU_Purchase_Price >= 0),
    SKU_Retail_Price NUMERIC CHECK (SKU_Retail_Price >= 0)
);

CREATE TABLE IF NOT EXISTS Checks (
    Transaction_ID BIGINT NOT NULL REFERENCES Transactions(Transaction_ID),
    SKU_ID BIGINT NOT NULL REFERENCES Product_grid(SKU_ID),
    SKU_Amount NUMERIC CHECK (SKU_Amount >= 0),
    SKU_Summ NUMERIC CHECK (SKU_Summ >= 0),
    SKU_Summ_Paid NUMERIC CHECK (SKU_Summ_Paid >= 0),
    SKU_Discount NUMERIC CHECK (SKU_Discount >= 0)
);

CREATE TABLE IF NOT EXISTS Date_Of_Analysis_Formation (
    Analysis_formation TIMESTAMP WITHOUT TIME ZONE
);

CREATE OR REPLACE PROCEDURE import(table_name VARCHAR, path VARCHAR, delimiter VARCHAR DEFAULT 'E''\t''')
    LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('COPY %I FROM %L WITH DELIMITER %s', table_name, path, delimiter);
END
$$;


CREATE OR REPLACE PROCEDURE export(table_name VARCHAR, path VARCHAR, delimiter VARCHAR DEFAULT 'E''\t''')
    LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('COPY %I TO %L WITH DELIMITER %s', table_name, path, delimiter);
END
$$;

SET DATESTYLE TO "ISO, DMY";
-- SHOW DATESTYLE;

DO
$$
DECLARE
    path VARCHAR := '/mnt/c/Users/ivanb/Desktop/datasets/';
BEGIN
    CALL import('personal_information', path || 'Personal_Data.tsv');
    CALL import('sku_group', path || 'Groups_SKU.tsv');
    CALL import('product_grid', path || 'SKU.tsv');
    CALL import('cards', path || 'Cards.tsv');
    CALL import('transactions', path || 'Transactions.tsv');
    CALL import('checks', path || 'Checks.tsv');
    CALL import('store', path || 'Stores.tsv');
    CALL import('date_of_analysis_formation', path || 'Date_Of_Analysis_Formation.tsv');
END
$$;
