-- Индекс гарантирует, что айди будет уникальным 
CREATE TABLE IF NOT EXISTS Personal_information (
    Customer_ID SERIAL PRIMARY KEY,
    Customer_Name VARCHAR CHECK (Customer_Name ~ '(^[A-Z]([a-z]|-|\s)*|^[А-Я]([а-я]|-|\s)*)'),
    Customer_Surname VARCHAR CHECK (Customer_Surname ~ '(^[A-Z]([a-z]|-|\s)*|^[А-Я]([а-я]|-|\s)*)'),
    Customer_Primary_Email VARCHAR CHECK (Customer_Primary_Email ~ '^[-\w\.]+@([\w]+\.)+[\w]{2,4}$'),
    Customer_Primary_Phone VARCHAR CHECK (Customer_Primary_Phone ~ '^((\+7)+([0-9]){10})$')
);
DROP INDEX IF EXISTS idx_personal_information_customer_id;
CREATE UNIQUE INDEX idx_personal_information_customer_id ON Personal_information USING btree(customer_id);

-- Индекс для айди карты уникальный, а для айди клиента нет, так как один клиент может иметь несколько карт
CREATE TABLE IF NOT EXISTS cards (
    Customer_Card_ID BIGINT NOT NULL PRIMARY KEY,
    Customer_ID BIGINT NOT NULL,
    FOREIGN KEY (Customer_ID) REFERENCES Personal_information (Customer_ID)
);
DROP INDEX IF EXISTS idx_cards_customer_card_id;
DROP INDEX IF EXISTS idx_cards_customer_id;
CREATE UNIQUE INDEX IF NOT EXISTS idx_cards_customer_card_id ON Cards USING btree(Customer_Card_ID);
CREATE INDEX IF NOT EXISTS idx_cards_customer_id ON Cards USING btree(Customer_ID);

-- NUMERIC - для финансовых данных, 
CREATE TABLE IF NOT EXISTS Transactions (
    Transaction_ID SERIAL PRIMARY KEY UNIQUE,
    Customer_Card_ID BIGINT REFERENCES cards(Customer_Card_ID),
    Transaction_Summ NUMERIC(10,2),
    Transaction_DateTime TIMESTAMP WITHOUT TIME ZONE,
    Transaction_Store_ID BIGINT NOT NULL
);
DROP INDEX IF EXISTS idx_transactions_customer_card_id;
DROP INDEX IF EXISTS idx_transactions_transaction_id;
CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_transaction_id ON Transactions USING btree(Transaction_ID);
CREATE INDEX IF NOT EXISTS idx_transactions_customer_card_id ON Transactions USING btree(Customer_Card_ID);


CREATE TABLE IF NOT EXISTS SKU_Groups
(
    Group_ID BIGINT NOT NULL PRIMARY KEY,
    Group_Name VARCHAR NOT NULL CHECK (Group_Name ~ '^[A-zА-я0-9_\/\s-]+$')
);
DROP INDEX IF EXISTS idx_groups_sku_group_id;
CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_sku_group_id ON SKU_Groups USING btree(Group_ID);


CREATE TABLE IF NOT EXISTS SKU_Matrix (
    SKU_ID SERIAL NOT NULL PRIMARY KEY,
    SKU_Name VARCHAR NOT NULL,
    Group_ID BIGINT NOT NULL REFERENCES SKU_Groups(Group_ID)
);
DROP INDEX IF EXISTS  idx_sku_matrix_sku_id;
CREATE UNIQUE INDEX IF NOT EXISTS idx_sku_matrix_sku_id ON SKU_Matrix USING btree(SKU_ID);

CREATE TABLE IF NOT EXISTS Stores (
    Transaction_Store_ID BIGINT NOT NULL PRIMARY KEY,
    SKU_ID BIGINT NOT NULL REFERENCES SKU_Matrix(SKU_ID),
    SKU_Purchase_Price NUMERIC CHECK (SKU_Purchase_Price >= 0),
    SKU_Retail_Price NUMERIC CHECK (SKU_Purchase_Price >= 0)
);
DROP INDEX IF EXISTS idx_stores_sku_id;
CREATE INDEX IF NOT EXISTS idx_stores_sku_id ON Stores USING btree(SKU_ID);



CREATE TABLE IF NOT EXISTS Checks (
    Transaction_ID BIGINT NOT NULL REFERENCES Transactions(Transaction_ID),
    SKU_ID BIGINT NOT NULL REFERENCES SKU_Matrix(SKU_ID),
    SKU_Amount REAL NOT NULL,
    SKU_Summ REAL NOT NULL,
    SKU_Summ_Paid REAL NOT NULL,
    SKU_Discount REAL NOT NULL
);
DROP INDEX IF EXISTS idx_checks_sku_id;
DROP INDEX IF EXISTS idx_checks_transaction_id;
CREATE INDEX IF NOT EXISTS idx_checks_transaction_id ON Checks USING btree(Transaction_ID);
CREATE INDEX IF NOT EXISTS idx_checks_sku_id ON Checks USING btree(SKU_ID);


CREATE TABLE IF NOT EXISTS Date_Of_Analysis_Formation (
    Analysis_Formation TIMESTAMP WITHOUT TIME ZONE
);
