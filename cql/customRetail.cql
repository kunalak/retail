-- ---------------------------------------------------
-- NATE - setting up the usertype for customers and adding to existing tables
-- ---------------------------------------------------
CREATE TYPE retail.cust_addr (
        first_name text,
        last_name text,
        addr1 text,
        city text,
        state text,
        zip text
);

ALTER TABLE retail.receipts ADD customer frozen <cust_addr> static;

ALTER TABLE retail.receipts_by_store_date ADD customer frozen <cust_addr>;

ALTER TABLE retail.receipts_by_credit_card ADD customer frozen <cust_addr> static;

-- ---------------------------------------------------
-- NATE - adding for spark job to get top customers
-- --------------------------------------------------
CREATE TABLE IF NOT EXISTS retail.top_customers_by_store (
        customer frozen<cust_addr>,
        store_id int,
        receipts_total decimal,
        credit_card_number bigint,
        PRIMARY KEY (store_id, receipts_total, credit_card_number))
        WITH CLUSTERING ORDER BY (receipts_total desc)
;



-- ----------------------------------------------
-- MATWATER  - Adding some basic tables for fraud
-- ----------------------------------------------
CREATE TABLE IF NOT EXISTS retail.credit_card_fraud_alert_by_day (
    credit_card_number bigint,
    usage_date text,
    num_times_used int,
    PRIMARY KEY ((credit_card_number, usage_date), num_times_used)
) WITH CLUSTERING ORDER BY (num_times_used DESC);

CREATE TABLE IF NOT EXISTS retail.credit_card_fraud_alert_by_day_hour (
    credit_card_number bigint,
    usage_date text,
    num_times_used int,
    hour_used text,
    PRIMARY KEY ((credit_card_number, usage_date), num_times_used)
) WITH CLUSTERING ORDER BY (num_times_used DESC);


