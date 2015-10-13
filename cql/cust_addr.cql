CREATE TYPE retail.cust_addr (
        first_name text,
        last_name,
        addr1 text,
        city text,
        state text,
        zip text
);

ALTER TABLE retail.receipts ADD customer cust_addr;

ALTER TABLE retail.receipts_by_store_date ADD customer cust_addr;

ALTER TABLE retail.receipts_by_credit_card ADD customer cust_addr;

