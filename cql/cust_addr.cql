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

