CREATE TABLE IF NOT EXISTS retail.top_customers_by_store (
	customer frozen<cust_addr>,
	store_id int,
	receipts_total decimal,
	credit_card_number bigint,
	PRIMARY KEY (store_id, receipts_total, credit_card_number))
	WITH CLUSTERING ORDER BY (receipts_total desc)
;
