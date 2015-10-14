CREATE TABLE IF NOT EXISTS retail.top_customers_by_store (
	store_id int,
	receipts_total decimal,
	customer frozen<cust_addr>,
	PRIMARY KEY (store_id, receipts_total, customer))
	WITH CLUSTERING ORDER BY (receipts_total desc)
;
