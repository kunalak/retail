# Capstone project - Bootcamp8 (Team America - Kunal, Nate and Matt)

## Goals:
1. Extend/Modify the data model to add Customer information to receipts.
2. Determine top 10 customers at each store (by dollar amount).
3. Search receipts by product description, customer info.
4. Detect fraud cases. (use of same card in different state within 24 hours)

## draft edits
val top_customers_by_store_df = hc.sql("select customer, store_id, SUM(receipt_total) as receipts_total, credit_card_number FROM receipts_by_credit_card GROUP BY store_id, credit_card_number,customer ORDER BY store_id ASC,receipts_total DESC")
    top_customers_by_store_df.write.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "retail", "table" -> "top_customers_by_store")).mode(SaveMode.Overwrite).save()
    
## Results/Lessons/Comments:
