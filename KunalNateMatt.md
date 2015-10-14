# Capstone project - Bootcamp8 (Team America - Kunal, Nate and Matt)

## Goals:
1. Extend/Modify the data model to add Customer information to receipts.
2. Determine top 10 customers at each store (by dollar amount).
3. Search receipts by product description, customer info.
4. Detect fraud cases. (use of same card in different state within 24 hours)

## draft edits
commands below are for running in "dse spark shell"

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.SQLUserDefinedType

csc.setKeyspace("retail")

val top_customers_by_store_df = csc.sql("select customer, store_id, SUM(receipt_total) as receipts_total, credit_card_number FROM receipts_by_credit_card GROUP BY store_id, credit_card_number,customer ORDER BY store_id ASC,receipts_total DESC")
    
top_customers_by_store_df.write.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "retail", "table" -> "top_customers_by_store")).mode(SaveMode.Overwrite).save()
    
## Results/Lessons/Comments:


## Doing Fraud Detection
For fraud detection started with a couple of basic, simple rules.   If a credit card is used more than once per day, want to send a warning for someone to look into.  If the same credit card is used the same day in the same hour, send a bigger flag.

This is over simplification, what you will want to be able to do long term is to build in custome usage behavior through machine learning to figure out individual limits.  This simple rule is a starting place to show what can be done.

From the code side doing this as scala spark,  though the whole code is not done yet below are snipets used

```

import java.text.SimpleDateFormat

// Create a class of data we want to use
case class CreditCard (creditCard: Long, dateTime: Long, storeId: Int)


// Load the initial Data
val ccusage = sc.cassandraTable("retail","receipts_by_credit_card").select("credit_card_number","receipt_timestamp","store_id").as(CreditCard)


// add a column for the date, for the hour, and 1 (to add later if date is the same)
val ccDateHour = ccusage.map{case CreditCard (creditCard, dateTime, storeId) => (creditCard, dateTime, storeId, new SimpleDateFormat("MM-dd-yyyy").format(dateTime), new SimpleDateFormat("hh").format(dateTime), 1)}


// reducing by the formatted date time and credit card, sorted by the count desc
//val ccDateKey = ccusage.map{case CreditCard (creditCard, dateTime, storeId) => ((creditCard, new SimpleDateFormat("MM-dd-yyyy").format(dateTime)),1)}.reduceByKey(_+_).sortBy(- _._2)
val ccDateKey = ccusage.map{case CreditCard (creditCard, dateTime, storeId) => ((creditCard, new SimpleDateFormat("MM-dd-yyyy").format(dateTime)),1)}.reduceByKey(_+_).filter(_._2 > 1)
ccDateKey.map{ case ((x,y),z) => (x,y,z)}.saveToCassandra("retail", "credit_card_fraud_alert_by_day")

// reducing by formanted date, and hour, sorted by decending count,  red alert if lots in teh same ahoure
//val ccDateHourKey = ccusage.map{case CreditCard (creditCard, dateTime, storeId) => ((creditCard, new SimpleDateFormat("MM-dd-yyyy").format(dateTime), new SimpleDateFormat("hh").format(dateTime)),1)}.reduceByKey(_+_).sortBy(- _._2)
val ccDateHourKey = ccusage.map{case CreditCard (creditCard, dateTime, storeId) => ((creditCard, new SimpleDateFormat("MM-dd-yyyy").format(dateTime), new SimpleDateFormat("hh").format(dateTime)),1)}.reduceByKey(_+_).filter(_._2 > 1)
ccDateHourKey.map{ case ((a,b,c),d) => (a,b,d,c)}.saveToCassandra("retail", "credit_card_fraud_alert_by_day_hour")

// now need to drop anything that is just 1
val ccDateKeyFiltered = ccDateKey.filter(_._2 > 1).take(100).foreach(println)

```
