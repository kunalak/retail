# Capstone project - Bootcamp8 (Team America - Kunal, Nate and Matt)

## Goals:
1. Extend/Modify the data model to add Customer information to receipts.
2. Determine top 10 customers at each store (by dollar amount).
3. Search receipts by product description, customer info.
4. Detect fraud cases. (use of same card in different state within 24 hours)

### 1. Modify data model to include Customer information to receipts.
<<Matt edits here>> <<data was loaded using Jmeter with random data generator library>>

### 2. Determine top 10 customers at each store (by dollar amount spent)
commands below were executed in DSE SPARK shell.

* import org.apache.spark.sql.SaveMode
* import org.apache.spark.sql.types.SQLUserDefinedType
* csc.setKeyspace("retail")

val top_customers_by_store_df = csc.sql("select customer, store_id, SUM(receipt_total) as receipts_total, credit_card_number FROM receipts_by_credit_card GROUP BY store_id, credit_card_number,customer ORDER BY store_id ASC,receipts_total DESC")
    
top_customers_by_store_df.write.format("org.apache.spark.sql.cassandra").options(Map("keyspace" -> "retail", "table" -> "top_customers_by_store")).mode(SaveMode.Overwrite).save()
    
    
## 3. Search receipts by product
### Create a solr core

First had the system create it by running
```
dsetool create_core retail.receipts generateResources=true reindex=true deleteAll=true 
```
then looking at the core create via http://localhost:8983/solr/#/~cores and pull down the schema and tweak.
Main tweak was making sure the product_id field was not split like a text field, but instead was a string. This means the schema ended up being.
```
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<schema name="autoSolrSchema" version="1.5">
	<types>
		<fieldType class="org.apache.solr.schema.TrieLongField"
			name="TrieLongField" />
		<fieldType class="org.apache.solr.schema.TextField" name="NonParsed" />
		<fieldType class="org.apache.solr.schema.TextField" name="TextField">
			<analyzer>
				<tokenizer class="solr.StandardTokenizerFactory" />
				<filter class="solr.LowerCaseFilterFactory" />
			</analyzer>
		</fieldType>
		<fieldType class="com.datastax.bdp.search.solr.core.types.DecimalStrField"
			name="DecimalStrField" />
		<fieldType class="org.apache.solr.schema.UUIDField" name="UUIDField" />
		<fieldType class="org.apache.solr.schema.TrieIntField" name="TrieIntField" />
	</types>
	<fields>
		<field indexed="true" multiValued="false" name="receipt_id"
			stored="true" type="TrieLongField" />
		<field indexed="true" multiValued="false" name="product_name"
			stored="true" type="TextField" />
		<field indexed="true" multiValued="false" name="product_id"
			stored="true" type="NonParsed" />
		<field indexed="true" multiValued="false" name="unit_price"
			stored="true" type="DecimalStrField" />
		<field indexed="true" multiValued="false" name="scan_id" stored="true"
			type="UUIDField" />
		<field indexed="true" multiValued="false" name="total" stored="true"
			type="DecimalStrField" />
		<field indexed="true" multiValued="false" name="quantity"
			stored="true" type="TrieIntField" />
	</fields>
	<defaultSearchField>product_name</defaultSearchField>
	<uniqueKey>(receipt_id,scan_id)</uniqueKey>
</schema>
```
With the file name being saved as receipts.xml

We then dropped the core so it can be recreated with our tweaks 
```
dsetool unload_core retail.receipts
```
then recreated
```
dsetool create_core retail.receipts schema=receipts.xml solrconfig=solrconfig.xml reindex=true
```
### create web pages
Created using the python builds and templates.  Added link to index page.  The page allows you to do a solr search on product name against the receipts table.  Data is presented 
```
 Receipt ID	 Product ID	 Product Name	 Store ID	 Register 	 Price 	 Quantity
```

With Receipt ID taking you to the recipet details page, and product id taking you to the product details page.  Facet search on 'Receipt ID' and 'Product ID'.  Becuase of the product id faceting is why we made sure the field was not parsed.


## Results/Lessons/Comments:


## Doing Fraud Detection
For fraud detection started with a couple of basic, simple rules.   If a credit card is used more than once per day, want to send a warning for someone to look into.  If the same credit card is used the same day in the same hour, send a bigger flag.

This is over simplification, what you will want to be able to do long term is to build in custome usage behavior through machine learning to figure out individual limits.  This simple rule is a starting place to show what can be done.

The code to do this is created in a singlce file, FraudDetection.scala.  See code below

```
import com.datastax.spark.connector._
import org.apache.spark._
import java.text.SimpleDateFormat

case class CreditCard(
  creditCard: Long,
  dateTime: Long,
  storeId: Int)

object FraudDetection {
  def main(args: Array[String]) {

    // Create Spark Context
    val conf = new SparkConf(true).setAppName("FraudDetection")

    // We set master on the command line for flexibility
    val sc = new SparkContext(conf)

    // Load the initial Data
    val ccusage = sc.cassandraTable("retail", "receipts_by_credit_card").select("credit_card_number", "receipt_timestamp", "store_id").as(CreditCard)

    // add a column for the date, for the hour, and 1 (to add later if date is the same)
    val ccDateHour = ccusage.map { case CreditCard(creditCard, dateTime, storeId) => (creditCard, dateTime, storeId, new SimpleDateFormat("MM-dd-yyyy").format(dateTime), new SimpleDateFormat("hh").format(dateTime), 1) }

    // reducing by the formatted date time and credit card, sorted by the count desc
    //val ccDateKey2 = ccusage.map{case CreditCard (creditCard, dateTime, storeId) => ((creditCard, new SimpleDateFormat("MM-dd-yyyy").format(dateTime)),1)}.reduceByKey(_+_).sortBy(- _._2)
    val ccDateKey = ccusage.map { case CreditCard(creditCard, dateTime, storeId) => ((creditCard, new SimpleDateFormat("MM-dd-yyyy").format(dateTime)), 1) }.reduceByKey(_ + _).sortBy(-_._2).filter(_._2 > 1)
    ccDateKey.map { case ((x, y), z) => (x, y, z) }.saveToCassandra("retail", "credit_card_fraud_alert_by_day")

    // reducing by formanted date, and hour, sorted by decending count,  red alert if lots in teh same ahoure
    //val ccDateHourKey2 = ccusage.map{case CreditCard (creditCard, dateTime, storeId) => ((creditCard, new SimpleDateFormat("MM-dd-yyyy").format(dateTime), new SimpleDateFormat("hh").format(dateTime)),1)}.reduceByKey(_+_).sortBy(- _._2)
    val ccDateHourKey = ccusage.map { case CreditCard(creditCard, dateTime, storeId) => ((creditCard, new SimpleDateFormat("MM-dd-yyyy").format(dateTime), new SimpleDateFormat("hh").format(dateTime)), 1) }.sortBy(-_._2).reduceByKey(_ + _).filter(_._2 > 1)
    ccDateHourKey.map { case ((a, b, c), d) => (a, b, d, c) }.saveToCassandra("retail", "credit_card_fraud_alert_by_day_hour")
  }
}
```


to execute the job, after you build the project using sbt, just do a 
```
dse spark-submit --class FraudDetection spark-fraud-assembly-1.1.jar
```


