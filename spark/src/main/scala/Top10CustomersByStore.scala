import com.datastax.spark.connector._
import org.apache.spark.{SparkConf, SparkContext}


case class Cust(
                 first_name: String,
                 last_name: String,
                 zip:String
                 )

object Top10CustomersByStore {

  // Function to create Cust class for Customer UDT
  def udt2Cust(cust:UDTValue) = Cust(cust.getString("first_name"), cust.getString("last_name"), cust.getString("zip"))

  def main(args: Array[String]) {

    //  Create Spark Context
    val conf = new SparkConf(true).setAppName("Top10CustomersByStore")

    //  We set master on the command line for flexibility
    val sc = new SparkContext(conf)

    //  Compute Top 10 Customers By Store
    val storereceipts = sc.cassandraTable("retail","receipts_by_credit_card")

    val storecustomer_totals= storereceipts.map( row =>  ( (row.getInt("store_id"), row.getUDTValue("customer")), row.getDecimal("receipt_total") ) )
        .reduceByKey(_+_)   // Adding up receipt total for key=storeid+customer

        storecustomer_totals.map{ case ((store, cust ), amt) => (store, (amt,cust)) }
        .groupByKey.flatMap{ case (store, custAmtList) => custAmtList.toArray.sortBy( custamt => -custamt._1 ). // Flat map, convert to Array and sort by receipt_total
          take(10).map(ca => (store, ca))}  // Filter sorted flatmap to get 10 records for amount+customer and map to Storeid.

        .map{ case (store,(amt,cust) ) => (store, amt, cust) } //lastly map again to match the columnfamily order in top_customer_by_store table

        .saveToCassandra("retail","top_customers_by_store") // finally save to Cassandra
  }
}
