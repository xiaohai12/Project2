package cubeoperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object Main {
  def main(args: Array[String]) {

    val reducers = 10

    val inputFile= "src/test/resources/lineorder_small.tbl"
    val output = "output"

    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputFile)

    val rdd = df.rdd
        rdd.foreach(x => print(x+" "))

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")
    val aggAttribute = "lo_supplycost"


    val cb = new CubeOperator(reducers)


    val res = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")
    res.foreach(println)
    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */

    //res.saveAsTextFile(output)

    //Perform the same query using SparkSQL
        val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
          .agg(sum("lo_supplycost") as "avg supplycost")
        q1.show



  }
}