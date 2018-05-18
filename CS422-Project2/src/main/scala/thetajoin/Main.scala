package thetajoin

import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import org.dmg.pmml.ChildParent.Recursive

object Main {
  def main(args: Array[String]) {
    val t1 = System.nanoTime
    val reducers = 25
    val maxInput = 2000
    val inputFile1="src/test/resources/input1_2K.csv"
    val inputFile2="src/test/resources/input2_2K.csv"
    
    val output = "output"
    
    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)   
    
    val df1 = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(inputFile1)
    
    val df2 = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("delimiter", ",")
    .load(inputFile2)
    
    val rdd1 = df1.rdd
    val rdd2 = df2.rdd
    
    val schema1 = df1.schema.toList.map(x => x.name)
    val schema2 = df2.schema.toList.map(x => x.name)
    
    val dataset1 = new Dataset(rdd1, schema1)
    val dataset2 = new Dataset(rdd2, schema2)

    //test ....





    val tj = new ThetaJoin(dataset1.getRDD().count, dataset2.getRDD.count, reducers, maxInput)
    val res = tj.theta_join(dataset1, dataset2, "num", "num", ">")
    res.foreach(println)
    //val a = dataset1.getRDD().cartesian(dataset2.getRDD()).filter{case (a,b)=>a.getInt(1)>b.getInt(1)}.count()
    //val b = res.count()
    println(res.count)
    val duration = (System.nanoTime - t1) / 1e9d
    print(duration)
//    res.saveAsTextFile(output)
  }     
}
