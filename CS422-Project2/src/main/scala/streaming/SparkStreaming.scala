package streaming;

import scala.io.Source
import scala.util.control._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.streaming._
import scala.collection.Seq
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.Strategy
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;

import org.apache.spark.util.sketch.CountMinSketch

class SparkStreaming(sparkConf: SparkConf, args: Array[String]) {

  val sparkconf = sparkConf;

  // get the directory in which the stream is filled.
  val inputDirectory = args(0)

  // number of seconds per window
  val seconds = args(1).toInt;

  // K: number of heavy hitters stored
  val TOPK = args(2).toInt;

  // precise Or approx
  val execType = args(3);

  //  create a StreamingContext, the main entry point for all streaming functionality.
  val ssc = new StreamingContext(sparkConf, Seconds(seconds));
  var global = collection.mutable.Map[(String,String),Int]()
  val width = 100
  val depth = 10
  val prime_number = List[Int](2,3,5,7,11,13,17,19,23,29)
  var global_CMS = Array.ofDim[Int](depth,width)

  def consume() {

    // create a DStream that represents streaming data from a directory source.
    val linesDStream = ssc.textFileStream(inputDirectory);

    // parse the stream. (line -> (IP1, IP2))
    val words = linesDStream.map(x => (x.split("\t")(0), x.split("\t")(1)))


    if (execType.contains("precise")) {
        //TODO : Implement precise calculation
        words.foreachRDD(rdd=> {
          if (rdd.count() != 0) {
            println("============precise=============")
            val batch = rdd.map(x=>(x,1)).reduceByKey(_+_).map{ x=>(x._2,x._1)}.sortByKey(false)
            val batch_list = batch.map(x=>(x._2,x._1)).collect()
            batch_list.foreach(data=>{
              if(global.contains(data._1)){
                val count_update = global.get(data._1).get + data._2
                global.put(data._1,count_update)
              }else{
                global.put(data._1,data._2)
              }
            })
            val output =batch.take(TOPK).mkString(",")
            val GlobalOutput = global.toList.sortWith((x,y)=>x._2>y._2).map(x=>(x._2,x._1)).take(TOPK).mkString(",")
            println(s"This batch:[$output]")
            println(s"Global:[$GlobalOutput]")
          }
        }
        )


    } else if (execType.contains("approx")) {
      //TODO : Implement approx calculation (you will have to implement the CM-sketch as well
      def hashFunc(key: String, hashNumber: Int): Int = {
        var hash: Long = 0
        for (ch <- key.toCharArray)
          hash = hashNumber * hash + ch.toInt
        hash = hash ^ (hash >> 20) ^ (hash >> 12)
        val Re = (math.abs(hash ^ (hash >> 7) ^ (hash >> 4))%width).toInt
        Re
      }


      words.foreachRDD(rdd=>{
        if(rdd.count()!=0){
          println("============approx=============")
          var batch_CMS = Array.ofDim[Int](depth,width)
          val Collects = rdd.map(x=>x._1+","+x._2).collect()
          Collects.foreach(x=>{
            for(i<-0 to (depth-1)){
              var Index = hashFunc(x,prime_number(i))
              //println(x,Index)
              batch_CMS(i)(Index) = batch_CMS(i)(Index)+1
              global_CMS(i)(Index) = global_CMS(i)(Index)+1
            }
          })
          val ip = "161.69.48.219,161.69.45.5"
          var value_global = List[Int]()
          var value_batch = List[Int]()
          for(i<-0 to (depth-1)){
            var index = hashFunc(ip,prime_number(i))
            value_global = value_global:+global_CMS(i)(index)
            value_batch = value_batch:+batch_CMS(i)(index)
          }
          println(s"This batch: [${value_batch.min}]")
          println(s"Global: [${value_global.min}]")
        }
      })


    }

    // Start the computation
    ssc.start()
  
    // Wait for the computation to terminate
    ssc.awaitTermination()
  }
}