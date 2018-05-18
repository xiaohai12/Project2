package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd = dataset.getRDD()// data by tuple datatype is row
    val schema = dataset.getSchema()//the name of attributes

    val index = groupingAttributes.map(x => schema.indexOf(x))//index of agg attribute
    val indexAgg = schema.indexOf(aggAttribute) //aggragate attribute index

    //TODO Task 1

    def Partial(rdd:RDD[(String,Double)]):RDD[(String,Double)]= {
      val Part_ini = rdd.map { case (key, value) =>
        val keylist = key.split(",").map(_.trim).toList
        val AllList = keylist.toSet[String].subsets().map(_.toList).toList.map(x => (x.mkString(","), value))
        AllList
      }
      val part = Part_ini.flatMap(list=>list)
      part
    }


    def Get(rdd:RDD[Row],index: List[Int],indexAgg:Int):RDD[(String,Double)]={
      val rdd_map = rdd.map{x=>
        var groupAttr = ""
        for (i<-index){
          if(i==index(0))  {
            groupAttr = groupAttr+x.getAs[String](i)
          }
          else{
            groupAttr = groupAttr+","+x.getAs[String](i)
          }

        }

        (groupAttr, x.getInt(indexAgg).toDouble)
      }
      rdd_map
    }


    def COUNT(rdd: RDD[Row], schema: List[String], index: List[Int],indexAgg:Int):RDD[(String,Double)]={

      def Getcount(rdd:RDD[Row],index: List[Int]):RDD[(String,Double)]={
        val rdd_map = rdd.map{x=>
          var groupAttr = ""
          for (i<-index){
            if(i==index(0))  {
              groupAttr = groupAttr+x.getAs[String](i)
            }
            else{
              groupAttr = groupAttr+","+x.getAs[String](i)
            }

          }

          (groupAttr,1.0)
        }

        rdd_map
      }

      // MRSpread:
      val MapSpread = Getcount(rdd,index).reduceByKey((x,y)=>x+y).aggregateByKey(0.0)(_+_,_+_)

      val MapSpreadPartial = Partial(MapSpread)

      //MRAssembel:
      val MapAssemble = MapSpreadPartial.reduceByKey((x,y)=>x+y).aggregateByKey(0.0)(_+_,_+_)
      MapAssemble
    }

    def SUM(rdd: RDD[Row], schema: List[String], index: List[Int],indexAgg:Int):RDD[(String,Double)]={

      val MapSpread = Get(rdd,index,indexAgg).reduceByKey((x,y)=>x+y).aggregateByKey(0.0)(_+_,_+_)
      val MapSpreadPartial = Partial(MapSpread)
      val MapAssemble = MapSpreadPartial.reduceByKey((x,y)=>x+y).aggregateByKey(0.0)(_+_,_+_)
      MapAssemble
    }


    def MAX(rdd: RDD[Row], schema: List[String], index: List[Int],indexAgg:Int):RDD[(String,Double)]={

      val MapSpread = Get(rdd,index,indexAgg).reduceByKey(math.max(_,_)).aggregateByKey(0.0)(math.max(_,_),math.max(_,_))
      val MapSpreadPartial = Partial(MapSpread)
      val MapAssemble = MapSpreadPartial.reduceByKey(math.max(_,_)).aggregateByKey(0.0)(math.max(_,_),math.max(_,_))
      MapAssemble
    }

    def MIN(rdd: RDD[Row], schema: List[String], index: List[Int],indexAgg:Int):RDD[(String,Double)]={

      val MapSpread = Get(rdd,index,indexAgg).reduceByKey(math.min(_,_)).aggregateByKey(Double.PositiveInfinity)(math.min(_,_),math.min(_,_))
      val MapSpreadPartial = Partial(MapSpread)
      val MapAssemble = MapSpreadPartial.reduceByKey(math.min(_,_)).aggregateByKey(Double.PositiveInfinity)(math.min(_,_),math.min(_,_))
      MapAssemble
    }

    def AVG(rdd: RDD[Row], schema: List[String], index: List[Int],indexAgg:Int):RDD[(String,Double)]={

      val Sum = SUM(rdd,schema,index,indexAgg)
      //Sum.foreach(println)
      val Count = COUNT(rdd,schema,index,indexAgg)
      //Count.foreach(println)
      val combine = (Sum++Count).reduceByKey((x,y)=>math.max(x,y)/math.min(x,y))
      combine
    }




    val output = agg match{
      case "SUM" => SUM(rdd,schema,index,indexAgg)
      case "COUNT" => COUNT(rdd,schema,index,indexAgg)
      case "MAX" => MAX(rdd,schema,index,indexAgg)
      case "MIN" => MIN(rdd,schema,index,indexAgg)
      case "AVG" => AVG(rdd,schema,index,indexAgg)
    }


    output
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    //TODO naive algorithm for cube computation


    def Partial(rdd:RDD[Row],index: List[Int],indexAgg:Int,agg:String):RDD[(String,Double)]={
      val rdd_map = rdd.map{x=>
        var groupAttr = ""
        for (i<-index){
          if(i==index(0))  {
            groupAttr = groupAttr+x.getAs[String](i)
          }
          else{
            groupAttr = groupAttr+","+x.getAs[String](i)
          }

        }
        if(agg!= "COUNT"){
          (groupAttr, x.getInt(indexAgg).toDouble)
        }else{
          (groupAttr, 1.0)
        }

      }
      val Part_ini = rdd_map.map { case (key, value) =>
        val keylist = key.split(",").map(_.trim).toList
        val AllList = keylist.toSet[String].subsets().map(_.toList).toList.map(x => (x.mkString(","), value))
        AllList
      }
      val part = Part_ini.flatMap(list=>list)
      part
    }




    val rdd = dataset.getRDD()
    val schema = dataset.getSchema()
    val index = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg = schema.indexOf(aggAttribute)
    val emit = Partial(rdd,index,indexAgg,agg)

    val output = agg match{


      case "SUM" => {
        val Sum_naive = emit.reduceByKey((x,y)=>x+y).aggregateByKey(0.0)(_+_,_+_)
        Sum_naive
      }
      case "COUNT" =>  {
        val Count_naive = emit.reduceByKey((x,y)=>x+y).aggregateByKey(0.0)(_+_,_+_)
        Count_naive

      }
      case "MAX" => {
        val Max_naive = emit.reduceByKey(math.max(_,_)).aggregateByKey(0.0)(math.max(_,_),math.max(_,_))
        Max_naive
      }
      case "MIN" => {
        val Max_naive = emit.reduceByKey(math.min(_,_)).aggregateByKey(Double.PositiveInfinity)(math.min(_,_),math.min(_,_))
        Max_naive
      }
      case "AVG" => {
        val Sum_naive = emit.reduceByKey((x,y)=>x+y).aggregateByKey(0.0)(_+_,_+_)
        //Sum_naive.foreach(println)
        val Count_naive = Partial(rdd,index,indexAgg,"COUNT").reduceByKey((x,y)=>x+y).aggregateByKey(0.0)(_+_,_+_)
        //Count_naive.foreach(println)
        val combine = (Sum_naive++Count_naive).reduceByKey((x,y)=>math.max(x,y)/math.min(x,y))
        //combine.foreach(println)
        combine
      }
    }

    output
  }

}
