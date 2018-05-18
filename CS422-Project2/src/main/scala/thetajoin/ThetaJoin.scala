package thetajoin


import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.util.control.Breaks.{break, breakable}


class ThetaJoin(numR: Long, numS: Long, reducers: Int, bucketsize: Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("ThetaJoin")

  // random samples for each relation
  // helper structures, you are allowed
  // not to use them
  var horizontalBoundaries = Array[Int]()
  var verticalBoundaries = Array[Int]()

  // number of values that fall in each partition
  // helper structures, you are allowed
  // not to use them
  var horizontalCounts = Array[Int]()
  var verticalCounts = Array[Int]()
  val size = math.sqrt(numR * numS / reducers).toInt


  /*
   * this method gets as input two datasets and the condition
   * and returns an RDD with the result by projecting only 
   * attr1 and attr2
   * You are not allowed to change the definition of this function.
   * */
  def theta_join(dataset1: Dataset, dataset2: Dataset, attr1: String, attr2: String, op: String): RDD[(Int, Int)] = {
    val schema1 = dataset1.getSchema
    val schema2 = dataset2.getSchema

    val rdd1 = dataset1.getRDD
    val rdd2 = dataset2.getRDD
    //rdd1.foreach(println)

    val index1 = schema1.indexOf(attr1)
    val index2 = schema2.indexOf(attr2)

    // implement the algorithm
    val sample_number_hor = (numS / size).toInt
    val sample_number_ver = (numR / size).toInt

    def histogram(X: Int, Y: Int, sample: Array[Int]): (Int, Int) = {
      var left = 0
      var right = 0
      var length = sample.length
      var index = 0
      breakable {
        for (i <- 0 to length) {
          if (i == 0) {
            left = 0
            right = sample(i)
          } else if (i == length) {
            left = sample(i - 1) + 1
            right = Int.MaxValue

          } else {
            left = sample(i - 1) + 1
            right = sample(i)
          }
          if ((X >= left) && (X <= right)) {
            index = i
            break()
          }
        }
      }
      (index, Y)
    }

    val sort_hor = rdd1.map(x => (x.getInt(index1), 'R')).sortBy(_._1)
    val sort_ver = rdd2.map(x => (x.getInt(index1), 'C')).sortBy(_._1)
    //    sort.foreach(println)
    //    val sort_hor = rdd1.map(x => x.getInt(index1)).collect().sortWith(_ < _)
    //    val sort_ver = rdd2.map(x => x.getInt(index1)).collect().sortWith(_ < _)
    //sort_hor.foreach(println)

    val Reduce_hor = rdd1.map(x => x.getInt(index1)).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    horizontalBoundaries = Reduce_hor.takeSample(false,sample_number_hor).map(x=>x._1).sortWith(_ < _)
    //horizontalBoundaries = Array(66, 152, 3355, 18819, 969580)
    //horizontalBoundaries = Array(94,7945,47225,94551,1140868)


    val Histogram_hor = Reduce_hor.map(x => histogram(x._1, x._2, horizontalBoundaries)).reduceByKey((x, y) => x + y).collect().sortWith(_._1 < _._1)
    horizontalCounts = Histogram_hor.map(x => x._2)
    val Reduce_ver = rdd2.map(x => x.getInt(index2)).map(x => (x, 1)).reduceByKey((x, y) => x + y)
    verticalBoundaries  = Reduce_ver.takeSample(false,sample_number_ver).map(x=>x._1).sortWith(_ < _)
    //verticalBoundaries = Array(604, 936, 4027, 8436, 48177444)
    //verticalBoundaries = Array(0,32,86,2337,5518)
    val Histogram_ver = Reduce_ver.map(x => histogram(x._1, x._2, verticalBoundaries)).reduceByKey((x, y) => x + y).collect().sortWith(_._1 < _._1)
    verticalCounts = Histogram_ver.map(x => x._2)


    println(horizontalBoundaries.deep.mkString(","))
    println(verticalBoundaries.deep.mkString(","))
    println(horizontalCounts.deep.mkString(","))
    println(verticalCounts.deep.mkString(","))


    //task two
    def build_matrix(op: String, sample_number_ver: Int, sample_number_hor: Int, horizontalBoundaries: Array[Int], verticalBoundaries: Array[Int]): Array[Array[String]] = {

      var Matrix = Array.ofDim[String](sample_number_ver + 1, sample_number_hor + 1)
      var left_hor = new Array[Int](sample_number_hor + 1)
      var left_ver = new Array[Int](sample_number_ver + 1)
      var right_hor = new Array[Int](sample_number_hor + 1)
      var right_ver = new Array[Int](sample_number_ver + 1)

      for (i <- 0 to sample_number_ver) {
        if (i == 0) {
          left_ver(i) = 0
          right_ver(i) = verticalBoundaries(i)
        } else if (i == sample_number_ver) {
          left_ver(i) = verticalBoundaries(i - 1) + 1
          right_ver(i) = Int.MaxValue
        } else {
          left_ver(i) = verticalBoundaries(i - 1) + 1
          right_ver(i) = verticalBoundaries(i)
        }
      }

      for (i <- 0 to sample_number_hor) {
        if (i == 0) {
          left_hor(i) = 0
          right_hor(i) = horizontalBoundaries(i)
        } else if (i == sample_number_hor) {
          left_hor(i) = horizontalBoundaries(i - 1) + 1
          right_hor(i) = Int.MaxValue
        } else {
          left_hor(i) = horizontalBoundaries(i - 1) + 1
          right_hor(i) = horizontalBoundaries(i)
        }
      }

      op match {
        case "!=" => {
          for (i <- 0 to sample_number_ver) {
            for (j <- 0 to sample_number_hor) {
              Matrix(i)(j) = "X"
            }
          }
        }

        case "=" => {
          for (i <- 0 to sample_number_ver) {
            for (j <- 0 to sample_number_hor) {
              if ((left_hor(j) > right_ver(i)) || (left_ver(i) > right_hor(j))) {
                Matrix(i)(j) = "0"
              } else {
                Matrix(i)(j) = "X"
              }
            }
          }
        }


        case "<" => {
          for (i <- 0 to sample_number_ver) {
            for (j <- 0 to sample_number_hor) {
              if (left_hor(j) >= right_ver(i)) {
                Matrix(i)(j) = "0"
              } else {
                Matrix(i)(j) = "X"
              }
            }
          }
        }

        case "<=" => {
          for (i <- 0 to sample_number_ver) {
            for (j <- 0 to sample_number_hor) {
              if (left_hor(j) > right_ver(i)) {
                Matrix(i)(j) = "0"
              } else {
                Matrix(i)(j) = "X"
              }
            }
          }
        }

        case ">" => {
          for (i <- 0 to sample_number_ver) {
            for (j <- 0 to sample_number_hor) {
              if (left_ver(i) >= right_hor(j)) {
                Matrix(i)(j) = "0"
              } else {
                Matrix(i)(j) = "X"
              }
            }
          }
        }

        case ">=" => {
          for (i <- 0 to sample_number_ver) {
            for (j <- 0 to sample_number_hor) {
              if (left_ver(i) > right_hor(j)) {
                Matrix(i)(j) = "0"
              } else {
                Matrix(i)(j) = "X"
              }
            }
          }
        }
      }
      print(Matrix.map(_.mkString).mkString("\n"))
      Matrix
    }

    // print(Matrix.map(_.mkString).mkString("\n"))
    var matrix = build_matrix(op, sample_number_ver, sample_number_hor, horizontalBoundaries, verticalBoundaries)

    def CoverRow(row_first: Int, row_last: Int, maxInput: Int, Matrix: Array[Array[String]]): (Double, Int, List[(Int, Int)], List[(Int, Int)]) = {

      // find the location of row or col
      def find_location(row: Int, Counts: Array[Int]): Int = {
        var Row = row
        var Index = 0
        while (Row >= 0) {
          Row = Row - Counts(Index)
          Index = Index + 1
        }
        Index - 1
      }

      val row_size = Matrix.length
      val col_size = Matrix(0).length
      val region_row = row_last - row_first + 1

      var candidate_num = 0

      var region = 0
      var score = 0.0
      var col_boundaries = List[(Int, Int)]()
      var row_boundaries = List[(Int, Int)]()
      var Return = (0.0, 0, col_boundaries, row_boundaries)

      var col = 0
      var region_col = maxInput / region_row
      while (col <= numS - 1 && region_col >= 1) {
        // use to creat another region
        var count = 0
        var start_col = col

        while (col <= math.min(numS - 1, start_col + region_col - 1)) {
          //use to add col in each region
          for (r <- row_first to row_last) {
            var Index_row = find_location(r, verticalCounts)
            var Index_col = find_location(col, horizontalCounts)
            if (Matrix(Index_row)(Index_col) == "X") {
              count = count + 1
            }
          }
          col = col + 1
        }
        candidate_num = candidate_num + count

        if (count != 0) {
          region = region + 1
          var col_range = (start_col, col - 1)
          var row_range = (row_first, row_last)
          col_boundaries = col_boundaries :+ col_range
          row_boundaries = row_boundaries :+ row_range
        }
      }

      if (region == 0) {
        Return = (0.0, 0, List[(Int, Int)](), List[(Int, Int)]())
      } else {
        score = candidate_num / region.toDouble
        Return = (score, region, col_boundaries, row_boundaries)
      }
      Return
    }


    def CoverSubMatrix(row_start: Int, maxInput: Int, Matrix: Array[Array[String]]): (Int, List[(Int, Int)], List[(Int, Int)]) = {
      var maxScore = -1.0
      var bestRow = 0
      var rUsed = 0
      var Col_boundaries = List[(Int, Int)]()
      var Row_boundaries = List[(Int, Int)]()


      breakable {
        for (i <- 0 to (maxInput - 1)) {
          if (row_start + i >= numR) {
            break()
          }
          var Return = CoverRow(row_start, row_start + i, maxInput, Matrix)
          if (Return._1 >= maxScore) {
            maxScore = Return._1
            Col_boundaries = Return._3
            Row_boundaries = Return._4
            bestRow = row_start + i

          }
        }
      }


      var Return = (bestRow + 1, Row_boundaries, Col_boundaries)
      Return
    }

    def M_Bucket_I(maxInput: Int, Matrix: Array[Array[String]]): (List[(Int, (Int, Int))], List[(Int, (Int, Int))]) = {
      var row = 0
      var Row_boundaries = List[(Int, Int)]()
      var Col_boundaries = List[(Int, Int)]()

      while (row < numR) {
        var Return = CoverSubMatrix(row, maxInput, Matrix)
        row = Return._1
        Row_boundaries = Row_boundaries ++ Return._2
        Col_boundaries = Col_boundaries ++ Return._3
      }
      var col_boundaries_withreducer = Col_boundaries.zipWithIndex.map(x => (x._2, x._1))
      var row_boundaries_withreducer = Row_boundaries.zipWithIndex.map(x => (x._2, x._1))
      val Return = (col_boundaries_withreducer, row_boundaries_withreducer)
      Return
    }

    def Assign(input: ((Int, Char), Long), boundaries_withreducer: List[(Int, (Int, Int))]): List[(Int, (Int, Char))] = {
      var assignment = List[(Int, (Int, Char))]()
      for (i <- 0 to boundaries_withreducer.length - 1) {
        if ((input._2 >= boundaries_withreducer(i)._2._1) && (input._2 <= boundaries_withreducer(i)._2._2)) {
          assignment = assignment :+ (boundaries_withreducer(i)._1, input._1)
        }
      }

      assignment
    }

    val boundaries_withreducer = M_Bucket_I(bucketsize, matrix)
    val row_boundaries_withreducer = boundaries_withreducer._2
    val col_boundaries_withreducer = boundaries_withreducer._1
    val Reducer_number = col_boundaries_withreducer.length
    val row_assign = sort_ver.zipWithIndex.map(x => Assign(x, row_boundaries_withreducer)).flatMap(list => list)
    val col_assign = sort_hor.zipWithIndex.map(x => Assign(x, col_boundaries_withreducer)).flatMap(list => list)


    var merge = (row_assign++col_assign).groupByKey(Reducer_number).flatMap{x=>{
      var key = x._1
      var left = x._2.filter(x=>x._2=='R').map(x=>(key,x._1)).iterator
      var right = x._2.filter(x=>x._2=='C').map(x=>(key,x._1)).iterator
      var join = local_thetajoin(left,right,op)
      join
      }
    }
    merge
  }



  /*
   * this method takes as input two lists of values that belong to the same partition
   * and performs the theta join on them. Both datasets are lists of tuples (Int, Int)
   * where ._1 is the partition number and ._2 is the value. 
   * Of course you might change this function (both definition and body) if it does not 
   * fit your needs :)
   * */
  def local_thetajoin(dat1: Iterator[(Int, Int)], dat2: Iterator[(Int, Int)], op: String): Iterator[(Int, Int)] = {
    var res = List[(Int, Int)]()
    var dat2List = dat2.toList

    while (dat1.hasNext) {
      val row1 = dat1.next()
      for (row2 <- dat2List) {
        if (checkCondition(row1._2, row2._2, op)) {
          res = res :+ (row1._2, row2._2)
        }
      }
    }
    res.iterator
  }

  def checkCondition(value1: Int, value2: Int, op: String): Boolean = {
    op match {
      case "=" => value1 == value2
      case "<" => value1 < value2
      case "<=" => value1 <= value2
      case ">" => value1 > value2
      case ">=" => value1 >= value2
    }
  }
}

