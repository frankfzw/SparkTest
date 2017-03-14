import org.apache.spark._
import org.apache.spark.rdd.RDD


object MultiShuffleTest {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println("Usage: spark-submit --class MultiShuffleTest --jar path_to_SparkTest.jar " +
        "num_shuffle min_partition max_partition records_per_partition")
      System.exit(0)
    }
    val numShuffle = args(0).toInt
    val minPartition = args(1).toInt
    val maxPartition = args(2).toInt
    val recordsPerIndex = args(3).toInt
    if (numShuffle > 4) {
      println("Usage: spark-submit --class MultiShuffleTest --jar path_to_SparkTest.jar " +
        "num_shuffle min_partition max_partition records")
      println(s"num_shuffle should be 1 or 2 or 3 or 4. Got ${numShuffle}")
      System.exit(0)
    }

    val rddArray = new Array[RDD[String]](numShuffle)

    val conf = new SparkConf()
    conf.setAppName("MultiShuffleTest")
    val sc = new SparkContext(conf)
    for (i <- 0 until numShuffle) {
      val random = scala.util.Random
      val par = random.nextInt(maxPartition - minPartition) + minPartition
      val r = sc.parallelize(0 to par, par).mapPartitionsWithIndex { case (index, _) =>
        Iterator.tabulate(recordsPerIndex) {offset =>
          var s = ""
          val x = scala.util.Random.alphanumeric
          x take 10 foreach(x => s += x)
          s
        }
      }
      rddArray(i) = r
    }
    val stepArray = rddArray.map { rdd =>
      rdd.map(s => (s(0), 1)).reduceByKey(_ + _)
    }
    val a = stepArray(0)
    val res = numShuffle match {
      case 1 => a
      case 2 => a.cogroup(stepArray(1))
      case 3 => a.cogroup(stepArray(1), stepArray(2))
      case 4 => a.cogroup(stepArray(1), stepArray(2), stepArray(3))
    }
    println(res.count())
    println(res.toDebugString)
  }
}
