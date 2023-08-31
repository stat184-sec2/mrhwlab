import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

object WordCount {
  def main(args: Array[String]) = {
    val sc = getSC()
    val input = getRDD(sc) //comment out for testing  
    //val input = getTestRDD(sc) //uncomment for testing, or use spark-shell for testing
                                 // the doWordCount function
    val counts = doWordCount(input)
    saveit(counts)
  }
  
  def getSC() = {
    val conf = new SparkConf().setAppName("wc")
    val sc = new SparkContext(conf)
    sc
  }
  def getRDD(sc: SparkContext) = {
    val input = sc.textFile("/datasets/wap")
    input
  }

  def getTestRDD(sc: SparkContext) = {
     val myTestList = List("this is the first line",
                           "this is the second line",
                           "the end")
     val input = sc.parallelize(myTestList, 2)
     input

  }

  def doWordCount(input: RDD[String]) = {
    val words = input.flatMap(_.split(" "))
    val kv = words.map(word => (word,1))
    val counts = kv.reduceByKey((x,y) => x+y)
    counts
  }
  def saveit(counts: org.apache.spark.rdd.RDD[(String, Int)]) = {
    counts.saveAsTextFile("result")
  }
}
