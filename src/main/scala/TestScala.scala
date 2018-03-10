import org.apache.spark.{SparkConf, SparkContext}
object TestScala {2
  def main(args: Array[String]):Unit= {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/Users/yzh/Desktop/njtest/input")
    val counts = textFile.map(str=>webParser.parse(str))
    val a=1
    //counts.foreach(a=>println(a.toList))
    /*val a=scala.io.StdIn.readLine()
    webParser.parse(a).foreach(println)*/
  }
}
