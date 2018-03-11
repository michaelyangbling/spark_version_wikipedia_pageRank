import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
object TestScala {
  def getPairRdd(in:Array[String]) :(String,Set[String])={
    if (in.length==1)
      (in(0),Set())
    else {
      var set: Set[String] = Set()
      var index = 1
      while (index <= in.length - 1) {
        set = set + in(index)
        index = index + 1
      }
      (in(0), set)
    }
  }
  def main(args: Array[String]):Unit= {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/Users/yzh/Desktop/njtest/inSmall/meme")
    val counts = textFile.map(webParser.parse)
                         .filter(array=>array.length!=0)//eliminate ill-formatted HTMLs
                         .map(str=>(getPairRdd(str)._1,getPairRdd(str)._2))//convert to pairRDD : ( pageName,set(links) )
    //val nowPages=counts.countByKey()
    val nowPages=counts.lookup("10")
    counts.foreach(println)
    //println(nowPages)
    //counts.saveAsTextFile("/Users/yzh/Desktop/njtest/output")
    /*val a=scala.io.StdIn.readLine()
    webParser.parse(a).foreach(println)*/
  }
}
