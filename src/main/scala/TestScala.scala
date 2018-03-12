import org.apache.spark.{SparkConf, SparkContext}
object TestScala {
  def getPairRdd(in:Array[String]) :(String,Set[String])= {
    if (in.length == 1)
      (in(0), Set())
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
  def filterDangNodes(in:String,pageSet:Set[String]):Boolean={
      if (pageSet.contains(in))
        false
      else
        true
  }
  def addDangNodes(in: String):(String,Set[String])={
    (in, Set())
  }
  def main(args: Array[String]):Unit= {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/Users/yzh/Desktop/njtest/input2")
    val pageLinksPair = textFile.map(webParser.parse)
      .filter(array=>array.length!=0)//eliminate ill-formatted HTMLs
      .map(getPairRdd).persist//convert to pairRDD : ( pageName,set(links) )

    val setPages=pageLinksPair.countByKey().keySet.toSet //set of provided pages
    val dangleRdd=pageLinksPair.flatMap(pair=>pair._2).
      filter(page=>filterDangNodes(page,setPages)).map(addDangNodes)//filter and add dangling nodes
    val cleanedData=dangleRdd.union(pageLinksPair).reduceByKey((x,y)=>x.++(y))
    cleanedData.saveAsTextFile("/Users/yzh/Desktop/njtest/output");
    
    /*print(pageLinksPair.flatMap(pair=>pair._2).count)
    println("-------------")
    println("-------------")
    print(dangleRdd.count)*/
    //counts.foreach(println)
    //println(nowPages)
    //counts.saveAsTextFile("/Users/yzh/Desktop/njtest/output")
    /*val a=scala.io.StdIn.readLine()
    webParser.parse(a).foreach(println)*/
  }
}