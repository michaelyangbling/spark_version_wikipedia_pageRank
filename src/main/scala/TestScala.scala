import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel._
import java.io._
import org.apache.spark.broadcast.Broadcast
//this method store graph as rdd but distribute Pageranks as map to all nodes,
//optimize running time but use too much memory
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

  def addDangNodes(in: String):(String,Set[String])={
    (in, Set())
  }
  def giveRank(in:(Set[String],Double)):Array[(String,Double)]={
    val out=new Array[(String,Double)](in._1.size)
    var index=0
    val outRank=in._2/in._1.size
    for(each<-in._1){
      out(index)=(each, outRank)
      index=index+1
    }
    out
  }
  def main(args: Array[String]):Unit= {
    println("starting")
    val conf = new SparkConf().setMaster("local").setAppName("My App")  //for local run&debug
    //val conf = new SparkConf().setAppName("My App") //for AWS run
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    var pageLinksPair = textFile.map(webParser.parse)
      .filter(array=>array.length!=0)//eliminate ill-formatted HTMLs
      .map(getPairRdd).persist//convert to pairRDD : ( pageName,set(links) ) (String,Set[String])

    //var setPages=pageLinksPair.countByKey().keySet.toSet //set of provided pages
    //var broadcastSet=sc.broadcast(setPages)
    var dangleRdd=pageLinksPair.flatMap(pair=>pair._2).map(addDangNodes)//filter and add dangling nodes
    var cleanedData=dangleRdd.union(pageLinksPair).reduceByKey((x,y)=>x.++(y)).persist() //(String,Set[String])

    println("data-clean finished")
    //cleanedData is the graph
    val numPages=sc.broadcast(cleanedData.count())
    var PR=cleanedData.map(x=>(x._1,1.toDouble/numPages.value)).persist()//initial pageRank: Rdd (String,double)
    var PR0=cleanedData.map(x=>(x._1,0.toDouble)).persist()
    //the map can fit into the memory
    val randP=sc.broadcast(0.15/numPages.value) //here define probability of random jumping to a website as 0.15
    var dangNodes=cleanedData.filter(x=>x._2.isEmpty).map(x=>(x._1,0)).persist()//dangling nodes' RDD:(String,int)
    val numDang=dangNodes.count()
    //persist is to keep the RDD for future repeated use
    var dangWeight:Double=0
    var broadDangWeight:Broadcast[Double]=sc.broadcast(dangWeight)
    var i=1
    while(i<=args(2).toInt) { // iterate pageRank algorithm
      println("iterating")
      if(numDang==0)
        dangWeight=0.toDouble
      else
        dangWeight = dangNodes.join(PR).map(x=>x._2._2).aggregate(0.toDouble)((x, y) => x + y, (x2, y2) => x2 + y2) / numPages.value
      broadDangWeight=sc.broadcast(dangWeight)
      PR = cleanedData.join(PR) //(String, (Set[String],Double))
        .flatMap(x=>giveRank(x._2))  //Rdd (String,Double)
        .reduceByKey((x, y) => x + y).union(PR0).reduceByKey((x, y) => x + y)
        .mapValues(y => (y + broadDangWeight.value) * 0.85 + randP.value).persist() //update PageRank
      //
      i = i + 1
    }
    println("page-rank finished")
    val topPages=PR.sortBy(_._2,ascending=false).take(args(3).toInt) //take top k
    pageLinksPair=null
    dangleRdd=null
    cleanedData=null
    PR0=null
    PR=null
    dangNodes=null
    println("top k finished--will get results")
    try {
      val pw = new PrintWriter(new File(args(1)))
      for(each<-topPages)
      {pw.write(each._1+"~~~~~~~   "+each._2.toString+"\n")}
      pw.close
    } catch {
      case _: Throwable => {
        for(each<-topPages){
          println(each._1+"~~~~~~~   "+each._2.toString)
        }
      }
    }
    println("all done")

    //println(c)
    //cleanedData.saveAsTextFile(args(1));

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