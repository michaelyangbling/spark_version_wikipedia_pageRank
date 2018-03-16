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
  def giveRank(in:Set[String], page:String,map:Map[String,Double]):Array[(String,Double)]={
    val out=new Array[(String,Double)](in.size)
    var index=0
    val rank:Double=map(page)/in.size
    for(each<-in){
      out(index)=(each, rank)
      index=index+1
    }
    out
  }
  def main(args: Array[String]):Unit= {
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    //val conf = new SparkConf().setAppName("My App")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("/Users/yzh/Desktop/njtest/inSmall")
    val pageLinksPair = textFile.map(webParser.parse)
      .filter(array=>array.length!=0)//eliminate ill-formatted HTMLs
      .map(getPairRdd).persist//convert to pairRDD : ( pageName,set(links) ) (String,Set[String])

    val setPages=pageLinksPair.countByKey().keySet.toSet //set of provided pages
    val dangleRdd=pageLinksPair.flatMap(pair=>pair._2).
      filter(page=>filterDangNodes(page,setPages)).map(addDangNodes)//filter and add dangling nodes
    val cleanedData=dangleRdd.union(pageLinksPair).reduceByKey((x,y)=>x.++(y)) //(String,Set[String])
    //cleanedData is the graph
    val n=cleanedData.count()
    var PR=cleanedData.map(x=>(x._1,1.toDouble/n)).collectAsMap().toMap //initial pageRank: map[String,double]
    val PR0=cleanedData.map(x=>(x._1,0.toDouble)).persist
    //the map can fit into the memory
    val randP=0.15/n //here define probability of random jumping to a website as 0.15
    val dangNodes=cleanedData.filter(x=>x._2.isEmpty).map(x=>x._1).persist//dangling nodes' RDD:String
    //persist is to keep the RDD for future repeated use
    var dangWeight:Double=0
    var i=1
    while(i<=10) {
      dangWeight = dangNodes.map(x => PR(x)).aggregate(0.toDouble)((x, y) => x + y, (x2, y2) => x2 + y2) / n
      PR = cleanedData.flatMap(x => giveRank(x._2, x._1, PR)) //Rdd (String,Double)
        .reduceByKey((x, y) => x + y).union(PR0).reduceByKey((x, y) => x + y)
        .mapValues(y => (y + randP) * 0.85 + dangWeight).collectAsMap.toMap //update PageRank
      i = i + 1
    }
    val PRfinal=new Array[(String,Double)](PR.size)
    var index=0
    for((k,v)<-PR){
      PRfinal(index)=(k,v)
    }
    val a=sc.parallelize(PRfinal)
    val b=a.sortBy(_._2,ascending=false)
    b.persist
    val c=b.take(100)
    println(c)




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