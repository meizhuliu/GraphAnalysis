/**
 * Created by meizhu on 9/7/17.
 */
package connectedcomponents


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j._


object  connectecomponents {

  def idHash(id:String): VertexId = {
    id.toLowerCase.replace(" ","").hashCode.toLong
  }

  def main(args: Array[String]): Unit = {
    //  val conf = new SparkConf().setMaster("local").setAppName("test").set("spark.cores.max", "10").set("spark.authenticate.secret","junk");
    //  val spark = new SparkContext(conf)
    val conf = new SparkConf().setAppName("Components Statistics")

    val spark = new SparkContext(conf)
    val inputfile = args(0)+"/";
    val outputpath = "hdfs:///tmp/meizhu/cc/"+args(1).trim

    val testFiles = spark.textFile(inputfile);


    val timePtr = """regTs#(\d\d\d\d\d\d\d\d\d\d?)""".r

    val testraw: RDD[(String, String, String, String, String, String)] =
      testFiles.map(line => line.split("\t"))
        .filter( line => line.length >= 40)
        .map(lines=> {
        var otherFields:String = lines(39);
        val regTimer = timePtr.findFirstMatchIn(otherFields).getOrElse("no reg time")
        (lines(0).trim, lines(5).trim, lines(11).trim, lines(3).trim, lines(4).trim, regTimer.toString)
      })


    testraw.map(line=>{line._1+"\t"+line._2+"\t"+line._3+"\t"+line._4+"\t"+line._5+"\t"+line._6}).saveAsTextFile(outputpath+"feature")
    println("Finish Step 1......")

    //  val IP = test.map(line => line._2.to).distinct()
    //  val Acc = test.map(line => ).distinct()


    val testFiles2 = spark.textFile(outputpath+"feature/part*");
    val test: RDD[(String, String, String)] = testFiles2.map(line => {
      val lines = line.split("\t")
      (lines(0).trim, lines(1).trim, lines(2).trim) //label, IP, account ID
    })

    //     val test: RDD[(String, String, String)] = testraw.map(line => {
    //         (line._1, line._2, line._3)
    //     })
    println("Finish Step 2: Reading data into RDD......")

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val verticesIP: RDD[(VertexId, (String, Int))] = test.map(line => (idHash(line._2), (line._2, 1))).distinct()//IP vertices
    val verticesAcc: RDD[(VertexId, (String, Int))] = test.map(line => (idHash(line._3), (line._3, 0))).distinct()//account vertices
    val verticesAll: RDD[(VertexId, (String, Int))] = verticesIP.union(verticesAcc).cache()

    verticesAll.map(line => {line._1+"\t"+line._2._1}).saveAsTextFile(outputpath+"v_map")


    val edges: RDD[Edge[(Int)]] = test.map(line=>{
      val edge_label = line._1.toInt
      val srcVid = idHash(line._2)
      val desVid = idHash(line._3)
      // val timer = line._4.toLong
      Edge(srcVid, desVid, (edge_label)) //create the edges of IP-YID, where edge is label
    }).cache()


    println("Finish Step 3: Construct Edge and Vertices RDD......")

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    val defaultUser = ("", -1)
    val graph = Graph(verticesAll, edges, defaultUser)

    val t_0 = System.nanoTime()
    val cc =  graph.connectedComponents().vertices.cache() //get the connected components
    val t_1 = System.nanoTime()
    println("Elapsed time for CC construction: "+  (t_1-t_0)/60000000000.0 +"mins")

    cc.map(line => {line._1+"\t"+line._2}).saveAsTextFile(outputpath+"cc_map") //IP, YID

    println("Finish Step 4: Construct graph and obtain connected components......")

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////

    val t0 = System.nanoTime()

    //get the number of accounts and IPs in the cc
    val CompStats1 :RDD[(VertexId, (Long, Long))] = cc.join(verticesAll).map( line =>{
      if(line._2._2._2 == 0) { (line._2._1, (0L, 1L))} //if the vertex is account vertex
      else if(line._2._2._2 == 1) { (line._2._1, (1L, 0L))} //if the vertex is IP vertex
      else { (line._2._1, (0L, 0L))}
    }).reduceByKey((a,b)=>{(a._1+b._1, a._2+b._2)}) //get the number of accounts and IPs in the cc

    val t1 = System.nanoTime()

    println("Elapsed time of calculating the number of account IDs and IPs is: "+  (t1-t0)/60000000000.0 +"mins" )

    val edgeMap = edges.map{ i=> (i.srcId, i)}

    //get the number of positive/negative edges
    val CompStats2: RDD[(VertexId, (Long, Long))] = cc.join(edgeMap).map(line=>{
      if(line._2._2.attr == 1){(line._2._1, (1L, 0L))}
      else if(line._2._2.attr == -1){(line._2._1, (0L, 1L))}
      else{ (line._2._1, (0L, 0L)) }
    }).reduceByKey((a,b)=>{(a._1+b._1, a._2+b._2)})

    val t2 = System.nanoTime()
    println("Elapsed time of counting the number of positive and negative edges is: "+  (t2-t1)/60000000000.0 +"mins" )
    //  println("Finish Step 5: Obtain statistics of connected components......")

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////

    val result = CompStats1.join(CompStats2)



    result.map(line =>{
      line._1 +"\t"+line._2._1._1+"\t"+line._2._1._2+"\t"+line._2._2._1+"\t"+line._2._2._2
    }).saveAsTextFile(outputpath+"cc_stats")

    println("Finish Step 6: writing to the output done......")
  }
}
