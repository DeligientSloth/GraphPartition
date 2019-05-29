package partition
import util.Graph
import org.apache.spark.mllib.clustering.PowerIterationClustering

object SpectralClustering {
  //    val graphSimilarity = graph.edgeRDD.map(x=>(x._1,x._2,))
  def gaussianSimilarity(dist:Double,sigma:Double): Double =
    math.exp(-dist/(2*sigma*sigma))

  def partition(graph: Graph,graphNum:Int,maxIter:Int): Graph ={

    val simEdge = graph.edgeRDD.map(
      x=>(x._1.toString.toLong,
        x._2.toString.toLong,
        gaussianSimilarity(x._3,1))
    )

    val modelPIC = new PowerIterationClustering()
      .setK(graphNum)// k : 期望聚类数
      .setMaxIterations(maxIter)//幂迭代最大次数
      .setInitializationMode("degree")//使用度初始化
      .run(simEdge)

    //输出聚类结果
    val clusters = modelPIC.assignments.collect().groupBy(_.cluster).mapValues(_.map(_.id))
//    clusters.foreach(x=>println(x._1,x._2.getClass))
    val assign = modelPIC.assignments.map(x=>(x.id.toString,x.cluster))
    assign.foreach(println)
    graph.buildPartitionGraph(assign)

    graph
//    val assignments = clusters.toList.sortBy { case (k, v) => v.length }
//    val assignmentsStr = assignments
//      .map { case (k, v) =>
//        s"$k -> ${v.sorted.mkString("[", ",", "]")}"
//      }.mkString(", ")
//    val sizesStr = assignments.map {
//      _._2.length
//    }.sorted.mkString("(", ",", ")")
//    println(s"Cluster assignments: $assignmentsStr\ncluster sizes: $sizesStr")
    /*
     * Cluster assignments: 1 -> [4,6,8,10], 0 -> [0,1,2,3,5,7,9,11]
     * cluster sizes: (4,8)
     */

  }

}
