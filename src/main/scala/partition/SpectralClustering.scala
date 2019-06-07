package partition
import util.Graph
import org.apache.spark.mllib.clustering.PowerIterationClustering

object SpectralClustering {

  def gaussianSimilarity(dist:Double,sigma:Double): Double =
    math.exp(-dist/(2*sigma*sigma))

  def partition(graph: Graph, partitions:Int, maxIter:Int): Graph ={

    val simEdge = graph.edgeRDD.map(
      x=>(x._1.toString.toLong,
        x._2.toString.toLong,
        gaussianSimilarity(x._3,1))
    )

    val modelPIC = new PowerIterationClustering()
      .setK(partitions)// k : 期望聚类数
      .setMaxIterations(maxIter)//幂迭代最大次数
      .setInitializationMode("degree")//使用度初始化
      .run(simEdge)

    val assign = modelPIC.assignments.map(x=>(x.id.toString,x.cluster))
    graph.buildPartitionGraph(assign)

    graph
  }

}
