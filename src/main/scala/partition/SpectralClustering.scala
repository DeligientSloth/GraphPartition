package partition

import util.Graph
import org.apache.spark.mllib.clustering.PowerIterationClustering

object SpectralClustering {

    def gaussianSimilarity(dist: Double, sigma: Double): Double =
        math.exp(-dist / (2 * sigma * sigma))

    def partition(graph: Graph, partitions: Int, maxIter: Int, weightNorm:Boolean): Graph = {

        var weight:scala.collection.Map[String,Double]=null
        if(weightNorm==true)
            weight = graph.nodeRDD.map(x=>(x.getIdx,x.getWeight)).collectAsMap()

        def weight_norm(sim:Double,x:String,y:String):Double={
            if(weightNorm) sim/math.pow(weight(x)*weight(y), 2)
            else sim
        }
        val simEdge = graph.edgeRDD.map(
            x => (x._1.toString.toLong,
                    x._2.toString.toLong,
                    gaussianSimilarity(weight_norm(x._3,x._1,x._2), 1))
        )

        val modelPIC = new PowerIterationClustering()
                .setK(partitions) // k : 期望聚类数
                .setMaxIterations(maxIter) //幂迭代最大次数
                .setInitializationMode("degree") //使用度初始化
                .run(simEdge)

        val assign = modelPIC.assignments.map(x => (x.id.toString, x.cluster))
        graph.buildPartitionGraph(assign)
        graph
    }

}
