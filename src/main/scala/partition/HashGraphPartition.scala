package partition

import org.apache.spark.{HashPartitioner, TaskContext}
import util.Graph

object HashGraphPartition {
    def partition(graph: Graph, partitions: Int): Graph = {
        val assigenment = graph.nodeIdxRDD.map(x => (x, 0)).partitionBy(
            new HashPartitioner(partitions)).map(x => (x._1, TaskContext.getPartitionId))
        graph.buildPartitionGraph(assigenment)
    }
}
