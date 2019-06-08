package partition

import org.apache.spark.rdd.RDD

import util.Graph
import util.Node
import scala.util.Random
import scala.util.control.Breaks._


object MetisPartition{

    // Three Main Phases

    // Step 1: Coarsening Phase
    private def coarsen(graph: Graph, c: Int, k: Int): Graph={
        /*
        * @input:  origin graph G_o
        *          coarsening parameter c
        * @output: coarsen graph G_c
        * */

        var coarsenedGraph = graph
        while(coarsenedGraph.nodeNum < c*k){
            coarsenedGraph = heavyEdgeMatching(coarsenedGraph)
        }
        coarsenedGraph
    }

    // Step 2: Partitioning Phase
    private def initialPartition(graph: Graph, k: Int): Graph={
        // Use Spectral Clustering
        var splitGraph = graph

        splitGraph
    }

    private def uncoarsen(graph: Graph, k: Int): Graph={
        var refinedGraph = graph
        // 顶点可能从一个分区移动到其他许多分区中，甚至移动到其它k-1个分区中
        // 使用k(k-1)个优先队列，每一个对应一种移动

        // 算法每一步找到在这k(k-1)队列中具有最高获益的移动，
        // 移动能够保持或者改进均衡的具有最高获益的顶点。

        // 移动后，全部的k(k-1)个队列都更新数据。

        while(true) { // todo: While current refined Graph has coarsen nodes
            refinedGraph.nodeRDD.filter()

        }
        graph
    }

    // Maximal Matching Algorithm

    // Heavy-edge matching (HEM)
    private def heavyEdgeMatching(graph: Graph): Graph={
        // Step 1: Visit the vertices of the graph in random order.

        // Step 2: Match a vertex with the unmatched vertex that is connected with the heavier edge.
        graph
    }

    def partition(graph: Graph, k:Int): Graph={
        var partitionedGraph = graph
        // 1.coarsening phase
        partitionedGraph = coarsen(graph, 15, k)

        // 2.partitioning phase
        partitionedGraph = initialPartition(partitionedGraph, k)

        // 3.un-coarsening phase
        partitionedGraph = uncoarsen(graph, k)

        graph
    }

}