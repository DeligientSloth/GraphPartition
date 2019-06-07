package partition

import org.apache.spark.rdd.RDD

import util.Graph
import util.Node
import scala.util.Random
import scala.util.control.Breaks._


object MetisPartition{

    private var k = 0

    // Three Main Phases

    // Step 1: Coarsening Phase
    private def coarsen(graph: Graph, c: Int): Graph={
        /*
        * @input:  origin graph G_o
        *          coarsening parameter c
        * @output: coarsen graph G_c
        *
        * */

        var coarsenedGraph = graph

        while(coarsenedGraph.nodeNum < c*k){
            coarsenedGraph = heavyEdgeMatching(coarsenedGraph)
        }

        coarsenedGraph
    }

    // Step 2: Partitioning Phase
    private def initialPartition(graph: Graph): Graph={
        // Use Spectral Clustering
        var splitGraph = graph

        splitGraph
    }

    private def uncoarsen(graph: Graph): Graph={
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
        this.k = k
        graph
    }

}