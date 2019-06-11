package partition

import util.Graph
import util.Node
import scala.util.control.Breaks._

class MetisPartition{

    private var k:Int = 3

    def this(k:Int){
        this()
        this.k = k
    }

    def partition(graph: Graph, c:Int, mode:String,weightNorm:Boolean): Graph={
        var partitionedGraph = graph
        var level:Int=0 // coarsen and refine level
        // 1.coarsening phase
        val coarsenRes = coarsen(graph, c,mode)

        partitionedGraph = coarsenRes._1 // coarsen Graph
        level = coarsenRes._2  //coarsen level

        // 2.partitioning phase
        partitionedGraph = initialPartition(partitionedGraph, weightNorm:Boolean)
        println("Coarsened performance="+partitionedGraph.graphPartitionEvaluation)
        partitionedGraph.printGraph()

        // 3.un-coarsening phase
        partitionedGraph = refine(partitionedGraph,level) //need level to decide refine order
        println("Refined performance="+partitionedGraph.graphPartitionEvaluation)

        partitionedGraph
    }

    /** Part I:
      * Three functions of Main Phase -- coarsen(), initialPartition(), refine() */

    private def coarsen(graph: Graph, c: Int,mode:String): (Graph,Int)={
        /** Step 1: Coarsening Phase (via Maximal Matching Algorithm)
          * input:  origin graph G_o,
          *         coarsening parameter c,
          *         mode
          * output: coarsen graph G_c
        * */
        var coarsenedGraph = graph
        var level = 0
        while(coarsenedGraph.nodeNum > c*k){
            level += 1
            coarsenedGraph = maximalMatching(coarsenedGraph, mode,level)
            coarsenedGraph = refreshMarkedFlag(coarsenedGraph)
        }
        (coarsenedGraph,level)
    }

    private def initialPartition(graph: Graph, useWeightNorm:Boolean): Graph={
        /** Step 2: Partitioning Phase (via Spectral Clustering)
          * input:  graph - coarsened graph g
          *         useWeightNorm - whether to normalize the weights.
          * output: splitGraph - initial partition graph
          * */
        val splitGraph = SpectralClustering.partition(graph, k, 40, useWeightNorm)
        splitGraph
    }

    private def refine(graph: Graph,level:Int): Graph={
        /** Step 3: Refining Phase (via Kernighan-Lin Algorithm)
          * input: graph - initial partition graph
          *        level - refine level
          * output: refined graph
          * */

        var refinedGraph = graph
        var refineLevel = level

        //refine Level = 0 indicates that nodes all
        while(refineLevel!=0){
            /** Step 1: Split coarsen node to refined nodes. */

            // 1.1 Find coarsen nodes and refine them (new node)
            var refinedNodeRDD = refinedGraph.nodeRDD.filter(x=>x.getComposLevel==refineLevel)

            refinedNodeRDD = refinedNodeRDD.map(_.setCompositionPartition())

            // 1.2 Save the nodes which don't need to refine
            val nodeRDD = refinedGraph.nodeRDD.filter(x=>x.getComposLevel!=refineLevel)
            refinedNodeRDD = refinedNodeRDD.flatMap(x=>x.getComposition)

            // 1.3 Union two parts of refined nodes.
            refinedGraph.nodeRDD = refinedNodeRDD.union(nodeRDD)


            /** Step 2: Partitioning */
            val assignment = refinedGraph.nodeRDD.map(x=>(x.getIdx,x.getPartition))

            refinedGraph = KernighanLin.partition(refinedGraph, assignment, needMaxGain = true)
            refinedGraph.nodeRDD = refinedGraph.nodeRDD.map(_.setChosen(false))

            refineLevel-=1 //refine Level decrease 1, up refine
        }

        refinedGraph.nodeNum = refinedGraph.nodeRDD.count()
        refinedGraph
    }

    /** Part II:
      * Graph G_{l+1} is constructed from G_l by finding a maximal matching.
      * Maximal matching can be computed in different ways.
      *
      * Here we briefly describe two of the matching schemes:
      * random matching (randomHeavyEdge)
      * heavy-edge matching (heavyEdge)
      * */

    private def maximalMatching(graph: Graph, mode:String,level:Int): Graph={
        /** Maximal Matching Algorithm
          * input: graph
          *        matching mode, ie "random"
          *        level
          * output: graph
          * */

        var matchEdge:(Node,Node) = null
        breakable{
            while(true){
                if(mode=="random") // Select the implement of maximal matching algorithm.
                    matchEdge = randomHeavyEdge(graph)
                else
                    matchEdge = heavyEdge(graph)

                if(matchEdge==null)
                    break()

                mergeGraph(graph,matchEdge,level) // Merge node
            }// End of while
        }// End of Breakable
        graph
    }

    private def heavyEdge(graph: Graph): (Node,Node)={
        /** Heavy-edge matching (HEM)
          * input:  graph, Graph
          * output: maximal matching, tuple(Node, Node) */

        // Step 1: Visit the vertices of the graph.
        val edge = graph.edgeRDD.filter(!_._4) // Visit the edge in which nodes aren't marked.
        // If all edge are visited and marked, return null.
        if(edge.isEmpty())
            return null

        // Step 2: Match a vertex with the unmatched vertex that is connected with the heavier edge.
        val maxEdge = edge.reduce((x,y)=>if(x._3>=y._3) x else y) // Compare the weights of edges
        val node1 = graph.nodeRDD.filter(_.getIdx==maxEdge._1).
            repartition(1).
            take(1)(0)
        val node2 = graph.nodeRDD.filter(_.getIdx==maxEdge._2).
            repartition(1).
            take(1)(0)
        // Return
        (node1,node2)
    }

    private def randomHeavyEdge(graph: Graph): (Node,Node)={
         /** Heavy-edge matching (HEM) with random select
          *  input:  graph, Graph
          *  output: maximal matching, tuple(Node, Node) */

        // Step 1: Visit the vertices of the graph in random order.
        val edge = graph.edgeRDD.filter(!_._4) // Visit the edge in which nodes aren't marked.
        // If all edge are visited and marked, return null.
        if(edge.isEmpty())
            return null

        // Random sampling
        val randomEdge = edge.takeSample(withReplacement = false,1,seed = 324)(0)
        val randomEdges = edge.filter(x=>x._1==randomEdge._1||x._2==randomEdge._1)

        // Step 2: Match a vertex with the unmatched vertex that is connected with the heavier edge.
        val maxEdge = randomEdges.reduce((x,y)=>if(x._3>=y._3) x else y)
        val node1 = graph.nodeRDD.filter(_.getIdx==maxEdge._1).
            repartition(1).
            take(1)
        val node2 = graph.nodeRDD.filter(_.getIdx==maxEdge._2).
            repartition(1).
            take(1)
        if(node1.isEmpty||node2.isEmpty) return null
        // Return
        (node1(0),node2(0))
    }

    private def mergeGraph(graph: Graph, matchedNodes: (Node, Node),level:Int): Graph={
        /**  Merge nodes according to maximal matching.
          *  input:  graph - origin graph
          *  output: graph - merged graph (smaller)
          * */

        val node1 = matchedNodes._1
        val node2 = matchedNodes._2

        /** Create a new node by merging Node 1 and Node 2. */
        val newNode = mergeNode(node1,node2,level)
        var neighbourEdgeMap: Map[(String,String),Double] =
            newNode.getNeighbour.map(x=>((newNode.getIdx,x._1),x._2))

        /** Add newly merged node to Spark RDD, remove old node from RDD. */
        graph.nodeRDD = graph.nodeRDD.
            filter(_.getIdx!=node2.getIdx).// Remove Node 2 from current RDD.
            map(x=>
                if(x.getIdx == node1.getIdx) // Convert Node 1 to newly merged node.
                    newNode
                else{
                    val weight = x.edgeWeight(node1) + x.edgeWeight(node2)
                    if(weight!=0) {
                        // Modify node's neighbour array,
                        // if the node is connect to Node 1 or Node 2.
                        neighbourEdgeMap += (x.getIdx,newNode.getIdx) -> weight
                        x.popNeighbour(node1).popNeighbour(node2).
                            pushNeighbour((newNode.getIdx,weight))
                    }
                    else
                        x // Remain other node unchanged.
                }// End of If # 1
            )// End of If # 2

        /** Remove old edges from Spark RDD. */
        graph.edgeRDD = graph.edgeRDD.filter{ // Remove edges between Node 1 and Node 2.
            x => !((x._1 == node1.getIdx && x._2 == node2.getIdx) ||
            (x._1 == node2.getIdx) && (x._2 == node1.getIdx))}

        graph.edgeRDD = graph.edgeRDD.
            // Replace Node 1 and Node 2 in all edges with newly merged node.
            map(x=>
                if(x._1 == node1.getIdx || x._1 == node2.getIdx)
                    (newNode.getIdx, x._2, x._3, true)
                else if(x._2 == node1.getIdx || x._2 == node2.getIdx)
                    (x._1, newNode.getIdx, x._3, true)
               // Keep other nodes unchanged.
                else x).
            map(x=>
                if(neighbourEdgeMap.contains((x._1,x._2)))
                    (x._1,x._2,neighbourEdgeMap((x._1,x._2)),true)
                else x).
            distinct() // Remove duplicate edges after edge merging.

        graph.nodeNum -= 1 // Reduce the total node number.
        graph
    }


    private def refreshMarkedFlag(graph: Graph):Graph={
        /** Reset the marked flag of graph to false, after a maximal matching iteration over.
          * input:  graph - marked graph
          * output: graph - unmarked graph
          * */
        graph.nodeRDD = graph.nodeRDD.map(x=>x.setIsMark(false))
        graph.edgeRDD = graph.edgeRDD.map(x=>(x._1,x._2,x._3,false))
        graph
    }

    private def mergeNode(node1:Node, node2:Node,level:Int):Node={
        /** Merge two nodes
          * input:  node - Node 1
          *         node - Node 2
          *         level - composLevel
          * output: node - Merged node
          * */
        val node = new Node(node1.getIdx,mergeNeighbour(node1,node2))

        node.setComposition(List(node1,node2),level)
        node.setIsMark(true)
        node.setWeight(node1.getWeight + node2.getWeight)
        node
    }

    private def mergeNeighbour(node1:Node, node2:Node): Map[String, Double] = {
        /** Merge the neighbour lists of two nodes.
          * input:  node - Node 1
          *         node - Node 2
          * output: Map[String, Double]
          * */

        val unionNeighbour = node1.popNeighbour(node2).getNeighbour ++
                node2.popNeighbour(node1).getNeighbour

        // Shared neighbour
        val interSetNeighbour = node1.getNeighbour.keySet & node2.getNeighbour.keySet
        // Shared neighbour weight sum
        val neighbour = unionNeighbour.map(x=>
            if(interSetNeighbour.contains(x._1))
                (x._1,node1.edgeWeight(x._1) + node2.edgeWeight(x._1))
            else x)

        neighbour

    }

}