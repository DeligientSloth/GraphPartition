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

    /** Part I:
      * Three functions of Main Phase -- coarsen(), initialPartition(), refine() */

    private def coarsen(graph: Graph, c: Int,mode:String): Graph={
        /** Step 1: Coarsening Phase (via Maximal Matching Algorithm)
          * input:  origin graph G_o,
          *         coarsening parameter c,
          *         mode
          * output: coarsen graph G_c
        * */
        var coarsenedGraph = graph
        while(coarsenedGraph.nodeNum > c*k){
            coarsenedGraph = maximalMatching(coarsenedGraph, mode)
            coarsenedGraph = refreshMarkedFlag(coarsenedGraph)
        }
        coarsenedGraph
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

    private def refine(graph: Graph): Graph={
        /** Step 3: Refining Phase (via Kernighan-Lin Algorithm)
          * input: graph - initial partition graph
          * output: refined graph
          * */

        var refinedGraph = graph

        breakable{
            while(true){
                /** Step 1: Split coarsen node to refined nodes. */

                // 1.1 Find coarsen nodes and refine them (new node)
                var refinedNodeRDD = refinedGraph.nodeRDD.filter(x=>x.getWeight>1)
                if(refinedNodeRDD.isEmpty()) break()
                refinedNodeRDD = refinedNodeRDD.map(_.setCompositionPartition())

                // 1.2 Save the nodes which don't need to refine
                val nodeRDD = refinedGraph.nodeRDD.filter(x=>x.getWeight==1)
                refinedNodeRDD = refinedNodeRDD.flatMap(x=>x.getComposition)

                // 1.3 Union two parts of refined nodes.
                refinedGraph.nodeRDD = refinedNodeRDD.union(nodeRDD)

                /** Step 2: Partitioning */
                val assignment = refinedGraph.nodeRDD.map(x=>(x.getIdx,x.getPartition))
                refinedGraph = KernighanLin.partition(graph, assignment, needMaxGain = true)
                refinedGraph.nodeRDD = refinedGraph.nodeRDD.map(_.setChosen(false))
            }
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

    private def maximalMatching(graph: Graph, mode:String): Graph={
        /** Maximal Matching Algorithm
          * input: graph
          *        matching mode, ie "random"
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

                mergeNode(graph,matchEdge) // Merge node
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
        val node1 = graph.nodeRDD.filter(_.getIdx==maxEdge._1).repartition(1).take(1)(0)
        val node2 = graph.nodeRDD.filter(_.getIdx==maxEdge._2).repartition(1).take(1)(0)
        // Return
        (node1,node2)
    }

    private def randomHeavyEdge(graph: Graph): (Node,Node)={
         /** Heavy-edge matching (HEM) with random select
          * input:  graph, Graph
          * output: maximal matching, tuple(Node, Node) */

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
        val node1 = graph.nodeRDD.filter(_.getIdx==maxEdge._1).repartition(1).take(1)(0)
        val node2 = graph.nodeRDD.filter(_.getIdx==maxEdge._2).repartition(1).take(1)(0)
        // Return
        (node1,node2)
    }

    private def mergeNode(graph: Graph, matchedNodes: (Node, Node)):Graph={
        /** Merge nodes which are maximal matching. */
        //get neighbour node->weight map

        // filter connection between two nodes
        val node1 = matchedNodes._1
        val node2 = matchedNodes._2
        val newNode = unionNode(node1,node2)//union neighbour,composition

        //convert to edge map
        var neighbourEdgeMap:Map[(String,String),Double] =
            newNode.getNeighbour.map(x=>((newNode.getIdx,x._1),x._2))

        // update node rdd
        graph.nodeRDD = graph.nodeRDD.filter(
            //just filter node2
            _.getIdx!=node2.getIdx
        ).map(x=>
            if(x.getIdx==node1.getIdx)
                newNode
            else{
                val weight = x.edgeWeight(node1) + x.edgeWeight(node2)
                if(weight!=0) {
                    neighbourEdgeMap += (x.getIdx,newNode.getIdx)->weight
                    x.popNeighbour(node1).popNeighbour(node2).
                        pushNeighbour((newNode.getIdx,weight))
                }
                else x
            }
        )

        //update edge rdd
        //filter node1<->node2
        graph.edgeRDD = graph.edgeRDD.filter
            //node 2 doesn't exist
        { x =>
            !((x._1 == node1.getIdx && x._2 == node2.getIdx) ||
                (x._1 == node2.getIdx) && (x._2 == node1.getIdx))
        }

        graph.edgeRDD=graph.edgeRDD.map(
            x=>
                //which node are node1,node2
                if(x._1==node1.getIdx||x._1==node2.getIdx)
                    (newNode.getIdx,x._2,x._3,true)
                else if(x._2==node1.getIdx||x._2==node2.getIdx)
                    (x._1,newNode.getIdx,x._3,true)
                else x
        ).map(
            x=>
                if(neighbourEdgeMap.contains((x._1,x._2)))
                    (x._1,x._2,neighbourEdgeMap((x._1,x._2)),true)
                else x
        ).distinct()

        graph.nodeNum-=1 //combine two node
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


    private def unionNode(node1:Node, node2:Node):Node={
        val node = new Node(node1.getIdx,unionNeighbour(node1,node2))
        node.setComposition(List(node1,node2))
        node.setIsMark(true)
        node.setWeight(node1.getWeight+node2.getWeight)
        node
    }

    private def unionNeighbour(node1:Node, node2:Node): Map[String, Double] = {

        val unionNeighbour = node1.popNeighbour(node2).getNeighbour++
                node2.popNeighbour(node1).getNeighbour
        // shared neighbour
        val intersetNeighbour = node1.getNeighbour.keySet & node2.getNeighbour.keySet
        //shared neighbour weight sum
        val neighbour = unionNeighbour.map(x=>
            if(intersetNeighbour.contains(x._1))
                (x._1,node1.edgeWeight(x._1) + node2.edgeWeight(x._1))
            else x)
        neighbour
    }

    def partition(graph: Graph, c:Int, mode:String,weightNorm:Boolean): Graph={
        var partitionedGraph = graph
        // 1.coarsening phase
        partitionedGraph = coarsen(graph, c,mode)

        // 2.partitioning phase
        partitionedGraph = initialPartition(partitionedGraph, weightNorm:Boolean)
        println("Coarsened performance="+partitionedGraph.graphPartitionEvaluation)
        partitionedGraph.printGraph()

        // 3.un-coarsening phase
        partitionedGraph = refine(partitionedGraph)
        println("Refined performance="+partitionedGraph.graphPartitionEvaluation)

        partitionedGraph
    }

}