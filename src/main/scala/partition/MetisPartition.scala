package partition

import org.apache.spark.rdd.RDD

import util.Graph
import util.Node
import scala.util.Random
import scala.util.control.Breaks._
//import scala.collection.mutable.Map

class MetisPartition{

    private var k:Int = 3

    def this(k:Int){
        this()
        this.k = k
    }

    // Three Main Phases

    // Step 1: Coarsening Phase
    def coarsen(graph: Graph, c: Int,mode:String): Graph={
        /*
        * @input:  origin graph G_o
        *          coarsening parameter c
        * @output: coarsen graph G_c
        * */
//        graph.edgeRDD.foreach(println)
        var coarsenedGraph = graph
        while(coarsenedGraph.nodeNum > c*k){
            coarsenedGraph = maxMatching(coarsenedGraph,mode)
        }
        coarsenedGraph
    }

    // Step 2: Partitioning Phase
    private def initialPartition(graph: Graph, k: Int, weightNorm:Boolean): Graph={
        // Use Spectral Clustering
        var splitGraph = graph

        splitGraph = SpectralClustering.partition(graph, k, 40, weightNorm)

        splitGraph
    }

    private def uncoarsen(graph: Graph): Graph={

        var refinedGraph = graph
        // 顶点可能从一个分区移动到其他许多分区中，甚至移动到其它k-1个分区中
        // 使用k(k-1)个优先队列，每一个对应一种移动

        // 算法每一步找到在这k(k-1)队列中具有最高获益的移动，
        // 移动能够保持或者改进均衡的具有最高获益的顶点。

        // 移动后，全部的k(k-1)个队列都更新数据。

        // While current refined Graph has coarsen nodes
//        var count=1
        breakable{
            while(true){


                // Step 2: Refining nodes
                var refinedNodeRDD = refinedGraph.nodeRDD.filter(x=>x.getWeight>1)

                if(refinedNodeRDD.isEmpty()) break()

                refinedNodeRDD = refinedNodeRDD.map(_.setCompositionPartition())

                val nodeRDD = refinedGraph.nodeRDD.filter(x=>x.getWeight==1)
                refinedNodeRDD = refinedNodeRDD.
                        flatMap(x=>x.getComposition)

                refinedGraph.nodeRDD = refinedNodeRDD.union(nodeRDD)

                // Step 1: Partitioning
                val assigenment = refinedGraph.nodeRDD.map(x=>(x.getIdx,x.getPartition))

                refinedGraph = KernighanLin.partition(graph,assigenment,true)
//                count+=1
                refinedGraph.nodeRDD = refinedGraph.nodeRDD.map(_.setChosen(false))
            }
        }
        refinedGraph.nodeNum = refinedGraph.nodeRDD.count()
        //refinedGraph.buildPartitionGraph()
        refinedGraph
    }

    def unionNeighbour(node1:Node,node2:Node): Map[String,Double] = {

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
    private def unionNode(node1:Node,node2:Node):Node={
        val node = new Node(node1.getIdx,unionNeighbour(node1,node2))
        node.setComposition(List(node1,node2))
        node.setIsMark(true)
        node.setWeight(node1.getWeight+node2.getWeight)
        node
    }
    private def update(graph: Graph,node1:Node,node2:Node):Graph={
        //get neighbour node->weight map
        // filter connection between two nodes
        val newNode = unionNode(node1,node2)//union neighbour,composition

        //convert to edge map
        var neighbourEdgeMap:Map[(String,String),Double] =
            newNode.getNeighbour.map(x=>((newNode.getIdx,x._1),x._2))


        // update node rdd
        graph.nodeRDD = graph.nodeRDD.filter(
            //just filter node2
            _.getIdx!=node2.getIdx
        ).map(x=>
            if(x.getIdx==node1.getIdx) newNode
            else{

                val weight = x.edgeWeight(node1)+x.edgeWeight(node2)

                if(weight!=0) {
                    neighbourEdgeMap += (x.getIdx,newNode.getIdx)->weight
                    x.popNeighbour(node1).popNeighbour(node2).
                            pushNeighbour((newNode.getIdx,weight))
                }
                else x
            }
        )

//        graph.nodeRDD.foreach(x=>x.Print())

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

//        neighbourEdgeMap.foreach(println)
//        graph.edgeRDD.foreach(println)
//        println(node1.getIdx,node2.getIdx)
        graph.nodeNum-=1//combine two node
        graph
    }

    // Heavy-edge matching (HEM)
    private def heavyEdge(graph: Graph): (Node,Node)={
        val edge = graph.edgeRDD.filter(!_._4)//when two node isn't mark mark=false
        if(edge.isEmpty()) return null

        val maxEdge = edge.reduce((x,y)=>if(x._3>=y._3) x else y)
        val node1 = graph.nodeRDD.filter(_.getIdx==maxEdge._1).repartition(1).take(1)(0)
        val node2 = graph.nodeRDD.filter(_.getIdx==maxEdge._2).repartition(1).take(1)(0)//get two node
        (node1,node2)
    }

    private def randomheavyEdge(graph: Graph): (Node,Node)={
        val edge = graph.edgeRDD.filter(!_._4)//when two node isn't mark mark=false
        if(edge.isEmpty()) return null

        val randomEdge = edge.takeSample(false,1,seed = 324)(0)
//        ||
//        x._1==randomEdge._2||x._2==randomEdge._2
        val randomEdges = edge.filter(x=>x._1==randomEdge._1||x._2==randomEdge._1)
        val maxEdge = randomEdges.reduce((x,y)=>if(x._3>=y._3) x else y)
        val node1 = graph.nodeRDD.filter(_.getIdx==maxEdge._1).repartition(1).take(1)(0)
        val node2 = graph.nodeRDD.filter(_.getIdx==maxEdge._2).repartition(1).take(1)(0)//get two node
        (node1,node2)
    }


    private def rollBack(graph: Graph):Graph={
        // mark flag roll back to false
        graph.nodeRDD = graph.nodeRDD.map(x=>x.setIsMark(false))
        graph.edgeRDD = graph.edgeRDD.map(x=>(x._1,x._2,x._3,false))
        graph
    }

    // Maximal Matching Algorithm
    def maxMatching(graph: Graph,mode:String): Graph={

        // Step 1: Visit the vertices of the graph in random order.

        // Step 2: Match a vertex with the unmatched vertex that is connected with the heavier edge.
        var matchEdge:(Node,Node)=null
        breakable{
            while(true){
                //two node is match
                if(mode=="random") matchEdge = randomheavyEdge(graph)
                else matchEdge = heavyEdge(graph)

                if(matchEdge==null) break()

//                println(matchEdge._1.getIdx,matchEdge._2.getIdx)

                update(graph,matchEdge._1,matchEdge._2)
            }
        }
        rollBack(graph)
    }

    def partition(graph: Graph, c:Int, mode:String,weightNorm:Boolean): Graph={
        var partitionedGraph = graph
        // 1.coarsening phase
        partitionedGraph = coarsen(graph, c,mode)

        // 2.partitioning phase
        partitionedGraph = initialPartition(partitionedGraph, k,weightNorm:Boolean)
        println("coarsened performance="+partitionedGraph.graphPartitionEvaluation)
        partitionedGraph.Print()

        // 3.un-coarsening phase
        partitionedGraph = uncoarsen(partitionedGraph)
        println("uncoarsened performance="+partitionedGraph.graphPartitionEvaluation)

        partitionedGraph
    }

}