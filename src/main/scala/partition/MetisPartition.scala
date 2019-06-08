package partition

import org.apache.spark.rdd.RDD

import util.Graph
import util.Node
import scala.util.Random
import scala.util.control.Breaks._


class MetisPartition{

    private var k:Int = 3

    def this(k:Int){
        this()
        this.k = k
    }

    def getK():Int = this.k
    // Three Main Phases

    // Step 1: Coarsening Phase
    def coarsen(graph: Graph, c: Int): Graph={
        /*
        * @input:  origin graph G_o
        *          coarsening parameter c
        * @output: coarsen graph G_c
        *
        * */

        var coarsenedGraph = graph

        while(coarsenedGraph.nodeNum > c*k){
            coarsenedGraph = MaxMatching(coarsenedGraph)
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
    private def update(graph: Graph,node1:Node,node2:Node):Graph={
        //get neighbour node->weight map
        // filter connection between two nodes
        val neighbourMap1:Map[String,Double] = node1.getNeighbour.filter(_._1!=node2.getIdx).toMap
        val neighbourMap2:Map[String,Double] = node2.getNeighbour.filter(_._1!=node1.getIdx).toMap
        //union two neighbours, for those both neighbours share together, weight sum
        val neighbourMap:Map[String,Double] = (neighbourMap1++neighbourMap2).map(x=>
            if(neighbourMap1.contains(x._1)&&neighbourMap2.contains(x._1))
                (x._1, neighbourMap1(x._1)+neighbourMap2(x._1))
            else (x._1, x._2)
        )

        //convert to edge map
        val neighbourEdgeMap:Map[(String,String),Double] =
            neighbourMap.map(x=>((node1.getIdx,x._1),x._2))
        // update node rdd
        graph.nodeRDD = graph.nodeRDD.filter(
            //just filter node2
            _.getIdx!=node2.getIdx
        ).map(x=>
            if(x.getIdx==node1.getIdx)
                //if node1,set neighbour map,let fusion
                x.setNeighbour(neighbourMap.toList).
                        addComposition(node2).
                        setIsMark(true).
                        setWeight(node1.getWeight+node2.getWeight)

            //those nodes belongs to node1 or node2's neighbour
            else if(neighbourMap1.contains(x.getIdx)&&neighbourMap2.contains(x.getIdx))
            //remove node1 and node2,append new node
                x.removeNeighbour(node1.getIdx).removeNeighbour(node2.getIdx).
                        appendNeighbour((node1.getIdx, neighbourMap(x.getIdx)))

            else if(neighbourMap1.contains(x.getIdx))
            //remove node1, append new node
                x.removeNeighbour(node1.getIdx).
                        appendNeighbour((node1.getIdx, neighbourMap(x.getIdx)))

            else if(neighbourMap2.contains(x.getIdx))
            //remove node2, append new node
                x.removeNeighbour(node2.getIdx).
                        appendNeighbour((node1.getIdx, neighbourMap(x.getIdx)))
            else x
        )
        graph.nodeRDD.foreach(x=>x.Print())
        graph.edgeRDD = graph.edgeRDD.filter(
            //node 2 doesn't exist
            x=>x._1!=node2.getIdx&&x._2!=node2.getIdx
        ).map(x=>
            if(neighbourEdgeMap.contains((x._1,x._2)))
                (x._1,x._2,neighbourEdgeMap((x._1,x._2)),true)
            else if(neighbourEdgeMap.contains((x._2,x._1)))
                (x._1,x._2,neighbourEdgeMap((x._2,x._1)),true)
            else x)
        neighbourEdgeMap.foreach(println)
        graph.edgeRDD.foreach(println)
        println(node1.getIdx,node2.getIdx)
        graph.nodeNum-=1//combine two node
        graph
    }
    private def heavyEdge(graph: Graph):(Node,Node,Double)={

        val edge = graph.edgeRDD.filter(!_._4)//when two node isn't mark mark=false
        if(edge.isEmpty()) return null

        val maxEdge = edge.reduce((x,y)=>if(x._3>=y._3) x else y)
        val node1 = graph.nodeRDD.filter(_.getIdx==maxEdge._1).repartition(1).take(1)(0)
        val node2 = graph.nodeRDD.filter(_.getIdx==maxEdge._2).repartition(1).take(1)(0)//get two node

        (node1,node2,maxEdge._3)
    }
    private def rollBack(graph: Graph):Graph={
        // mark flag roll back to false
        graph.nodeRDD = graph.nodeRDD.map(x=>x.setIsMark(false))
        graph.edgeRDD = graph.edgeRDD.map(x=>(x._1,x._2,x._3,false))
        graph
    }
    // Maximal Matching Algorithm

    // Heavy-edge matching (HEM)
    def MaxMatching(graph: Graph): Graph={

        // Step 1: Visit the vertices of the graph in random order.

        // Step 2: Match a vertex with the unmatched vertex that is connected with the heavier edge.

        if(graph.nodeRDD==null) graph.buildGraph()//lazy construct

        breakable{
            while(true){
                //two node is match
                val matchEdge = heavyEdge(graph)

                if(matchEdge==null) break()

//                println(matchEdge._1.getIdx,matchEdge._2.getIdx)

                update(graph,matchEdge._1,matchEdge._2)
            }
        }
        rollBack(graph)
    }

    def partition(graph: Graph, k:Int): Graph={
        this.k = k
        graph
    }

}