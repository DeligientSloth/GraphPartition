package partition

import org.apache.spark.rdd.RDD

import util.Graph
import util.Node
import scala.util.Random
import scala.util.control.Breaks._

object KernighanLin{

    private def swapUpdate(graph:Graph, swap_node_a:Node, swap_node_b:Node): Graph={

        if(swap_node_a==null||swap_node_b==null){
            println("Input node is null. ")
            return graph
        }
        if(swap_node_a.getPartition == swap_node_b.getPartition){
            println("Nodes are in the same partition. ")
            return graph
        }
        if(swap_node_a.getChosen || swap_node_b.getChosen){
            println("Some Node has already swapped. ")
            return graph
        }

        graph.nodeRDD = graph.nodeRDD.map(
            x=>x.swapUpdate(swap_node_a,swap_node_b)
        )

        graph
    } // End of swapUpdate


    //partition algorithm
    private def randomPartition(graph:Graph, seed:Long): List[Array[String]]={

        val count:Int = graph.nodeIdxRDD.count().toInt
        val half_count:Int = count/2

        Random.setSeed(seed)
        val shuffle_idx = Random.shuffle(graph.nodeIdxRDD.collect.toList).toArray

        var vertex_partition:List[Array[String]]=List()
        vertex_partition:+=shuffle_idx.take(half_count)
        vertex_partition:+=shuffle_idx.takeRight(count-half_count)

        vertex_partition
    }// End of randomPartition

    // Implement Kernighan-Lin Algorithm
    def partition(graph:Graph, seed:Long, needMaxGain:Boolean):Graph= {

        // Random partition for initialization
        val vertex_partition = randomPartition(graph, seed)

        // Obtain the size of sub-graph.
        // If the graph node number is odd, then the second sub-graph has one more node than the first one.
        val partitionSize = vertex_partition(1).length
        println("两个子图的大小分别为："+ vertex_partition(0).length+" "+ vertex_partition(1).length)
        println("最多需要交换："+partitionSize+"次")

        partition(graph, vertex_partition,needMaxGain:Boolean)
        graph
    }// End of KernighanLin

    // Random partition for initialization
    def partition(graph:Graph,
                  init_vertex_partition:List[Array[String]],
                  needMaxGain:Boolean):Graph={

        if(init_vertex_partition.length>2){
            println("非法输入")
            return null
        }

        val size1 = init_vertex_partition(0).length
        val size2 = init_vertex_partition(1).length
        val partitionSize = if(size1>=size2) size1 else size2

        // Rebuild graph data according to initial partition.
        graph.buildPartitionGraph(init_vertex_partition)

        //calculate example
        var chosenNum = 0
        var gain_max = 0.0
        var count:Int = 0
        var evalList:List[Double] = List()
        println("开始进入KL")
        breakable{
            do{
                gain_max = iteration(graph,needMaxGain)

                count+=1
                chosenNum+=1

                println(chosenNum,gain_max)
                if(gain_max<=0) break()

                evalList:+=graph.graphPartitionEvaluation

            }while(chosenNum < partitionSize && gain_max > 0)//所有的点都选完或者最大增益小于0

        }//breakbale end

        println("performance 变化")
        evalList.foreach(x=>print(x+" "))
        println()

        graph
    }
    //不在一个子图里面
    def notInSameGraph(node1:Node, node2:Node):Boolean =
        node1.getPartition != node2.getPartition

    //无向图去重
    def distinct(node1:Node, node2:Node):Boolean =
        node1.getIdx.toString < node2.getIdx.toString


    def getMaxGain(nodeUnChosen:RDD[Node]): (Node,Node,Double) ={
        val node_gain = nodeUnChosen.cartesian(nodeUnChosen).filter(
            x=>
                distinct(x._1,x._2) &&
                  notInSameGraph(x._1, x._2)
        ).map(x=>{
            (x._1,x._2,x._1.swapGain(x._2))
        }).filter(_._3>0)

        if(node_gain.isEmpty()) null
        else node_gain.reduce((x,y)=>{
            if(x._3 >= y._3) x else y
        })
    }
    // Iteration
    def iteration(graph: Graph, needMaxGain:Boolean): Double={

        var swap_node_a: Node = null
        var swap_node_b: Node = null
        var maxGain:Double = 0.0

        val nodeUnChosen = graph.nodeRDD.filter(!_.getChosen)

        if(needMaxGain){

            val maxItem = getMaxGain(nodeUnChosen)

            if(maxItem==null) return -1000.0
            swap_node_a = maxItem._1
            swap_node_b = maxItem._2
            maxGain = maxItem._3
            swapUpdate(graph, swap_node_a, swap_node_b)

            maxGain
        }
        else{
            val graph0 = nodeUnChosen.filter(_.getPartition==0).collect()
            val graph1 = nodeUnChosen.filter(_.getPartition==1).collect()

            for(node0<-graph0;node1<-graph1){
                maxGain = node0.swapGain(node1)
                if(maxGain>0) {
                    swapUpdate(graph, node0, node1)
                    return maxGain
                }
            }
            if(maxGain<=0) return -1000.0
            0
        }

    }

}
