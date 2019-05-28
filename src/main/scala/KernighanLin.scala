import util.Graph
import util.Node
import scala.util.Random
import scala.util.control.Breaks._
import org.apache.spark.rdd.RDD

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
    private def randomPartition(graph:Graph, seed:Long): List[Array[Any]]={

        val node_idx = graph.edgeRDD.map(x=>x._1).distinct()
        val count:Int = node_idx.count().toInt
        val half_count:Int = count/2

        Random.setSeed(seed)
        val shuffle_idx = Random.shuffle(node_idx.collect.toList).toArray

        graph.vertex_partition=List()
        graph.vertex_partition:+=shuffle_idx.take(half_count)
        graph.vertex_partition:+=shuffle_idx.takeRight(count-half_count)

        graph.vertex_partition
    }// End of randomPartition

    // Implement Kernighan-Lin Algorithm
    def partition(graph:Graph, seed:Long):Graph= {

        // Random partition for initialization
        randomPartition(graph, seed)

        // Obtain the size of sub-graph. 
        // If the graph node number is odd, then the second sub-graph has one more node than the first one.
        val partitionSize = graph.vertex_partition(1).length
        println("两个子图的大小分别为："+ graph.vertex_partition(0).length+" "+ graph.vertex_partition(1).length)
        println("最多需要交换："+partitionSize+"次")
        // graph.Print()

        partition(graph, graph.vertex_partition)
        graph
    }// End of KernighanLin

    // Random partition for initialization
    def partition(graph:Graph, init_vertex_partition:List[Array[Any]]):Graph={

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
        var gain_max = 0.0
        var swap_node_a: Node = null
        var swap_node_b: Node = null
        var chosenNum = 0

        var count:Int = 0
        var evalList:List[Double] = List()

        breakable{
            do{

                println("开始第%d轮次"(count))
                graph.nodeRDD.foreach(x=>x.Print())

                swap_node_a = null
                swap_node_b = null
                gain_max = 0.0

                //不在一个子图里面
                def notInSameGraph(node1:Node, node2:Node):Boolean =
                    node1.getPartition!=node2.getPartition

                //两个点都没有被选择过
                def unChosen(node1:Node, node2:Node):Boolean =
                    (!node1.getChosen)&&(!node2.getChosen)

                //无向图去重
                def distinct(node1:Node, node2:Node):Boolean =
                    node1.getIdx.toString<node2.getIdx.toString

                val node_gain = graph.nodeRDD.cartesian(graph.nodeRDD).filter(
                x=>
                    distinct(x._1,x._2)&&
                    notInSameGraph(x._1, x._2)&&
                    unChosen(x._1, x._2)
                ).map(x=>{
                    (x, x._1.swapGain(x._2))
                })
                //理论上来说不需要判断node_gain是否为空，如果没有满足条件的点，上一个循环以经退出
                //判断是否为空需要action，消耗较大，可以判断partitions参数是否为空，空RDD没有分区
                val max_gain_item = node_gain.reduce((x,y)=>{
                    if(x._2>=y._2) x else y
                })
                if(max_gain_item._2>0){
                    println("positive gain!!! find "+max_gain_item._1._1.getIdx
                        +" and "+max_gain_item._1._2.getIdx+" with gain "+max_gain_item._2)
                    swap_node_a = max_gain_item._1._1
                    swap_node_b = max_gain_item._1._2
                    gain_max = max_gain_item._2
                }
                else{
                    println("negative gain!!! find "+
                        max_gain_item._1._1.getIdx+" and "+
                        max_gain_item._1._2.getIdx+" with gain "+max_gain_item._2)
                    println("game over!!")
                    println("没有可以交换的点，可以离开了")
                    break()
                }

                println("swap two node : "+swap_node_a.getIdx+"and"+swap_node_b.getIdx+" wirh gain "+gain_max)
                chosenNum+=1

                swapUpdate(graph, swap_node_a, swap_node_b)

                println("第%d轮结束"(count))
                count+=1
                graph.nodeRDD.foreach(x=>x.Print())


                println("K-L = "+graph.graphPartitionEvaluation)
                evalList:+=graph.graphPartitionEvaluation

            }while(chosenNum < partitionSize && gain_max > 0)//所有的点都选完或者最大增益小于0

        }//breakbale end

        println("最终的K-L值是="+graph.graphPartitionEvaluation)

        println("历次的K-L变化:")
        evalList.foreach(x=>print(x+" "))
        println()

        graph.vertex_partition = List()
        graph.vertex_partition:+=graph.nodeRDD.filter(x=>
            x.getPartition==0).map(x=>x.getIdx).collect()
        graph.vertex_partition:+=graph.nodeRDD.filter(x=>
            x.getPartition==1).map(x=>x.getIdx).collect()

        println("performance 变化")
        evalList.foreach(x=>print(x+" "))

        graph
    }

    // Iteration

}
