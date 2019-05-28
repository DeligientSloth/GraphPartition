package util

import org.apache.spark.rdd.RDD
import scala.util.control.Breaks._

import scala.util.Random

class Graph(edge:RDD[(Any,Any,Double)]) extends Serializable {

    val edgeRDD = edge
    var nodeRDD:RDD[Node] = _
    var map_idx_partition:Map[Any,Int] = _
    var vertex_partition:List[Array[Any]] = List()

    def this(edge:RDD[(Any,Any,Double)],
            vertex_partition:List[Array[Any]])={
        this(edge)
        this.vertex_partition = vertex_partition
        this.map_idx_partition = idx_to_partition(vertex_partition)
        buildPartitionGraph(vertex_partition)
    }
    private def idx_to_partition(vertex_partition:List[Array[Any]]):Map[Any,Int]={
        var lists:List[Array[(Any,Int)]] = List()
        for(idx<-vertex_partition.indices)
        lists :+= vertex_partition(idx).map(x=>(x,idx))
        lists.flatten.toMap
    }

    def buildPartitionGraph():Graph=this

    def buildPartitionGraph(vertex_partition:List[Array[Any]]):Graph={
        /**
         * 1、首先组织一下数据，原来是两两连接的点，现在是(点:key, tuple(点，weight):value)
        *    也就是把连接点和连接的权重组织成一个tuple
        * 2、聚合相同的点，得到邻近点的集合，注意，临近点是一个tuple(点，weight)
        *    并且转化为List
        * 3、计算每个点跟两个子图连接的权重，定义了一个函数计算
        *    思路是filter出那些在子图里面的点的集合，map一下只保留权重，进而sum
        * 4、可以通过map，生成每一个点的类，partition表示这个点在第一个graph与否
        * */
        this.vertex_partition = vertex_partition
        this.map_idx_partition = idx_to_partition(vertex_partition)

        val createCombiner =(x:(Any,Any,Double))=>{
        (
            List((x._2,x._3)),
            if(map_idx_partition(x._1)!=map_idx_partition(x._2)) x._3 else 0,
            if(map_idx_partition(x._1)==map_idx_partition(x._2)) x._3 else 0
        )
        }
        val mergeValue = (combineValue:(List[(Any,Double)],Double,Double),
                        x:(Any,Any,Double))=>{
        (
            combineValue._1:+(x._2,x._3),
            combineValue._2+(if(map_idx_partition(x._1)!=map_idx_partition(x._2)) x._3 else 0),
            combineValue._3+(if(map_idx_partition(x._1)==map_idx_partition(x._2)) x._3 else 0)
        )
        }//end method
        val mergeCombiner = (combineValue1:(List[(Any,Double)],Double,Double),
                            combineValue2:(List[(Any,Double)],Double,Double))=>{
        (
            combineValue1._1:::combineValue2._1,
            combineValue1._2+combineValue2._2,
            combineValue1._3+combineValue2._3
        )
        }//end method

        this.nodeRDD = edgeRDD.map(x=>(x._1,x)).combineByKey(
            createCombiner,
            mergeValue,//end method
            mergeCombiner//end method
        ).map(
            x=>(x._1,x._2._1,x._2._2,x._2._3)
        ).map(
            x=>new Node(x._1,x._2,x._3,x._4,map_idx_partition(x._1),false)
        )
        this
    } // End of buildPartitionGraph

    def swapUpdate(swap_node_a:Node,
                    swap_node_b:Node):Graph={
        //入口参数检查
        if(swap_node_a==null||swap_node_b==null){
            println("Input node is null. ")
            return this
        }

        if(swap_node_a.getPartition == swap_node_b.getPartition){
            println("Nodes are in the same partition. ")
            return this
        }

        if(swap_node_a.getChosen || swap_node_b.getChosen){
            println("Some Node has already swapped. ")
            return this
        }

        this.nodeRDD = this.nodeRDD.map(
            x=>x.swapUpdate(swap_node_a,swap_node_b)
        )
        this
    } // End of swapUpdate

    def graphPartitionEvaluation:Double={
        //计算图内聚和图外连
        /**
         * 把点map为E和，计算E和I的总和，
        * 然后累计起来，因为每个点都有两次相连，所以除以2*/
        val inner_external_Weight = this.nodeRDD.map(
        x=>(x.getI, x.getE)).reduce(
            (x,y)=>(x._1+y._1,x._2+y._2)
        )

    //    0.5*(inner_external_Weight._1-inner_external_Weight._2)
        inner_external_Weight._1/inner_external_Weight._2
    } // end of graphPartitionEvaluation

    def Print()={

        if(this.edgeRDD!=null){
            println("=====Edge Set============")
            this.edgeRDD.foreach(println)
        }

        if(this.nodeRDD!=null){
            println("=====Node Set============")
            this.nodeRDD.foreach(x=>x.Print())
        }

        if(this.vertex_partition!=null){
            println("=====Each Partition============")
            for (idx <- vertex_partition.indices) {
                printf("===========第%d个子图是============", idx)
                vertex_partition(idx).foreach(x=>print(x+" "))
                println()
            }
        }
    } // End of Print

}
