package util

import org.apache.spark.rdd.RDD

class Graph extends Serializable {

    var edgeRDD: RDD[(String, String, Double, Boolean)] = _//edge的RDD组成，(起点，终点，权重，是否是匹配边)
    //directed and undirected
    var nodeNum:Long = 0//图内node的个数

    var nodeRDD: RDD[Node] = _//图内所有node的RDD组成，每条记录都是node类
//    var map_idx_partition: Map[String, Int] = _ //test mode use
    def this(edge: RDD[(String,String,Double)])={
        this()
        edgeRDD=edge.map(x=>(x._1,x._2,x._3,false))
        if(this.nodeRDD==null) this.buildGraph()//lazy construct
    }

    private def idx_to_partition(vertex_partition: List[Array[String]]): Map[String, Int] = {
        var lists: List[Array[(String, Int)]] = List()
        for (idx <- vertex_partition.indices)
            lists :+= vertex_partition(idx).map(x => (x, idx))
        lists.flatten.toMap
    }

    def buildGraph():Graph={
        nodeRDD = edgeRDD.map(
            x=>(x._1,x._2,x._3)).map(x => (x._1, (x._2,x._3))).
                groupByKey().map(x=>(x._1,x._2.toMap)).
                map(x=>new Node(x._1,x._2,0.0,0.0,0,false,false,1.0))
        this.nodeNum=nodeRDD.count()
        this
    }

    def assignPartitionToComposition():Graph={
        this.nodeRDD.map(_.setCompositionPartition())
        this
    }
    def buildPartitionGraph():Graph={
        if(this.nodeRDD==null) buildGraph()
        val map_idx_partition = this.nodeRDD.map(x=>(x.getIdx,x.getPartition)).collectAsMap()
        this.nodeRDD=this.nodeRDD.map(
            x=>{
                val neighbour = x.getNeighbour
                var E = 0.0
                var I = 0.0
                for (elem <- neighbour) {
                    if(map_idx_partition.contains(elem._1)){
                        if(map_idx_partition(elem._1)==x.getPartition)
                            I+=elem._2
                        else
                            E+=elem._2
                    }
                }
                x.setE(E).setI(I)
            }
        )
        this
    }
    def buildPartitionGraph(assignment: RDD[(String, Int)]):Graph={

        if(this.nodeRDD==null) buildGraph()

        this.nodeRDD = this.nodeRDD.map(x=>(x.getIdx,x)).
                join(assignment).map( //join (idx,(node,partition))
            x=>x._2._1.setPartition(x._2._2)  //node,set partition
        )

        buildPartitionGraph()

        this
//        assignPartitionToComposition()
    }
    private def buildPartitionGraph(map_idx_partition:Map[String,Int]): Graph = {
        /**
          * 1、首先组织一下数据，原来是两两连接的点，现在是(点:key, tuple(点，weight):value)
          * 也就是把连接点和连接的权重组织成一个tuple
          * 2、聚合相同的点，得到邻近点的集合，注意，临近点是一个tuple(点，weight)
          * 并且转化为List
          * 3、计算每个点跟两个子图连接的权重，定义了一个函数计算
          * 思路是filter出那些在子图里面的点的集合，map一下只保留权重，进而sum
          * 4、可以通过map，生成每一个点的类，partition表示这个点在第一个graph与否
          **/
        val createCombiner = (x: (String, String, Double)) => {
            (
                    List((x._2, x._3)),
                    if (map_idx_partition(x._1) != map_idx_partition(x._2)) x._3 else 0,
                    if (map_idx_partition(x._1) == map_idx_partition(x._2)) x._3 else 0
            )
        }
        val mergeValue = (combineValue: (List[(String, Double)], Double, Double),
                          x: (String, String, Double)) => {
            (
                    combineValue._1 :+ (x._2, x._3),
                    combineValue._2 + (if (map_idx_partition(x._1) != map_idx_partition(x._2)) x._3 else 0),
                    combineValue._3 + (if (map_idx_partition(x._1) == map_idx_partition(x._2)) x._3 else 0)
            )
        } //end method
        val mergeCombiner = (combineValue1: (List[(String, Double)], Double, Double),
                             combineValue2: (List[(String, Double)], Double, Double)) => {
            (
                    combineValue1._1 ::: combineValue2._1,
                    combineValue1._2 + combineValue2._2,
                    combineValue1._3 + combineValue2._3
            )
        } //end method

        this.nodeRDD = edgeRDD.map(
            x=>(x._1,x._2,x._3)).map(x => (x._1, x)).combineByKey(
            createCombiner,
            mergeValue, //end method
            mergeCombiner //end method
        ).map(
            x => (x._1, x._2._1, x._2._2, x._2._3)
        ).map(
            x => new Node(x._1, x._2.toMap, x._3, x._4, map_idx_partition(x._1), false,false,1.0)
        )
//        assignPartitionToComposition()
        this
    }


    def buildPartitionGraph(vertex_partition: List[Array[String]]): Graph = {

        val map_idx_partition = idx_to_partition(vertex_partition)
        buildPartitionGraph(map_idx_partition)
    } // End of buildPartitionGraph

    def swapUpdate(swap_node_a: Node,
                   swap_node_b: Node): Graph = {
        //入口参数检查
        if (swap_node_a == null || swap_node_b == null) {
            println("Input node is null. ")
            return this
        }

        if (swap_node_a.getPartition == swap_node_b.getPartition) {
            println("Nodes are in the same partition. ")
            return this
        }

        if (swap_node_a.getChosen || swap_node_b.getChosen) {
            println("Some Node has already swapped. ")
            return this
        }

        this.nodeRDD = this.nodeRDD.map(
            x => x.swapUpdate(swap_node_a, swap_node_b)
        )
        this
    } // End of swapUpdate

    def graphPartitionEvaluation: Double = {
        //计算图内聚和图外连
        /**
          * 把点map为E和，计算E和I的总和，
          * 然后累计起来，因为每个点都有两次相连，所以除以2*/
        val inner_external_Weight = this.nodeRDD.map(
            x => (x.getI, x.getE)).reduce(
            (x, y) => (x._1 + y._1, x._2 + y._2)
        )

        //    0.5*(inner_external_Weight._1-inner_external_Weight._2)
        inner_external_Weight._1 / inner_external_Weight._2
    } // end of graphPartitionEvaluation

    def printGraph() = {
        nodeRDD.foreach(x=>x.Print())
        nodeRDD.map(
            x => (x.getPartition, x.getIdx)
        ).groupByKey().map(x => (x._1, x._2.toList)).foreach(println)
    } // End of Print

}
