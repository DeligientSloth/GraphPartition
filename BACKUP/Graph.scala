package graphPartition

import org.apache.spark.rdd.RDD

class Graph(edge:RDD[(Any,Any,Double)]) {

  var edgeRDD = edge
  var nodeRDD:RDD[Node] = _
  var map_idx_partition:Map[Any,Int] = _

  def this(edge:RDD[(Any,Any,Double)],
           vertex_partition:List[Array[Any]])={
    this(edge)
    this.map_idx_partition = idx_to_partition(vertex_partition)
    buildGraph(vertex_partition)
  }
  private def idx_to_partition(vertex_partition:List[Array[Any]]):Map[Any,Int]={
    var lists:List[List[(Any,Int)]] = List()
    for(idx<-vertex_partition.indices)
      lists :+= vertex_partition(idx).map(x=>(x,idx))
    lists.flatten.toMap
  }

  def buildGraph(vertex_partition:List[Array[Any]]):Graph={
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
    this.map_idx_partition = idx_to_partition(vertex_partition)
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
  }

  def swapUpdate(swap_node_a:Node,
                  swap_node_b:Node):Graph={
    //入口参数检查
    if(swap_node_a==null||swap_node_b==null||
      swap_node_a.getPartition==swap_node_b.getPartition||
      swap_node_a.getChosen||swap_node_b.getChosen){
      println("输入交换的点不合法")
      return this
    }
    nodeRDD = nodeRDD.map(
      x=>x.swapUpdate(swap_node_a,swap_node_b)
    )
    this
  }
  def graphPartitionEvaluation(nodeRDD:RDD[Node]):Double={
    //计算图内聚和图外连
    /**
      * 把点map为E和，计算E和I的总和，
      * 然后累计起来，因为每个点都有两次相连，所以除以2*/
    val inner_external_Weight = nodeRDD.map(
      x=>(x.getI, x.getE)).reduce(
      (x,y)=>(x._1+y._1,x._2+y._2)
    )

    0.5*(inner_external_Weight._1-inner_external_Weight._2)
  }
  def print()={
    println("=====Edge Set============")
    this.edgeRDD.foreach(println)
    println("=====Node Set============")
    this.nodeRDD.foreach(x=>x.print())
  }
}
