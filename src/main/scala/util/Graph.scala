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
    this.nodeRDD = this.nodeRDD.map(
      x=>x.swapUpdate(swap_node_a,swap_node_b)
    )
    this
  }
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
  }
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
  }
  //partition algorithm
  private def randomPartition(seed:Long):List[Array[Any]]={

    val node_idx = this.edgeRDD.map(x=>x._1).distinct()

    val count:Int = node_idx.count().toInt
    val half_count:Int = count/2

    Random.setSeed(seed)
    val shuffle_idx = Random.shuffle(node_idx.collect.toList).toArray

    this.vertex_partition=List()
    this.vertex_partition:+=shuffle_idx.take(half_count)
    this.vertex_partition:+=shuffle_idx.takeRight(count-half_count)
    //lazy build graph
//    buildPartitionGraph(this.vertex_partition)
    this.vertex_partition
  }
  def KernighanLin(seed:Long):Graph= {

    randomPartition(seed)//随机分割子图

    val partitionSize = this.vertex_partition(1).length//第二个子图比第1个多一个，如果奇数的话
    println("两个子图的大小分别为："+this.vertex_partition(0).length+" "+this.vertex_partition(1).length)
    println("最多需要交换："+partitionSize+"次")
    this.Print()

    KernighanLin(this.vertex_partition)
  }
  def KernighanLin(init_vertex_partition:List[Array[Any]]):Graph={

    if(init_vertex_partition.length>2){
      println("非法输入")
      return null
    }
    val size1 = init_vertex_partition(0).length
    val size2 = init_vertex_partition(1).length
    val partitionSize = if(size1>=size2) size1 else size2

    buildPartitionGraph(init_vertex_partition)

    //calculate example
    var gain_max = 0.0
    var swap_node_a:Node=null
    var swap_node_b:Node=null
    var chosenNum = 0

    var count:Int = 0
    var evalList:List[Double] = List()


    breakable{
      do{

        println("开始第%d轮次"(count))
        //        val this.nodeRDD_ = this.nodeRDD
        this.nodeRDD.foreach(x=>x.Print())


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

        val node_gain = this.nodeRDD.cartesian(this.nodeRDD).filter(
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

        swapUpdate(swap_node_a, swap_node_b)

        println("第%d轮结束"(count))
        count+=1
        this.nodeRDD.foreach(x=>x.Print())


        println("K-L = "+graphPartitionEvaluation)
        evalList:+=graphPartitionEvaluation

      }while(chosenNum<partitionSize&&gain_max>0)//所有的点都选完或者最大增益小于0

    }//breakbale end

    println("最终的K-L值是="+graphPartitionEvaluation)

    println("历次的K-L变化:")
    evalList.foreach(x=>print(x+" "))
    println()

    this.vertex_partition = List()
    this.vertex_partition:+=this.nodeRDD.filter(x=>
      x.getPartition==0).map(x=>x.getIdx).collect()
    this.vertex_partition:+=this.nodeRDD.filter(x=>
      x.getPartition==1).map(x=>x.getIdx).collect()

    println("performance 变化")
    evalList.foreach(x=>print(x+" "))
    //buildPartitionGraph(this.vertex_partition)
    this
  }
}
