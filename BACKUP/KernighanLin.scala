import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks._
import graphPartition.Node
import graphPartition.Graph




object KernighanLin {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import sparkSession.implicits._
    val df = sparkSession.read.csv("test2.csv")


    /**
      * 1、首先组织一下数据，原来是两两连接的点，现在是(点:key, tuple(点，weight):value)
      *    也就是把连接点和连接的权重组织成一个tuple
      * 2、聚合相同的点，得到邻近点的集合，注意，临近点是一个tuple(点，weight)
      *    并且转化为List
      * 3、计算每个点跟两个子图连接的权重，定义了一个函数计算
      *    思路是filter出那些在子图里面的点的集合，map一下只保留权重，进而sum
      * 4、可以通过map，生成每一个点的类，partition表示这个点在第一个graph与否
      * */


    val df_rdd = df.rdd
//    val split_rdd = df_rdd.map(x=>x(0)).randomSplit(Array(0.5,0.5),1996)

    //    var graph_1  = Array[Any]("1", "2")
    //    var graph_2  = Array[Any]("3", "4")
    //array可变，便于改变
    var vertex_partition_1  = Array[Any]("1", "2", "3")
    var vertex_partition_2  = Array[Any]("4", "5", "6")
    var vertex_partition = List[Array[Any]](vertex_partition_1, vertex_partition_2)

    var df_data = df_rdd.map(x=>(x(0), x(1)))
    df_data = df_data.union(df_data.map(x=>(x._2, x._1)))

    val edgeRDD = df_data.map(x=>(x._1, x._2, 1.0))
    edgeRDD.foreach(println)
//    val graph = new Graph(edgeRDD,vertex_partition)
//    graph.print()
    def get_idx_to_partition(graph_idx:List[Array[Any]]):Map[Any,Int]={

      var lists:List[List[(Any,Int)]] = List()
      for(idx<-graph_idx.indices) {
        println(graph_idx(idx).map(x=>(x,idx)).getClass.getSimpleName)
        lists :+= graph_idx(idx).map(x=>(x,idx))
      }


      lists.flatten.toMap
    }
//    var graph_1 = split_rdd(0).collect()
//    var graph_2 = split_rdd(1).collect()
//    pr



    val map_idx_partition = get_idx_to_partition(vertex_partition)


//    val node_partition = df_data.map(x=>x._1)
//    node_partition.foreach(println)
//    val createCombiner =(x:(Any,Any,Double))=>{
//      (
//        List((x._2,x._3)),
//        if(map_idx_partition(x._1)!=map_idx_partition(x._2)) x._3 else 0,
//        if(map_idx_partition(x._1)==map_idx_partition(x._2)) x._3 else 0
//      )
//    }
//    val mergeValue = (combineValue:(List[(Any,Double)],Double,Double),
//                      x:(Any,Any,Double))=>{
//      (
//        combineValue._1:+(x._2,x._3),
//        combineValue._2+(if(map_idx_partition(x._1)!=map_idx_partition(x._2)) x._3 else 0),
//        combineValue._3+(if(map_idx_partition(x._1)==map_idx_partition(x._2)) x._3 else 0)
//      )
//    }//end method
//    val mergeCombiner = (combineValue1:(List[(Any,Double)],Double,Double),
//                         combineValue2:(List[(Any,Double)],Double,Double))=>{
//      (
//        combineValue1._1:::combineValue2._1,
//        combineValue1._2+combineValue2._2,
//        combineValue1._3+combineValue2._3
//      )
//    }//end method
//    def generateNodeRDD(data:RDD[(Any,Any,Double)]):RDD[Node]={
//      val result = data.map(x=>(x._1,x)).combineByKey(
//        createCombiner,
//        mergeValue,//end method
//        mergeCombiner//end method
//      )//end combineByKey
//      //return
//      result.map(
//        x=>(x._1,x._2._1,x._2._2,x._2._3)
//      ).map(
//        x=>new Node(x._1,x._2,x._3,x._4,map_idx_partition(x._1),false)
//      )
//    }
//    var node_rdd = generateNodeRDD(data)
//    node_rdd.foreach(x=>x.print())
//
//    var node_rdd = data.combineByKey(
//      (x:(Any, Double))=>
//        (
//          List(x),
//          //计算当前连接点跟graph1的连接权重
//          if(graph_1.contains(x._1)) x._2 else 0.0,
//          //计算当前连接点跟graph2的连接权重
//          if(graph_2.contains(x._1)) x._2 else 0.0
//        ),//相当于对value做了一次变换，方便计算
//
//      (combineValue:(List[(Any,Double)], Double, Double), x:(Any, Double))=>
//        (
//          combineValue._1:+x,
//          //合并：累计跟graph1的连接权重+同一个分区不同key对连接点跟graph1的权重
//          combineValue._2+(if(graph_1.contains(x._1)) x._2 else 0.0),
//          //合并：累计跟graph2的连接权重+同一个分区不同key对连接点跟graph2的权重
//          combineValue._3+(if(graph_2.contains(x._1)) x._2 else 0.0)
//        ),//聚合一个分区内部不同key的值，value对应领域
//      //(if(partition_1.contains(x._1)) {println("haha");x._2} else 0.0
//      (combineValue1:(List[(Any,Double)], Double, Double),
//       combineValue2:(List[(Any,Double)], Double, Double))=>
//        (
//          combineValue1._1:::combineValue2._1,
//          //合并不同分区的累计权重
//          combineValue1._2+combineValue2._2,
//          combineValue1._3+combineValue2._3
//        )
//    ).map(
//      x=>(
//        x._1, x._2._1,x._2._2,x._2._3,graph_1.contains(x._1)
//      )
//    ).map(
//      x=>(
//        x._1, x._2,
//        if(x._5) x._4 else x._3,//在第一个图中，E是图2的连接，I是图1的连接
//        if(x._5) x._3 else x._4,//在第一个图中，I是图2的连接，E是图1的连接
//        x._5,
//      )
//    ).map(
//      x=>new Node(x._1, x._2, x._3, x._4, false, x._5)
//    )
//
//    node_rdd.foreach(x=>x.print())
//
//    //calculate example
//    var gain_max = 0.0
//    var swap_node_a:Node=null
//    var swap_node_b:Node=null
//    var chosenNum = 0
//
//    var count:Int = 0
//    var evalList:List[Double] = List()
//
//
//    breakable{
//      do{
//
//        println("开始第%d轮次"(count))
//        //        val node_rdd_ = node_rdd
//        node_rdd.foreach(x=>x.print())
//
//
//        swap_node_a = null
//        swap_node_b = null
//        gain_max = 0.0
//
//        //不在一个子图里面
//        def notInSameGraph(node1:Node, node2:Node):Boolean =
//          node1.get_partition()!=node2.get_partition()
//        //两个点都没有被选择过
//        def unChosen(node1:Node, node2:Node):Boolean =
//          (!node1.get_chosen())&&(!node2.get_chosen())
//        //无向图去重
//        def distinct(node1:Node, node2:Node):Boolean =
//          node1.get_idx().toString<node2.get_idx().toString
//
//        val node_gain = node_rdd.cartesian(node_rdd).filter(
//          x=>
//            distinct(x._1,x._2)&&
//              notInSameGraph(x._1, x._2)&&
//              unChosen(x._1, x._2)
//        ).map(x=>{
//          (x, x._1.swapGain(x._2))
//        })
//        //理论上来说不需要判断node_gain是否为空，如果没有满足条件的点，上一个循环以经退出
//        //判断是否为空需要action，消耗较大，可以判断partitions参数是否为空，空RDD没有分区
//        val max_gain_item = node_gain.reduce((x,y)=>{
//          if(x._2>=y._2) x else y
//        })
//        if(max_gain_item._2>0){
//          println("positive gain!!! find "+max_gain_item._1._1.get_idx
//            +" and "+max_gain_item._1._2.get_idx+" with gain "+max_gain_item._2)
//          swap_node_a = max_gain_item._1._1
//          swap_node_b = max_gain_item._1._2
//          gain_max = max_gain_item._2
//        }
//        else{
//          println("negative gain!!! find "+
//            max_gain_item._1._1.get_idx+" and "+
//            max_gain_item._1._2.get_idx+" with gain "+max_gain_item._2)
//          println("game over!!")
//          println("没有可以交换的点，可以离开了")
//          break()
//        }
//
//        //已经保证了交换的两个点不为空
////        if(swap_node_a==null || swap_node_b==null) {
////
////        }
//        //swap
//        println("swap two node : "+swap_node_a.get_idx+"and"+swap_node_b.get_idx+" wirh gain "+gain_max)
//        chosenNum+=1
//
//        node_rdd = graphUpdate(node_rdd, swap_node_a, swap_node_b)
//
//        println("第%d轮结束"(count))
//        count+=1
//        node_rdd.foreach(x=>x.print())
//
//
//        graph_1 = node_rdd.filter(x=>x.get_partition==0).map(x=>x.get_idx()).collect()
//        graph_2 = node_rdd.filter(x=>x.get_partition==1).map(x=>x.get_idx()).collect()
//
//        println("图1的点包括：")
//        graph_1.foreach(x=>print(x+" "))
//        println()
//        println("图2的点包括：")
//        graph_2.foreach(x=>print(x+" "))
//        print("现在的K-L值是="+(graphPartitionEvaluation(node_rdd)))
//        evalList:+=graphPartitionEvaluation(node_rdd)
//
//      }while(chosenNum!=graph_1.length&&gain_max>0)//所有的点都选完或者最大增益小于0
//
//    }//breakbale end
//
//    println("图1的点包括：")
//    graph_1.foreach(x=>print(x+" "))
//    println()
//    println("图2的点包括：")
//    graph_2.foreach(x=>print(x+" "))
//    println("最终的K-L值是="+(graphPartitionEvaluation(node_rdd)))
//
//    println("历次的K-L变化:")
//    evalList.foreach(x=>print(x+" "))
//    println()

  }

}