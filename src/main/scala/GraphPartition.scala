import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import java.util.Date

import util.Graph

import partition.HashGraphPartition
import partition.KernighanLin
import partition.SpectralClustering
import partition.MetisPartition

object GraphPartition {


    def readGraph(filePath: String, isDirected: Boolean): RDD[(String, String, Double)] = {
        val sparkSession = SparkSession
                .builder()
                .appName("Graph Partition")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate()

        val fileType: String = filePath.split('.')(1)

        val rdd = fileType match {
            case "txt" => sparkSession.sparkContext.textFile(filePath).map(_.split(" "))
            case "csv" => sparkSession.read.csv(filePath).rdd.map(x => x.toSeq.toArray)
            case _ => sparkSession.read.csv(filePath).rdd.map(x => x.toSeq.toArray)
        }

        val edgeRDD = rdd.map(x => {
            if (x.length == 2) (x(0).toString, x(1).toString, 1.0)
            else (x(0).toString, x(1).toString, x(2).toString.toDouble)
        })

        if (isDirected) edgeRDD
        else edgeRDD.union(edgeRDD.map(x => (x._2, x._1, x._3)))

    }
//=======
//  def readGraph(filePath:String,isDirected:Boolean):RDD[(String,String,Double)]={
//    val sparkSession = SparkSession
//      .builder()
//      .appName("Graph Partition")
//      .master("local")
//      .config("spark.some.config.option", "some-value")
//      .getOrCreate()
//
//    val fileType:String = filePath.split('.')(1)
//
//    // val rdd = fileType match{
//    //   case "txt"=>sparkSession.sparkContext.textFile(filePath).map(_.split(" "))
//    //   case "csv"=>sparkSession.read.csv(filePath).rdd.map(x=>x.toSeq.toArray)
//    //   case _ => sparkSession.read.csv(filePath).rdd.map(x=>x.toSeq.toArray)
//    // }
//
//
//    val rdd = sparkSession.sparkContext.textFile(filePath).map(_.split(" "))
//
//    // rdd.foreach(x=>println(x(0), x(1)))
//
//    val edgeRDD = rdd.map(x=> (x(0).toString, x(1).toString, 1.0))
//
//    // val edgeRDD = rdd.map(x=> {
//        // if(x.length==2) (x(0).toString, x(1).toString, 1.0)
//        // else (x(0).toString, x(1).toString, x(2).toString.toDouble)
//    //   }
//    // )
//
//        val edgeRDD = rdd.map(x => {
//            if (x.length == 2) (x(0).toString, x(1).toString, 1.0)
//            else (x(0).toString, x(1).toString, x(2).toString.toDouble)
//        }
//        )



    def main(args: Array[String]): Unit = {


        val edgeRDD = readGraph(args(0), true).persist()
        var graph = new Graph(edgeRDD) //构建图
//        edgeRDD.foreach(x=>print(x+" "))
//        println()

        graph = new MetisPartition(3).coarsen(graph,5)

//        graph = MetisPartition.MaxMatching(graph)
//        val startTime = new Date().getTime
//        // seed=324,12324,2324
//        graph = KernighanLin.partition(graph, 324, true) //运行算法
//        //graph = SpectralClustering.partition(graph,2,40)
//        //    graph = HashGraphPartition.partition(graph,2)
//        val endTime = new Date().getTime
//        println("运行时间=" + (endTime - startTime) / 1000.0)
//
//        val performance = graph.graphPartitionEvaluation //评价图分割结果
//        println("图分割的performance为: " + performance)
//        println("划分得到的子图")
        graph.Print()
        graph.edgeRDD.foreach(println)

        println(graph.nodeRDD.count())
        println(graph.nodeNum)
    }

}